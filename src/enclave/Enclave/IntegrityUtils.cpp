#include "IntegrityUtils.h"

void init_log(const tuix::EncryptedBlocks *encrypted_blocks) {
  // Add past entries to log first
  std::vector<LogEntry> past_log_entries;
  auto curr_entries_vec = encrypted_blocks->log()->curr_entries();
  auto past_entries_vec = encrypted_blocks->log()->past_entries();

  for (uint32_t i = 0; i < past_entries_vec->size(); i++) {
    auto entry = past_entries_vec->Get(i);
    int ecall = entry->ecall();
    int snd_pid = entry->snd_pid();
    int rcv_pid = entry->rcv_pid();
    if (rcv_pid == -1) { // Received by PID hasn't been set yet
      rcv_pid = EnclaveContext::getInstance().get_pid();
    }
    int job_id = entry->job_id();
    EnclaveContext::getInstance().append_past_log_entry(ecall, snd_pid, rcv_pid, job_id);

    // Initialize log entry object
    LogEntry le;
    le.ecall = ecall;
    le.snd_pid = snd_pid;
    le.rcv_pid = rcv_pid;
    le.job_id = job_id;
    past_log_entries.push_back(le);
  }

  if (curr_entries_vec->size() > 0) {
    verify_log(encrypted_blocks, past_log_entries);
  }

  // Master list of mac lists of all input partitions
  std::vector<std::vector<std::vector<uint8_t>>> partition_mac_lsts;

  // Check that each input partition's mac_lst_mac is indeed a HMAC over the mac_lst
  for (uint32_t i = 0; i < curr_entries_vec->size(); i++) {
    auto input_log_entry = curr_entries_vec->Get(i);

    // Retrieve mac_lst and mac_lst_mac
    const uint8_t* mac_lst_mac = input_log_entry->mac_lst_mac()->data();
    int num_macs = input_log_entry->num_macs();
    const uint8_t* mac_lst = input_log_entry->mac_lst()->data();
    
    uint8_t computed_hmac[OE_HMAC_SIZE];
    mcrypto.hmac(mac_lst, num_macs * SGX_AESGCM_MAC_SIZE, computed_hmac);

    // Check that the mac lst hasn't been tampered with
    for (int j = 0; j < OE_HMAC_SIZE; j++) {
        if (mac_lst_mac[j] != computed_hmac[j]) {
            throw std::runtime_error("MAC over Encrypted Block MACs from one partition is invalid");
        }
    }
    
    uint8_t* tmp_ptr = (uint8_t*) mac_lst;

    // the mac list of one input log entry (from one partition) in vector form
    std::vector<std::vector<uint8_t>> p_mac_lst;
    for (int j = 0; j < num_macs; j++) {
      std::vector<uint8_t> a_mac (tmp_ptr, tmp_ptr + SGX_AESGCM_MAC_SIZE);
      p_mac_lst.push_back(a_mac);
      tmp_ptr += SGX_AESGCM_MAC_SIZE;
    }

    // Add the macs of this partition to the master list
    partition_mac_lsts.push_back(p_mac_lst);

    // Add this input log entry to history of log entries
    EnclaveContext::getInstance().append_past_log_entry(
        input_log_entry->ecall(), 
        input_log_entry->snd_pid(), 
        EnclaveContext::getInstance().get_pid(), 
        input_log_entry->job_id());
  }

  if (curr_entries_vec->size() > 0) {
    // Check that the MAC of each input EncryptedBlock was expected, i.e. also sent in the LogEntry
    for (auto it = encrypted_blocks->blocks()->begin(); it != encrypted_blocks->blocks()->end(); 
        ++it) {
      size_t ptxt_size = dec_size(it->enc_rows()->size());
      uint8_t* mac_ptr = (uint8_t*) (it->enc_rows()->data() + SGX_AESGCM_IV_SIZE + ptxt_size);
      std::vector<uint8_t> cipher_mac (mac_ptr, mac_ptr + SGX_AESGCM_MAC_SIZE); 

      // Find this element in partition_mac_lsts;
      bool mac_in_lst = false;
      for (uint32_t i = 0; i < partition_mac_lsts.size(); i++) {
        bool found = false;
        for (uint32_t j = 0; j < partition_mac_lsts[i].size(); j++) {
          if (cipher_mac == partition_mac_lsts[i][j]) {
            partition_mac_lsts[i].erase(partition_mac_lsts[i].begin() + j);
            found = true;
            break;
          }
        }
        if (found) {
          mac_in_lst = true;
          break;
        }
      }

      if (!mac_in_lst) {
        throw std::runtime_error("Unexpected block given as input to the enclave");
      }
    }

    // Check that partition_mac_lsts is now empty - we should've found all expected MACs
    for (std::vector<std::vector<uint8_t>> p_lst : partition_mac_lsts) {
      if (!p_lst.empty()) {
        throw std::runtime_error("Did not receive expected EncryptedBlock");
      }
    }
  }
}

void verify_log(const tuix::EncryptedBlocks *encrypted_blocks, 
    std::vector<LogEntry> past_log_entries) {
  auto num_past_entries_vec = encrypted_blocks->log()->num_past_entries();
  auto curr_entries_vec = encrypted_blocks->log()->curr_entries();

  if (curr_entries_vec->size() > 0) {
    int num_curr_entries = curr_entries_vec->size();
    int past_entries_seen = 0;

    for (int i = 0; i < num_curr_entries; i++) {
      auto curr_log_entry = curr_entries_vec->Get(i);
      int curr_ecall = curr_log_entry->ecall();
      int snd_pid = curr_log_entry->snd_pid();
      int rcv_pid = -1;
      int job_id = curr_log_entry->job_id();
      int num_macs = curr_log_entry->num_macs();
      int num_past_entries = num_past_entries_vec->Get(i);

      uint8_t mac_lst_mac[OE_HMAC_SIZE];
      memcpy(mac_lst_mac, curr_log_entry->mac_lst_mac()->data(), OE_HMAC_SIZE);

      int num_bytes_to_mac = OE_HMAC_SIZE + 6 * sizeof(int) + num_past_entries * 4 * sizeof(int); 

      uint8_t to_mac[num_bytes_to_mac];

      // MAC the data
      uint8_t actual_mac[32];
      mac_log_entry_chain(num_bytes_to_mac, to_mac, mac_lst_mac, curr_ecall, snd_pid, rcv_pid, 
          job_id, num_macs, num_past_entries, past_log_entries, past_entries_seen, 
          past_entries_seen + num_past_entries_vec->Get(i), actual_mac);

      uint8_t expected_mac[32];
      memcpy(expected_mac, encrypted_blocks->log_mac()->Get(i)->mac()->data(), 32);

      if (!std::equal(std::begin(expected_mac), std::end(expected_mac), std::begin(actual_mac))) {
        throw std::runtime_error("MAC did not match");
      }
      past_entries_seen += num_past_entries_vec->Get(i);
    }
  }
}

void mac_log_entry_chain(int num_bytes_to_mac, uint8_t* to_mac, uint8_t* mac_lst_mac, 
    int curr_ecall, int curr_pid, int rcv_pid, int job_id, int num_macs, 
    int num_past_entries, std::vector<LogEntry> past_log_entries, int first_le_index, 
    int last_le_index, uint8_t* ret_hmac) {

  // Copy what we want to mac to contiguous memory
  memcpy(to_mac, mac_lst_mac, OE_HMAC_SIZE);
  memcpy(to_mac + OE_HMAC_SIZE, &curr_ecall, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + sizeof(int), &curr_pid, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + 2 * sizeof(int), &rcv_pid, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + 3 * sizeof(int), &job_id, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + 4 * sizeof(int), &num_macs, sizeof(int));
  memcpy(to_mac + OE_HMAC_SIZE + 5 * sizeof(int), &num_past_entries, sizeof(int));

  // Copy over data from past log entries
  uint8_t* tmp_ptr = to_mac + OE_HMAC_SIZE + 6 * sizeof(int);
  for (int i = first_le_index; i < last_le_index; i++) {
    auto past_log_entry = past_log_entries[i];
    int past_ecall = past_log_entry.ecall;
    int pe_snd_pid = past_log_entry.snd_pid;
    int pe_rcv_pid = past_log_entry.rcv_pid;
    int pe_job_id = past_log_entry.job_id;
    
    memcpy(tmp_ptr, &past_ecall, sizeof(int));
    memcpy(tmp_ptr + sizeof(int), &pe_snd_pid, sizeof(int));
    memcpy(tmp_ptr + 2 * sizeof(int), &pe_rcv_pid, sizeof(int));
    memcpy(tmp_ptr + 3 * sizeof(int), &pe_job_id, sizeof(int));

    tmp_ptr += 4 * sizeof(int);
  }
  // MAC the data
  mcrypto.hmac(to_mac, num_bytes_to_mac, ret_hmac);

}

// int get_past_ecalls_lengths(std::vector<LogEntry> past_log_entries, int first_le_index, 
//     int last_le_index) {
//   int past_ecalls_lengths = 0;
//   for (int i = first_le_index; i < last_le_index; i++) {
//     auto past_log_entry = past_log_entries[i];
//     std::string ecall = past_log_entry.ecall;
//     past_ecalls_lengths += ecall.length();
//   }
//   return past_ecalls_lengths;
// }
