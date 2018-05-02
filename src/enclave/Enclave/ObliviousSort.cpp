#include "Sort.h"

#include <algorithm>
#include <queue>

#include "ExpressionEvaluation.h"

class MergeItem {
 public:
  const tuix::Row *v;
  uint32_t run_idx;
};

flatbuffers::Offset<tuix::EncryptedBlocks> sort_single_encrypted_block(
  FlatbuffersRowWriter &w,
  const tuix::EncryptedBlock *block,
  FlatbuffersSortOrderEvaluator &sort_eval) {

  EncryptedBlockToRowReader r;
  r.reset(block);
  std::vector<const tuix::Row *> sort_ptrs(r.begin(), r.end());

  std::sort(
    sort_ptrs.begin(), sort_ptrs.end(),
    [&sort_eval](const tuix::Row *a, const tuix::Row *b) {
      return sort_eval.less_than(a, b);
    });

  for (auto it = sort_ptrs.begin(); it != sort_ptrs.end(); ++it) {
    w.write(*it);
  }
  return w.write_encrypted_blocks();
}

void oblivious_merge(FlatbuffersSortOrderEvaluator &sort_eval, const tuix::EncryptedBlocks *block1, const tuix::EncryptedBlocks *block2, FlatbuffersRowWriter *w1, FlatbuffersRowWriter *w2) {
  auto compare = [&sort_eval](const MergeItem &a, const MergeItem &b) {
    return sort_eval.less_than(b.v, a.v);
  };
  std::priority_queue<MergeItem, std::vector<MergeItem>, decltype(compare)>
    queue(compare);
    MergeItem item;
    EncryptedBlocksToRowReader r1(block1);
    uint32_t r1_rows = r1.num_rows();
    EncryptedBlocksToRowReader r2(block2);
    item.v = r1.next();
    item.run_idx = 0;
    queue.push(item);
    item.v = r2.next();
    item.run_idx = 1;
    queue.push(item);

  // Merge the runs using the priority queue
  uint32_t i = 0;
  while (!queue.empty()) {
    MergeItem item = queue.top();
    queue.pop();
    if (i < r1_rows) {
      w1.write(item.v);
    }
    else {
      w2.write(item.v);
    }
    // Read another row from the same run that this one came from
    EncryptedBlocksToRowReader *r_next = (item.run_idx == 0) ? r1 : r2;
    if (r_next.has_next()) {
      item.v = r_next.next();
      queue.push(item);
    }
    i++;
  }
}

void oblivious_sort(uint8_t *sort_order, size_t sort_order_length,
                   uint8_t *input_rows, size_t input_rows_length,
                   uint8_t **output_rows, size_t *output_rows_length) {
  FlatbuffersSortOrderEvaluator sort_eval(sort_order, sort_order_length);

  // 1. Sort each EncryptedBlock individually by decrypting it, sorting within the enclave, and
  // re-encrypting to a different buffer.
  FlatbuffersRowWriter w;
  {
    EncryptedBlocksToEncryptedBlockReader r(input_rows, input_rows_length);
    std::vector<flatbuffers::Offset<tuix::EncryptedBlocks>> runs;
    uint32_t i = 0;
    for (auto it = r.begin(); it != r.end(); ++it, ++i) {
      debug("Sorting buffer %d with %d rows\n", i, it->num_rows());
      runs.push_back(sort_single_encrypted_block(w, *it, sort_eval));
    }
    if (runs.size() > 1) {
      w.finish(w.write_sorted_runs(runs));
    } else if (runs.size() == 1) {
      w.finish(runs[0]);
      *output_rows = w.output_buffer().release();
      *output_rows_length = w.output_size();
      return;
    } else {
      w.finish(w.write_encrypted_blocks());
      *output_rows = w.output_buffer().release();
      *output_rows_length = w.output_size();
      return;
    }
  }

  // 2. Merge sorted runs. Initially each buffer forms a sorted run. We merge B runs at a time by
  // decrypting an EncryptedBlock from each one, merging them within the enclave using a priority
  // queue, and re-encrypting to a different buffer.
  auto runs_buf = w.output_buffer();
  auto runs_len = w.output_size();
  SortedRunsReader r(runs_buf.get(), runs_len);
  std::vector<BufferRef<tuix::EncryptedBlocks>> blocks; //TODO initialize blocks
  int len = blocks.size();
  int log_len = log_2(len) + 1;
  int offset = 0;
  // Merge sorted buffers pairwise
  for (int stage = 1; stage <= log_len; stage++) {
    for (int stage_i = stage; stage_i >= 1; stage_i--) {
      int part_size = pow_2(stage_i);
      int part_size_half = part_size / 2;
      for (int i = offset; i <= (offset + len - 1); i += part_size) {
        for (int j = 1; j <= part_size_half; j++) {
          int idx = i + j - 1;
          int pair_idx = i + part_size - j;
          if (pair_idx < offset + len) {
            debug("Merging buffers %d and %d with %d, %d rows\n",
                  idx, pair_idx, num_rows[idx], num_rows[pair_idx]);
            FlatbuffersRowWriter w1, w2;
            oblivious_merge(sort_eval, blocks[idx], blocks[pair_idx],
                            &w1, &w2);
            BufferRef b1(w1.output_buffer(), w1.output_size()); 
	    blocks[idx] = b1;
            BufferRef b2(w2.output_buffer(), w2.output_size()); 
	    blocks[pair_idx] = b2;
          }
        }
      }
    }
  }
  FlatbuffersRowWriter w; 
  for (auto& block : blocks) {
    w.write(block.GetRoot());
  }  
  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}

void sample(uint8_t *input_rows, size_t input_rows_length,
			uint8_t **output_rows, size_t *output_rows_length) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  // Sample ~5% of the rows or 1000 rows, whichever is greater
  uint16_t sampling_ratio;
  if (r.num_rows() > 1000 * 20) {
    sampling_ratio = 3276; // 5% of 2^16
  } else {
    sampling_ratio = 16383;
  }

  while (r.has_next()) {
    const tuix::Row *row = r.next();

    uint16_t rand;
    sgx_read_rand(reinterpret_cast<uint8_t *>(&rand), 2);
    if (rand <= sampling_ratio) {
      w.write(row);
    }
  }

  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}

void find_range_bounds(uint8_t *sort_order, size_t sort_order_length,
                       uint32_t num_partitions,
                       uint8_t *input_rows, size_t input_rows_length,
                       uint8_t **output_rows, size_t *output_rows_length) {
  // Sort the input rows
  uint8_t *sorted_rows;
  size_t sorted_rows_length;
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                &sorted_rows, &sorted_rows_length);

  // Split them into one range per partition
  EncryptedBlocksToRowReader r(sorted_rows, sorted_rows_length);
  FlatbuffersRowWriter w;
  uint32_t num_rows_per_part = r.num_rows() / num_partitions;
  uint32_t current_rows_in_part = 0;
  while (r.has_next()) {
    const tuix::Row *row = r.next();
    if (current_rows_in_part == num_rows_per_part) {
      w.write(row);
      current_rows_in_part = 0;
	} else {
	  ++current_rows_in_part;
	}
  }

  w.finish(w.write_encrypted_blocks());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();

  ocall_free(sorted_rows);
}

void partition_for_sort(uint8_t *sort_order, size_t sort_order_length,
                        uint32_t num_partitions,
                        uint8_t *input_rows, size_t input_rows_length,
                        uint8_t *boundary_rows, size_t boundary_rows_length,
                        uint8_t **output_partition_ptrs, size_t *output_partition_lengths) {
  // Sort the input rows
  uint8_t *sorted_rows;
  size_t sorted_rows_length;
  external_sort(sort_order, sort_order_length,
                input_rows, input_rows_length,
                &sorted_rows, &sorted_rows_length);

  // Scan through the input rows and copy each to the appropriate output partition specified by the
  // ranges encoded in the given boundary_rows. A range contains all rows greater than or equal to
  // one boundary row and less than the next boundary row. The first range contains all rows less
  // than the first boundary row, and the last range contains all rows greater than or equal to the
  // last boundary row.
  FlatbuffersSortOrderEvaluator sort_eval(sort_order, sort_order_length);
  EncryptedBlocksToRowReader r(sorted_rows, sorted_rows_length);
  FlatbuffersRowWriter w;
  uint32_t output_partition_idx = 0;

  EncryptedBlocksToRowReader b(boundary_rows, boundary_rows_length);
  // Invariant: b_upper is the first boundary row strictly greater than the current range, or
  // nullptr if we are in the last range
  FlatbuffersTemporaryRow b_upper(b.has_next() ? b.next() : nullptr);

  while (r.has_next()) {
    const tuix::Row *row = r.next();

    // Advance boundary rows to maintain the invariant on b_upper
    while (b_upper.get() != nullptr && !sort_eval.less_than(row, b_upper.get())) {
      b_upper.set(b.has_next() ? b.next() : nullptr);

      // Write out the newly-finished partition
      w.finish(w.write_encrypted_blocks());
      output_partition_ptrs[output_partition_idx] = w.output_buffer().release();
      output_partition_lengths[output_partition_idx] = w.output_size();
      w.clear();
      output_partition_idx++;
    }

    w.write(row);
  }

  // Write out the final partition. If there were fewer boundary rows than expected output
  // partitions, write out enough empty partitions to ensure the expected number of output
  // partitions.
  while (output_partition_idx < num_partitions) {
    w.finish(w.write_encrypted_blocks());
    output_partition_ptrs[output_partition_idx] = w.output_buffer().release();
    output_partition_lengths[output_partition_idx] = w.output_size();
    w.clear();
    output_partition_idx++;
  }

  ocall_free(sorted_rows);
}
