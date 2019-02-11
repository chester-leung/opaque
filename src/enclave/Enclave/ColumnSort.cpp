using namespace edu::berkeley::cs::rise::opaque;

#include "ColumnSort.h"

void shift_up(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length) {

  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t top_destination =
    (partition_idx == 0) ? 0 : partition_idx - 1;
  uint32_t bottom_destination = (partition_idx == 0) ? num_partitions - 1 : partition_idx;

  uint32_t i = 0;
  uint32_t n = r.num_rows();
  assert(n % 2 == 0);

  bool top_written = false, bottom_written = false;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    w.write(row);

    if (i + 1 == n / 2) {
      w.write_shuffle_output(w.write_encrypted_blocks(), top_destination);
      top_written = true;
    }
    if (i == n - 1) {
      w.write_shuffle_output(w.write_encrypted_blocks(), bottom_destination);
      bottom_written = true;
    }

    i++;
  }

  if (!top_written) {
    w.write_shuffle_output(w.write_encrypted_blocks(), top_destination);
  }
  if (!bottom_written) {
    w.write_shuffle_output(w.write_encrypted_blocks(), bottom_destination);
  }

  w.finish(w.write_shuffle_outputs());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}

void shift_down(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t top_destination = partition_idx
  uint32_t bottom_destination = (partition_idx == num_partitions - 1) ? 0 : partition_idx + 1;


  uint32_t i = 0;
  uint32_t n = r.num_rows();
  assert(n % 2 == 0);

  bool top_written = false, bottom_written = false;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    w.write(row);

    if (i + 1 == n / 2) {
      w.write_shuffle_output(w.write_encrypted_blocks(), top_destination);
      top_written = true;
    }
    if (i == n - 1) {
        w.write_shuffle_output(w.write_encrypted_blocks(), bottom_destination);
        bottom_written = true;
    }

    i++;
  }

  if (!top_written) {
    w.write_shuffle_output(w.write_encrypted_blocks(), top_destination);
  }
  if (!bottom_written) {
    w.write_shuffle_output(w.write_encrypted_blocks(), bottom_destination);
  }

  w.finish(w.write_shuffle_outputs());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size();
}

void transpose(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);

  std::vector<std::unique_ptr<FlatbuffersRowWriter>> ws(num_partitions);
  for (uint32_t k = 0; k < num_partitions; k++) {
    FlatbuffersRowWriter w;
    ws.push_back(w);
  }

  uint32_t i = 0;
  while (r.has_next()) {
    ws[i % num_partitions].write(row);
    i++;
  }

  FlatbuffersRowWriter shuffle_output_writer;
  for (uint32_t j = 0; j < ws.size(); j++) {
    ws[j].write_shuffle_output(ws[j].write_encrypted_blocks(), j);

    ShuffleOutputReader sor(ws[j].output_buffer(), ws[j].output_size());
    flatbuffers_copy(sor.get(), shuffle_output_writer);
  }

  shuffle_output_writer.finish(shuffle_output_writer.write_shuffle_outputs());
  *output_rows = shuffle_output_writer.output_buffer().release();
  *output_rows_length = shuffle_output_writer.output_size();
}

void untranspose(uint8_t *input_rows, size_t input_rows_length,
              uint32_t partition_idx, uint32_t num_partitions,
              uint8_t **output_rows, size_t *output_rows_length) {
  EncryptedBlocksToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  uint32_t n = r.num_rows();
  assert(n % 2 == 0);

  uint32_t row = 1;
  uint32_t col = partition_idx + 1;
  uint32_t idx = 0;
  uint32_t dst_column = 0;
  uint32_t dst_partition_idx = 0;
  uint32_t prev_dst_partition_idx = 0;

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    w.write(row)

    idx = (row - 1) * num_partitions + col;
    dst_column = (idx - 1) / input_rows_length + 1;
    dst_partition_idx = dst_column - 1;

    if (dst_partition_idx != prev_dst_partition_idx) {
      // Rows are going to a different partition
      w.write_shuffle_output(w.write_encrypted_blocks(), prev_dst_partition_idx);
    }

    prev_dst_partition_idx = dst_partition_idx;
    row++;
  }

  w.finish(w.write_shuffle_outputs());
  *output_rows = w.output_buffer().release();
  *output_rows_length = w.output_size(); 
}
