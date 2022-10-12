#pragma once

#include <memory>
#include <vector>

namespace arrow {
class ChunkedArray;
class Table;
}  // namespace arrow

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> xxhash_chunked_array(const arrow::ChunkedArray *arr);

// std::shared_ptr<arrow::ChunkedArray> search_sorted_table(const arrow::Table *data, const arrow::Table *keys,
//                                                          const std::vector<bool> &input_reversed);

}  // namespace kernels
}  // namespace daft
