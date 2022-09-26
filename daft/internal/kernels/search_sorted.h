#pragma once

#include <memory>
#include <vector>

namespace arrow {
class ChunkedArray;
class Table;
}  // namespace arrow

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> search_sorted_chunked_array(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *keys,
                                                                 const bool input_reversed);
std::shared_ptr<arrow::ChunkedArray> search_sorted_table(const arrow::Table *data, const arrow::Table *keys,
                                                         const std::vector<bool> &input_reversed);

}  // namespace kernels
}  // namespace daft
