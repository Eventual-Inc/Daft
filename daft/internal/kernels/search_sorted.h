#pragma once

#include <memory>

namespace arrow {
class ChunkedArray;
class Table;
}  // namespace arrow

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> search_sorted_chunked_array(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *keys);
std::shared_ptr<arrow::ChunkedArray> search_sorted_table(const arrow::Table *data, const arrow::Table *keys);

}  // namespace kernels
}  // namespace daft
