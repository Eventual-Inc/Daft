#pragma once

#include <memory>
#include <vector>

namespace arrow {
class ChunkedArray;
class Table;
}  // namespace arrow

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> xxhash_chunked_array(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *seed);

}  // namespace kernels
}  // namespace daft
