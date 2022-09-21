#include <memory>

namespace arrow {
class ChunkedArray;
}  // namespace arrow

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> search_sorted_chunked(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *keys);

}  // namespace kernels
}  // namespace daft
