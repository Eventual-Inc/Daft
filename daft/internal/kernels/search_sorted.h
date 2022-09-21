
namespace arrow {
class Array;

}  // namespace arrow

// std::shared_ptr<arrow::Array> search_sorted(const arrow::Array *arr, const arrow::Array *keys);
std::shared_ptr<arrow::ChunkedArray> search_sorted_chunked(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *keys);
