
namespace arrow {
class Array;

} // namespace arrow

std::shared_ptr<arrow::Array> search_sorted(const arrow::Array *arr,
                                            const arrow::Array *keys);
