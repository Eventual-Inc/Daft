#include <arrow/array.h>
#include <arrow/array/data.h>
#include <arrow/type_traits.h>

#include <iostream>

#include "daft/internal/kernels/memory_view.h"

namespace {

template <typename InType>
struct SearchSortedPrimativeSingle {
  static void Exec(const arrow::ArrayData *arr, const arrow::ArrayData *keys,
                   arrow::ArrayData *result) {
    using T = typename InType::c_type;
    std::cout << typeid(T).name() << std::endl;
    return;
  }
};

void search_sorted_primative_single(const arrow::ArrayData *arr,
                                    const arrow::ArrayData *keys,
                                    arrow::ArrayData *result) {
  switch (arr->type->id()) {
    case arrow::Type::INT8:
      return SearchSortedPrimativeSingle<arrow::Int8Type>::Exec(arr, keys,
                                                                result);
    case arrow::Type::INT16:
      return SearchSortedPrimativeSingle<arrow::Int16Type>::Exec(arr, keys,
                                                                 result);
    case arrow::Type::INT32:
      return SearchSortedPrimativeSingle<arrow::Int32Type>::Exec(arr, keys,
                                                                 result);
    case arrow::Type::INT64:
      return SearchSortedPrimativeSingle<arrow::Int64Type>::Exec(arr, keys,
                                                                 result);
    case arrow::Type::UINT8:
      return SearchSortedPrimativeSingle<arrow::UInt8Type>::Exec(arr, keys,
                                                                 result);
    case arrow::Type::UINT16:
      return SearchSortedPrimativeSingle<arrow::UInt16Type>::Exec(arr, keys,
                                                                  result);
    case arrow::Type::UINT32:
      return SearchSortedPrimativeSingle<arrow::UInt32Type>::Exec(arr, keys,
                                                                  result);
    case arrow::Type::UINT64:
      return SearchSortedPrimativeSingle<arrow::UInt64Type>::Exec(arr, keys,
                                                                  result);
    case arrow::Type::FLOAT:
      return SearchSortedPrimativeSingle<arrow::FloatType>::Exec(arr, keys,
                                                                 result);
    case arrow::Type::DOUBLE:
      return SearchSortedPrimativeSingle<arrow::DoubleType>::Exec(arr, keys,
                                                                  result);
    default:
      break;
  }
}

}  // namespace

std::shared_ptr<arrow::Array> search_sorted(const arrow::Array *arr,
                                            const arrow::Array *keys) {
  const size_t size = keys->length();
  std::shared_ptr<arrow::ArrayData> result = arrow::ArrayData::Make(
      std::make_shared<arrow::UInt64Type>(), size, keys->null_count());
  if (arrow::is_primitive(arr->type()->id())) {
    search_sorted_primative_single(arr->data().get(), keys->data().get(),
                                   result.get());
  }
  return 0;
}
