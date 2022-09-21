#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/logging.h>

#include <iostream>

#include "daft/internal/kernels/memory_view.h"

namespace {

template <typename InType>
struct SearchSortedPrimativeSingle {
  static void Exec(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result) {
    if (keys->GetNullCount() == 0) {
      return KernelNonNull(arr, keys, result);
    } else {
      return KernelWithNull(arr, keys, result);
    }
  }

  static void KernelNonNull(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result) {
    using T = typename InType::c_type;
    ARROW_CHECK(arr->GetNullCount() == 0);
    ARROW_CHECK(keys->GetNullCount() == 0);

    const T *arr_ptr = arr->GetValues<T>(1);
    ARROW_CHECK(arr_ptr != NULL);

    const T *keys_ptr = keys->GetValues<T>(1);
    ARROW_CHECK(keys_ptr != NULL);

    ARROW_CHECK(result->type->id() == arrow::Type::UINT64);

    uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
    ARROW_CHECK(result_ptr != NULL);

    auto cmp = std::less<T>{};
    size_t min_idx = 0;
    size_t arr_len = arr->length;
    size_t max_idx = arr_len;
    size_t key_len = keys->length;

    if (key_len == 0) {
      return;
    }

    T last_key_val = *keys_ptr;

    for (; key_len > 0; key_len--, keys_ptr++, result_ptr++) {
      const T key_val = *keys_ptr;
      /*
       * Updating only one of the indices based on the previous key
       * gives the search a big boost when keys are sorted, but slightly
       * slows down things for purely random ones.
       */
      if (cmp(last_key_val, key_val)) {
        max_idx = arr_len;
      } else {
        min_idx = 0;
        max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
      }

      last_key_val = key_val;

      while (min_idx < max_idx) {
        const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
        const T mid_val = *(arr_ptr + mid_idx);
        if (cmp(mid_val, key_val)) {
          min_idx = mid_idx + 1;
        } else {
          max_idx = mid_idx;
        }
      }
      *result_ptr = min_idx;
    }
  }

  static void KernelWithNull(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result) {
    using T = typename InType::c_type;
    ARROW_CHECK(arr->GetNullCount() == 0);

    const T *arr_ptr = arr->GetValues<T>(1);
    ARROW_CHECK(arr_ptr != NULL);

    const T *keys_ptr = keys->GetValues<T>(1);
    ARROW_CHECK(keys_ptr != NULL);

    const uint8_t *keys_bitmask_ptr = keys->GetValues<uint8_t>(0);
    ARROW_CHECK(keys_bitmask_ptr != NULL);

    ARROW_CHECK(result->type->id() == arrow::Type::UINT64);
    ARROW_CHECK(result->length == keys->length);

    uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
    ARROW_CHECK(result_ptr != NULL);

    uint8_t *result_bitmask_ptr = result->GetMutableValues<uint8_t>(0);
    ARROW_CHECK(result_bitmask_ptr != NULL);

    auto cmp = std::less<T>{};
    size_t min_idx = 0;
    size_t arr_len = arr->length;
    size_t max_idx = arr_len;
    size_t key_len = keys->length;

    if (key_len == 0) {
      return;
    }

    T last_key_val = *keys_ptr;

    for (size_t key_idx = 0; key_idx < key_len; key_idx++, keys_ptr++, result_ptr++) {
      const bool key_bit = arrow::bit_util::GetBit(keys_bitmask_ptr, key_idx);
      arrow::bit_util::SetBitTo(result_bitmask_ptr, key_idx, key_bit);
      if (!key_bit) {
        continue;
      }
      const T key_val = *keys_ptr;
      /*
       * Updating only one of the indices based on the previous key
       * gives the search a big boost when keys are sorted, but slightly
       * slows down things for purely random ones.
       */
      if (cmp(last_key_val, key_val)) {
        max_idx = arr_len;
      } else {
        min_idx = 0;
        max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
      }

      last_key_val = key_val;

      while (min_idx < max_idx) {
        const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
        const T mid_val = *(arr_ptr + mid_idx);
        if (cmp(mid_val, key_val)) {
          min_idx = mid_idx + 1;
        } else {
          max_idx = mid_idx;
        }
      }
      *result_ptr = min_idx;
    }
  }
};

void search_sorted_primative_single(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result) {
  switch (arr->type->id()) {
    case arrow::Type::INT8:
      return SearchSortedPrimativeSingle<arrow::Int8Type>::Exec(arr, keys, result);
    case arrow::Type::INT16:
      return SearchSortedPrimativeSingle<arrow::Int16Type>::Exec(arr, keys, result);
    case arrow::Type::INT32:
      return SearchSortedPrimativeSingle<arrow::Int32Type>::Exec(arr, keys, result);
    case arrow::Type::INT64:
      return SearchSortedPrimativeSingle<arrow::Int64Type>::Exec(arr, keys, result);
    case arrow::Type::UINT8:
      return SearchSortedPrimativeSingle<arrow::UInt8Type>::Exec(arr, keys, result);
    case arrow::Type::UINT16:
      return SearchSortedPrimativeSingle<arrow::UInt16Type>::Exec(arr, keys, result);
    case arrow::Type::UINT32:
      return SearchSortedPrimativeSingle<arrow::UInt32Type>::Exec(arr, keys, result);
    case arrow::Type::UINT64:
      return SearchSortedPrimativeSingle<arrow::UInt64Type>::Exec(arr, keys, result);
    case arrow::Type::FLOAT:
      return SearchSortedPrimativeSingle<arrow::FloatType>::Exec(arr, keys, result);
    case arrow::Type::DOUBLE:
      return SearchSortedPrimativeSingle<arrow::DoubleType>::Exec(arr, keys, result);
    default:
      break;
  }
}

}  // namespace

std::shared_ptr<arrow::Array> search_sorted(const arrow::Array *arr, const arrow::Array *keys) {
  ARROW_CHECK(arr != NULL);
  ARROW_CHECK(keys != NULL);
  const size_t size = keys->length();
  std::vector<std::shared_ptr<arrow::Buffer>> result_buffers{2};
  if (keys->null_count() > 0) {
    result_buffers[0] = arrow::AllocateBitmap(size).ValueOrDie();
  }
  result_buffers[1] = arrow::AllocateBuffer(sizeof(arrow::UInt64Type::c_type) * size).ValueOrDie();
  std::shared_ptr<arrow::ArrayData> result =
      arrow::ArrayData::Make(std::make_shared<arrow::UInt64Type>(), size, result_buffers, keys->null_count());
  ARROW_CHECK(result.get() != NULL);
  if (arrow::is_primitive(arr->type()->id())) {
    search_sorted_primative_single(arr->data().get(), keys->data().get(), result.get());
  }
  return arrow::MakeArray(result);
}
