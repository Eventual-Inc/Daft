#include "daft/internal/kernels/search_sorted.h"

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/chunked_array.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/logging.h>

#include <iostream>

#include "arrow/array/concatenate.h"

namespace {

#if ARROW_VERSION_MAJOR < 7
namespace bit_util = arrow::BitUtil;
#else
namespace bit_util = arrow::bit_util;
#endif

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
    ARROW_CHECK(arr->type->id() == keys->type->id());

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
    ARROW_CHECK(arr->type->id() == keys->type->id());

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
      const bool key_bit = bit_util::GetBit(keys_bitmask_ptr, key_idx);
      bit_util::SetBitTo(result_bitmask_ptr, key_idx, key_bit);
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
    case arrow::Type::DATE32:
      return SearchSortedPrimativeSingle<arrow::Date32Type>::Exec(arr, keys, result);
    case arrow::Type::DATE64:
      return SearchSortedPrimativeSingle<arrow::Date64Type>::Exec(arr, keys, result);
    case arrow::Type::TIME32:
      return SearchSortedPrimativeSingle<arrow::Time32Type>::Exec(arr, keys, result);
    case arrow::Type::TIME64:
      return SearchSortedPrimativeSingle<arrow::Time64Type>::Exec(arr, keys, result);
    case arrow::Type::TIMESTAMP:
      return SearchSortedPrimativeSingle<arrow::TimestampType>::Exec(arr, keys, result);
    case arrow::Type::DURATION:
      return SearchSortedPrimativeSingle<arrow::DurationType>::Exec(arr, keys, result);
    case arrow::Type::INTERVAL_MONTHS:
      return SearchSortedPrimativeSingle<arrow::MonthIntervalType>::Exec(arr, keys, result);
    // Need custom less function for this since it uses a custom struct for the data structure
    // case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    //   return SearchSortedPrimativeSingle<arrow::MonthDayNanoIntervalType>::Exec(arr, keys, result);
    case arrow::Type::INTERVAL_DAY_TIME:
      return SearchSortedPrimativeSingle<arrow::DayTimeIntervalType>::Exec(arr, keys, result);
    default:
      break;
  }
}

int strcmpNoTerminator(const uint8_t *str1, const uint8_t *str2, size_t str1len, size_t str2len) {
  // https://stackoverflow.com/questions/24770665/comparing-2-char-with-different-lengths-without-null-terminators
  // Get the length of the shorter string
  size_t len = str1len < str2len ? str1len : str2len;
  // Compare the strings up until one ends
  int cmp = memcmp(str1, str2, len);
  // If they weren't equal, we've got our result
  // If they are equal and the same length, they matched
  if (cmp != 0 || str1len == str2len) {
    return cmp;
  }
  // If they were equal but one continues on, the shorter string is
  // lexicographically smaller
  return str1len < str2len ? -1 : 1;
}

template <typename T>
void search_sorted_binary_single(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result) {
  ARROW_CHECK(arrow::is_base_binary_like(arr->type->id()));
  ARROW_CHECK(arr->type->id() == keys->type->id());
  ARROW_CHECK(result->type->id() == arrow::Type::UINT64);

  ARROW_CHECK(arr->GetNullCount() == 0);
  ARROW_CHECK(arr->type->id() == keys->type->id());

  const T *arr_index_ptr = arr->GetValues<T>(1);
  ARROW_CHECK(arr_index_ptr != NULL);

  const uint8_t *arr_data_ptr = arr->GetValues<uint8_t>(2);
  ARROW_CHECK(arr_data_ptr != NULL);

  const bool has_nulls = keys->GetNullCount() > 0;
  const uint8_t *keys_bitmask_ptr = keys->GetValues<uint8_t>(0);
  // ARROW_CHECK(keys_bitmask_ptr != NULL);

  const T *keys_index_ptr = keys->GetValues<T>(1);
  ARROW_CHECK(keys_index_ptr != NULL);

  const uint8_t *keys_data_ptr = keys->GetValues<uint8_t>(2);
  ARROW_CHECK(keys_data_ptr != NULL);

  ARROW_CHECK(result->length == keys->length);

  uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
  ARROW_CHECK(result_ptr != NULL);

  uint8_t *result_bitmask_ptr = result->GetMutableValues<uint8_t>(0);
  ARROW_CHECK(!has_nulls || (result_bitmask_ptr != NULL));

  size_t min_idx = 0;
  size_t arr_len = arr->length;
  size_t max_idx = arr_len;
  size_t key_len = keys->length;
  size_t last_key_idx = 0;

  if (key_len == 0) {
    return;
  }

  for (size_t key_idx = 0; key_idx < key_len; key_idx++, result_ptr++) {
    if (has_nulls) {
      const bool key_bit = bit_util::GetBit(keys_bitmask_ptr, key_idx);
      bit_util::SetBitTo(result_bitmask_ptr, key_idx, key_bit);
      if (!key_bit) {
        continue;
      }
    }
    const size_t curr_key_offset = keys_index_ptr[key_idx];
    const size_t curr_key_size = keys_index_ptr[key_idx + 1] - curr_key_offset;
    const size_t last_key_offset = keys_index_ptr[last_key_idx];
    const size_t last_key_size = keys_index_ptr[last_key_idx + 1] - last_key_offset;

    /*
     * Updating only one of the indices based on the previous key
     * gives the search a big boost when keys are sorted, but slightly
     * slows down things for purely random ones.
     */
    if (strcmpNoTerminator(keys_data_ptr + last_key_offset, keys_data_ptr + curr_key_offset, last_key_size, curr_key_size) < 0) {
      max_idx = arr_len;
    } else {
      min_idx = 0;
      max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
    }

    last_key_idx = key_idx;

    while (min_idx < max_idx) {
      const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
      const size_t mid_arr_offset = arr_index_ptr[mid_idx];
      const size_t mid_arr_size = arr_index_ptr[mid_idx + 1] - mid_arr_offset;

      if (strcmpNoTerminator(arr_data_ptr + mid_arr_offset, keys_data_ptr + curr_key_offset, mid_arr_size, curr_key_size) < 0) {
        min_idx = mid_idx + 1;
      } else {
        max_idx = mid_idx;
      }
    }
    *result_ptr = min_idx;
  }
}

}  // namespace

namespace daft {
namespace kernels {
std::shared_ptr<arrow::Array> search_sorted_single_array(const arrow::Array *arr, const arrow::Array *keys) {
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
  } else if (arrow::is_binary_like(arr->type()->id())) {
    search_sorted_binary_single<arrow::BinaryType::offset_type>(arr->data().get(), keys->data().get(), result.get());
  } else if (arrow::is_large_binary_like(arr->type()->id())) {
    search_sorted_binary_single<arrow::LargeBinaryType::offset_type>(arr->data().get(), keys->data().get(), result.get());
  } else {
    ARROW_LOG(FATAL) << "Unsupported Type " << arrow::is_large_binary_like(arr->type()->id());
  }
  return arrow::MakeArray(result);
}

std::shared_ptr<arrow::ChunkedArray> search_sorted_chunked(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *keys) {
  ARROW_CHECK_NE(arr, NULL);
  ARROW_CHECK_NE(keys, NULL);

  std::shared_ptr<arrow::Array> arr_single_chunk;
  if (arr->num_chunks() == 1) {
    arr_single_chunk = arr->chunk(0);
  } else {
    arr_single_chunk = arrow::Concatenate(arr->chunks()).ValueOrDie();
  }
  size_t num_key_chunks = keys->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> result_arrays;
  for (size_t i = 0; i < num_key_chunks; ++i) {
    result_arrays.push_back(search_sorted_single_array(arr_single_chunk.get(), keys->chunk(i).get()));
  }
  return arrow::ChunkedArray::Make(result_arrays, std::make_shared<arrow::UInt64Type>()).ValueOrDie();
}

}  // namespace kernels
}  // namespace daft
