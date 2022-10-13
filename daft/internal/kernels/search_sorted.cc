#include "daft/internal/kernels/search_sorted.h"

#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/chunked_array.h>
#include <arrow/table.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/logging.h>

#include <iostream>

#include "arrow/array/concatenate.h"
#include "daft/internal/kernels/memory_view.h"

namespace {

#if ARROW_VERSION_MAJOR < 7
namespace bit_util = arrow::BitUtil;
#else
namespace bit_util = arrow::bit_util;
#endif

template <typename InType>
struct SearchSortedPrimitiveSingle {
  static void Exec(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result, const bool input_reversed) {
    if (keys->GetNullCount() == 0) {
      return KernelNonNull(arr, keys, result, input_reversed);
    } else {
      return KernelWithNull(arr, keys, result, input_reversed);
    }
  }

  static void KernelNonNull(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result,
                            const bool input_reversed) {
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
        const size_t corrected_idx = input_reversed ? arr_len - mid_idx - 1 : mid_idx;
        const T mid_val = *(arr_ptr + corrected_idx);
        if (cmp(mid_val, key_val)) {
          min_idx = mid_idx + 1;
        } else {
          max_idx = mid_idx;
        }
      }
      *result_ptr = input_reversed ? arr_len - min_idx : min_idx;
    }
  }

  static void KernelWithNull(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result,
                             const bool input_reversed) {
    using T = typename InType::c_type;
    ARROW_CHECK(arr->GetNullCount() == 0);
    ARROW_CHECK(arr->type->id() == keys->type->id());

    const T *arr_ptr = arr->GetValues<T>(1);
    ARROW_CHECK(arr_ptr != NULL);

    const T *keys_ptr = keys->GetValues<T>(1);
    ARROW_CHECK(keys_ptr != NULL);

    const uint8_t *keys_bitmask_ptr = keys->GetValues<uint8_t>(0, 0);
    ARROW_CHECK(keys_bitmask_ptr != NULL);

    ARROW_CHECK(result->type->id() == arrow::Type::UINT64);
    ARROW_CHECK(result->length == keys->length);

    uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
    ARROW_CHECK(result_ptr != NULL);
    const size_t result_offset = result->offset;

    uint8_t *result_bitmask_ptr = result->GetMutableValues<uint8_t>(0, 0);
    ARROW_CHECK(result_bitmask_ptr == NULL);

    auto cmp = std::less<T>{};
    size_t min_idx = 0;
    size_t arr_len = arr->length;

    size_t max_idx = arr_len;

    size_t key_len = keys->length;
    const size_t key_offset = keys->offset;

    if (key_len == 0) {
      return;
    }

    T last_key_val = *keys_ptr;
    bool last_key_is_valid = bit_util::GetBit(keys_bitmask_ptr, key_offset);

    for (size_t key_idx = 0; key_idx < key_len; key_idx++, keys_ptr++, result_ptr++) {
      const bool curr_key_is_valid = bit_util::GetBit(keys_bitmask_ptr, key_idx + key_offset);
      const T key_val = *keys_ptr;
      /*
       * Updating only one of the indices based on the previous key
       * gives the search a big boost when keys are sorted, but slightly
       * slows down things for purely random ones.
       */
      if ((!curr_key_is_valid) || (last_key_is_valid && cmp(last_key_val, key_val))) {
        max_idx = arr_len;
      } else {
        min_idx = 0;
        max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
      }

      last_key_val = key_val;
      last_key_is_valid = curr_key_is_valid;

      while (min_idx < max_idx) {
        const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
        const size_t corrected_idx = input_reversed ? arr_len - mid_idx - 1 : mid_idx;
        const T mid_val = *(arr_ptr + corrected_idx);
        if ((!curr_key_is_valid) || cmp(mid_val, key_val)) {
          min_idx = mid_idx + 1;
        } else {
          max_idx = mid_idx;
        }
      }
      *result_ptr = input_reversed ? arr_len - min_idx : min_idx;
    }
  }
};

void search_sorted_primitive_single(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result,
                                    const bool input_reversed) {
  switch (arr->type->id()) {
    case arrow::Type::INT8:
      return SearchSortedPrimitiveSingle<arrow::Int8Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::INT16:
      return SearchSortedPrimitiveSingle<arrow::Int16Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::INT32:
      return SearchSortedPrimitiveSingle<arrow::Int32Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::INT64:
      return SearchSortedPrimitiveSingle<arrow::Int64Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::UINT8:
      return SearchSortedPrimitiveSingle<arrow::UInt8Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::UINT16:
      return SearchSortedPrimitiveSingle<arrow::UInt16Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::UINT32:
      return SearchSortedPrimitiveSingle<arrow::UInt32Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::UINT64:
      return SearchSortedPrimitiveSingle<arrow::UInt64Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::FLOAT:
      return SearchSortedPrimitiveSingle<arrow::FloatType>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::DOUBLE:
      return SearchSortedPrimitiveSingle<arrow::DoubleType>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::DATE32:
      return SearchSortedPrimitiveSingle<arrow::Date32Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::DATE64:
      return SearchSortedPrimitiveSingle<arrow::Date64Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::TIME32:
      return SearchSortedPrimitiveSingle<arrow::Time32Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::TIME64:
      return SearchSortedPrimitiveSingle<arrow::Time64Type>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::TIMESTAMP:
      return SearchSortedPrimitiveSingle<arrow::TimestampType>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::DURATION:
      return SearchSortedPrimitiveSingle<arrow::DurationType>::Exec(arr, keys, result, input_reversed);
    case arrow::Type::INTERVAL_MONTHS:
      return SearchSortedPrimitiveSingle<arrow::MonthIntervalType>::Exec(arr, keys, result, input_reversed);
    // Need custom less function for this since it uses a custom struct for the data structure
    // case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    //   return SearchSortedPrimitiveSingle<arrow::MonthDayNanoIntervalType>::Exec(arr, keys, result);
    case arrow::Type::INTERVAL_DAY_TIME:
      return SearchSortedPrimitiveSingle<arrow::DayTimeIntervalType>::Exec(arr, keys, result, input_reversed);
    default:
      ARROW_LOG(FATAL) << "Unknown arrow type" << arr->type->id();
  }
}

template <typename T>
void search_sorted_binary_single(const arrow::ArrayData *arr, const arrow::ArrayData *keys, arrow::ArrayData *result,
                                 const bool input_reversed) {
  ARROW_CHECK(arrow::is_base_binary_like(arr->type->id()));
  ARROW_CHECK(arr->type->id() == keys->type->id());
  ARROW_CHECK(result->type->id() == arrow::Type::UINT64);

  ARROW_CHECK(arr->GetNullCount() == 0);
  ARROW_CHECK(arr->type->id() == keys->type->id());

  const T *arr_index_ptr = arr->GetValues<T>(1);
  ARROW_CHECK(arr_index_ptr != NULL);

  const uint8_t *arr_data_ptr = arr->GetValues<uint8_t>(2, 0);
  ARROW_CHECK(arr_data_ptr != NULL);

  const bool has_nulls = keys->GetNullCount() > 0;
  const uint8_t *keys_bitmask_ptr = keys->GetValues<uint8_t>(0, 0);
  // ARROW_CHECK(keys_bitmask_ptr != NULL);

  const T *keys_index_ptr = keys->GetValues<T>(1);
  ARROW_CHECK(keys_index_ptr != NULL);

  const uint8_t *keys_data_ptr = keys->GetValues<uint8_t>(2, 0);
  ARROW_CHECK(keys_data_ptr != NULL);

  ARROW_CHECK(result->length == keys->length);

  uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
  ARROW_CHECK(result_ptr != NULL);

  uint8_t *result_bitmask_ptr = result->GetMutableValues<uint8_t>(0, 0);
  ARROW_CHECK(result_bitmask_ptr == NULL);

  size_t min_idx = 0;
  size_t arr_len = arr->length;
  size_t max_idx = arr_len;
  size_t key_len = keys->length;
  const size_t key_offset = keys->offset;

  size_t last_key_idx = 0;

  if (key_len == 0) {
    return;
  }
  bool last_key_is_valid = true;
  if (has_nulls) {
    last_key_is_valid = bit_util::GetBit(keys_bitmask_ptr, key_offset);
  }
  for (size_t key_idx = 0; key_idx < key_len; key_idx++, result_ptr++) {
    bool key_is_valid = true;
    if (has_nulls) {
      key_is_valid = bit_util::GetBit(keys_bitmask_ptr, key_idx + key_offset);
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
    if ((!key_is_valid) ||
        (last_key_is_valid && (daft::kernels::strcmpNoTerminator(keys_data_ptr + last_key_offset, keys_data_ptr + curr_key_offset,
                                                                 last_key_size, curr_key_size) < 0))) {
      max_idx = arr_len;
    } else {
      min_idx = 0;
      max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
    }

    last_key_idx = key_idx;
    last_key_is_valid = key_is_valid;

    while (min_idx < max_idx) {
      const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
      const size_t corrected_idx = input_reversed ? arr_len - mid_idx - 1 : mid_idx;
      const size_t mid_arr_offset = arr_index_ptr[corrected_idx];
      const size_t mid_arr_size = arr_index_ptr[corrected_idx + 1] - mid_arr_offset;

      if ((!key_is_valid) || (daft::kernels::strcmpNoTerminator(arr_data_ptr + mid_arr_offset, keys_data_ptr + curr_key_offset,
                                                                mid_arr_size, curr_key_size) < 0)) {
        min_idx = mid_idx + 1;
      } else {
        max_idx = mid_idx;
      }
    }
    *result_ptr = input_reversed ? arr_len - min_idx : min_idx;
  }
}

void add_primitive_memory_view_to_comp_view(daft::kernels::CompositeView &comp_view, const std::shared_ptr<arrow::ArrayData> arr) {
  ARROW_CHECK(arrow::is_primitive(arr->type->id()));
  switch (arr->type->id()) {
    case arrow::Type::INT8:
      return comp_view.AddPrimitiveMemoryView<arrow::Int8Type>(arr);
    case arrow::Type::INT16:
      return comp_view.AddPrimitiveMemoryView<arrow::Int16Type>(arr);
    case arrow::Type::INT32:
      return comp_view.AddPrimitiveMemoryView<arrow::Int32Type>(arr);
    case arrow::Type::INT64:
      return comp_view.AddPrimitiveMemoryView<arrow::Int64Type>(arr);
    case arrow::Type::UINT8:
      return comp_view.AddPrimitiveMemoryView<arrow::UInt8Type>(arr);
    case arrow::Type::UINT16:
      return comp_view.AddPrimitiveMemoryView<arrow::UInt16Type>(arr);
    case arrow::Type::UINT32:
      return comp_view.AddPrimitiveMemoryView<arrow::UInt32Type>(arr);
    case arrow::Type::UINT64:
      return comp_view.AddPrimitiveMemoryView<arrow::UInt64Type>(arr);
    case arrow::Type::FLOAT:
      return comp_view.AddPrimitiveMemoryView<arrow::FloatType>(arr);
    case arrow::Type::DOUBLE:
      return comp_view.AddPrimitiveMemoryView<arrow::DoubleType>(arr);
    case arrow::Type::DATE32:
      return comp_view.AddPrimitiveMemoryView<arrow::Date32Type>(arr);
    case arrow::Type::DATE64:
      return comp_view.AddPrimitiveMemoryView<arrow::Date64Type>(arr);
    case arrow::Type::TIME32:
      return comp_view.AddPrimitiveMemoryView<arrow::Time32Type>(arr);
    case arrow::Type::TIME64:
      return comp_view.AddPrimitiveMemoryView<arrow::Time64Type>(arr);
    case arrow::Type::TIMESTAMP:
      return comp_view.AddPrimitiveMemoryView<arrow::TimestampType>(arr);
    case arrow::Type::DURATION:
      return comp_view.AddPrimitiveMemoryView<arrow::DurationType>(arr);
    case arrow::Type::INTERVAL_MONTHS:
      return comp_view.AddPrimitiveMemoryView<arrow::MonthIntervalType>(arr);
    // Need custom less function for this since it uses a custom struct for the data structure
    // case arrow::Type::INTERVAL_MONTH_DAY_NANO:
    //   return comp_view.AddPrimitiveMemoryView<arrow::MonthDayNanoIntervalType>(arr);
    case arrow::Type::INTERVAL_DAY_TIME:
      return comp_view.AddPrimitiveMemoryView<arrow::DayTimeIntervalType>(arr);
    default:
      break;
  }
}

void add_binary_memory_view_to_comp_view(daft::kernels::CompositeView &comp_view, const std::shared_ptr<arrow::ArrayData> arr) {
  if (arrow::is_binary_like(arr->type->id())) {
    return comp_view.AddBinaryMemoryView<arrow::BinaryType>(arr);
  } else if (arrow::is_large_binary_like(arr->type->id())) {
    return comp_view.AddBinaryMemoryView<arrow::LargeBinaryType>(arr);
  } else {
    ARROW_LOG(FATAL) << "Unsupported Type " << arrow::is_large_binary_like(arr->type->id());
  }
}

std::shared_ptr<arrow::Array> search_sorted_multiple_columns(const std::vector<std::shared_ptr<arrow::ArrayData>> &sorted,
                                                             const std::vector<std::shared_ptr<arrow::ArrayData>> &keys,
                                                             const std::vector<bool> &input_reversed) {
  daft::kernels::CompositeView sorted_comp_view{}, keys_comp_view{};
  for (auto arr : sorted) {
    ARROW_CHECK_EQ(arr->GetNullCount(), 0);
    if (arrow::is_primitive(arr->type->id())) {
      add_primitive_memory_view_to_comp_view(sorted_comp_view, arr);
    } else if (arrow::is_base_binary_like(arr->type->id())) {
      add_binary_memory_view_to_comp_view(sorted_comp_view, arr);
    } else {
      ARROW_LOG(FATAL) << "Unsupported Type " << arr->type->id();
    }
  }
  // bool key_has_nulls = false;
  for (auto arr : keys) {
    // key_has_nulls = key_has_nulls | (arr->GetNullCount() > 0);
    if (arrow::is_primitive(arr->type->id())) {
      add_primitive_memory_view_to_comp_view(keys_comp_view, arr);
    } else if (arrow::is_base_binary_like(arr->type->id())) {
      add_binary_memory_view_to_comp_view(keys_comp_view, arr);
    } else {
      ARROW_LOG(FATAL) << "Unsupported Type " << arr->type->id();
    }
  }

  size_t min_idx = 0;
  size_t arr_len = sorted[0]->length;
  size_t max_idx = arr_len;
  size_t key_len = keys[0]->length;
  size_t last_key_idx = 0;

  std::vector<std::shared_ptr<arrow::Buffer>> result_buffers{2};
  result_buffers[1] = arrow::AllocateBuffer(sizeof(arrow::UInt64Type::c_type) * key_len).ValueOrDie();
  std::shared_ptr<arrow::ArrayData> result = arrow::ArrayData::Make(std::make_shared<arrow::UInt64Type>(), key_len, result_buffers);

  uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
  ARROW_CHECK(result_ptr != NULL);

  for (size_t key_idx = 0; key_idx < key_len; key_idx++, result_ptr++) {
    /*
     * Updating only one of the indices based on the previous key
     * gives the search a big boost when keys are sorted, but slightly
     * slows down things for purely random ones.
     */
    if (keys_comp_view.Compare(keys_comp_view, last_key_idx, key_idx, input_reversed) < 0) {
      max_idx = arr_len;
    } else {
      min_idx = 0;
      max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
    }

    last_key_idx = key_idx;

    while (min_idx < max_idx) {
      const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
      if (sorted_comp_view.Compare(keys_comp_view, mid_idx, key_idx, input_reversed) < 0) {
        min_idx = mid_idx + 1;
      } else {
        max_idx = mid_idx;
      }
    }
    *result_ptr = min_idx;
  }
  return arrow::MakeArray(result);
}

}  // namespace

namespace daft {
namespace kernels {
std::shared_ptr<arrow::Array> search_sorted_single_array(const arrow::Array *arr, const arrow::Array *keys, const bool input_reversed) {
  ARROW_CHECK(arr != NULL);
  ARROW_CHECK(keys != NULL);
  const size_t size = keys->length();
  std::vector<std::shared_ptr<arrow::Buffer>> result_buffers{2};
  result_buffers[1] = arrow::AllocateBuffer(sizeof(arrow::UInt64Type::c_type) * size).ValueOrDie();
  std::shared_ptr<arrow::ArrayData> result = arrow::ArrayData::Make(std::make_shared<arrow::UInt64Type>(), size, result_buffers, 0);
  ARROW_CHECK(result.get() != NULL);
  if (arrow::is_primitive(arr->type()->id())) {
    search_sorted_primitive_single(arr->data().get(), keys->data().get(), result.get(), input_reversed);
  } else if (arrow::is_binary_like(arr->type()->id())) {
    search_sorted_binary_single<arrow::BinaryType::offset_type>(arr->data().get(), keys->data().get(), result.get(), input_reversed);
  } else if (arrow::is_large_binary_like(arr->type()->id())) {
    search_sorted_binary_single<arrow::LargeBinaryType::offset_type>(arr->data().get(), keys->data().get(), result.get(), input_reversed);
  } else {
    ARROW_LOG(FATAL) << "Unsupported Type " << arr->type()->id();
  }
  return arrow::MakeArray(result);
}

std::shared_ptr<arrow::ChunkedArray> search_sorted_chunked_array(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *keys,
                                                                 const bool input_reversed) {
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
    result_arrays.push_back(search_sorted_single_array(arr_single_chunk.get(), keys->chunk(i).get(), input_reversed));
  }
  return arrow::ChunkedArray::Make(result_arrays, std::make_shared<arrow::UInt64Type>()).ValueOrDie();
}

std::shared_ptr<arrow::ChunkedArray> search_sorted_table(const arrow::Table *data, const arrow::Table *keys,
                                                         const std::vector<bool> &input_reversed) {
  ARROW_CHECK_NE(data, NULL);
  ARROW_CHECK_NE(keys, NULL);
  const auto data_schema = data->schema();
  const auto key_schema = keys->schema();
  ARROW_CHECK(data_schema->Equals(key_schema));
  if ((data_schema->num_fields() == 0) || (keys->num_rows() == 0)) {
    return arrow::ChunkedArray::Make({}, std::make_shared<arrow::UInt64Type>()).ValueOrDie();
  } else if (data_schema->num_fields() == 1) {
    ARROW_CHECK_EQ(input_reversed.size(), 1);
    return search_sorted_chunked_array(data->column(0).get(), keys->column(0).get(), input_reversed[0]);
  } else {
    const auto combined_data_table = data->CombineChunks().ValueOrDie();
    const auto combined_keys_table = keys->CombineChunks().ValueOrDie();
    std::vector<std::shared_ptr<arrow::ArrayData>> data_vector, key_vector;
    for (auto chunked_arr : combined_data_table->columns()) {
      ARROW_CHECK_EQ(chunked_arr->num_chunks(), 1);
      data_vector.push_back(chunked_arr->chunk(0)->data());
    }
    for (auto chunked_arr : combined_keys_table->columns()) {
      ARROW_CHECK_EQ(chunked_arr->num_chunks(), 1);
      key_vector.push_back(chunked_arr->chunk(0)->data());
    }
    auto result = search_sorted_multiple_columns(data_vector, key_vector, input_reversed);
    return arrow::ChunkedArray::Make({result}, std::make_shared<arrow::UInt64Type>()).ValueOrDie();
  }
}

}  // namespace kernels
}  // namespace daft
