#include <arrow/api.h>
#include <arrow/array.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/chunked_array.h>
#include <arrow/table.h>
#include <arrow/type_traits.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/logging.h>

#include "daft/internal/kernels/xxhash.h"

namespace {

#if ARROW_VERSION_MAJOR < 7
namespace bit_util = arrow::BitUtil;
#else
namespace bit_util = arrow::bit_util;
#endif

template <size_t WordSize>
struct HashingPrimativeArray {
  static void Exec(const arrow::ArrayData *arr, arrow::ArrayData *result) {
    const size_t arr_len = arr->length;
    const size_t bit_offset = arr->offset;
    const uint8_t *data_ptr = arr->GetValues<uint8_t>(1, bit_offset * WordSize);
    ARROW_CHECK(data_ptr != NULL);

    const bool has_nulls = arr->GetNullCount() > 0;
    const uint8_t *bitmask_ptr = arr->GetValues<uint8_t>(0, 0);

    uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
    ARROW_CHECK(result_ptr != NULL);

    ARROW_CHECK(result->GetMutableValues<uint8_t>(0) == NULL) << "bitmask should be NULL";
    const size_t result_len = result->length;
    ARROW_CHECK(arr_len == result_len);

    for (size_t idx = 0; idx < arr_len; ++idx) {
      size_t length = WordSize;

      if (has_nulls) {
        const bool is_valid = bit_util::GetBit(bitmask_ptr, idx + bit_offset);
        if (!is_valid) {
          length = 0;
        }
      }
      result_ptr[idx] = XXH3_64bits(data_ptr + idx * WordSize, length);
    }
  }
};

template <typename IndexType>
struct HashingBinaryArray {
  static void Exec(const arrow::ArrayData *arr, arrow::ArrayData *result) {
    ARROW_CHECK(arrow::is_base_binary_like(arr->type->id()));
    ARROW_CHECK(result->type->id() == arrow::Type::UINT64);
    const IndexType *arr_index_ptr = arr->GetValues<IndexType>(1);
    ARROW_CHECK(arr_index_ptr != NULL);

    const uint8_t *arr_data_ptr = arr->GetValues<uint8_t>(2, 0);
    ARROW_CHECK(arr_data_ptr != NULL);

    const uint8_t *bitmask_ptr = arr->GetValues<uint8_t>(0, 0);
    const bool has_nulls = arr->GetNullCount() > 0;

    uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
    ARROW_CHECK(result_ptr != NULL);
    ARROW_CHECK(result->GetMutableValues<uint8_t>(0) == NULL) << "bitmask should be NULL";

    const size_t arr_len = arr->length;
    const size_t bit_offset = arr->offset;

    const size_t result_len = result->length;
    ARROW_CHECK(arr_len == result_len);

    for (size_t idx = 0; idx < arr_len; ++idx) {
      const size_t curr_offset = arr_index_ptr[idx];
      size_t curr_size = arr_index_ptr[idx + 1] - curr_offset;

      if (has_nulls && !bit_util::GetBit(bitmask_ptr, idx + bit_offset)) {
        curr_size = 0;
      }

      result_ptr[idx] = XXH3_64bits(arr_data_ptr + curr_offset, curr_size);
    }
  }
};

void hash_primative_array(const arrow::ArrayData *arr, arrow::ArrayData *result) {
  switch (arrow::bit_width(arr->type->id()) >> 3) {
    case 1:
      return HashingPrimativeArray<1>::Exec(arr, result);
    case 2:
      return HashingPrimativeArray<2>::Exec(arr, result);
    case 4:
      return HashingPrimativeArray<4>::Exec(arr, result);
    case 8:
      return HashingPrimativeArray<8>::Exec(arr, result);
    case 16:
      return HashingPrimativeArray<16>::Exec(arr, result);
    case 32:
      return HashingPrimativeArray<32>::Exec(arr, result);
    default:
      ARROW_LOG(FATAL) << "Unknown bitwidth for arrow type" << arr->type->id();
  }
}

std::shared_ptr<arrow::Array> hash_single_array(const arrow::Array *arr) {
  ARROW_CHECK(arr != NULL);
  const size_t size = arr->length();
  std::vector<std::shared_ptr<arrow::Buffer>> result_buffers{2};
  result_buffers[1] = arrow::AllocateBuffer(sizeof(arrow::UInt64Type::c_type) * size).ValueOrDie();
  std::shared_ptr<arrow::ArrayData> result = arrow::ArrayData::Make(std::make_shared<arrow::UInt64Type>(), size, result_buffers, 0);
  ARROW_CHECK(result.get() != NULL);
  if (arrow::is_primitive(arr->type()->id())) {
    hash_primative_array(arr->data().get(), result.get());
  } else if (arrow::is_binary_like(arr->type()->id())) {
    HashingBinaryArray<arrow::BinaryType::offset_type>::Exec(arr->data().get(), result.get());
  } else if (arrow::is_large_binary_like(arr->type()->id())) {
    HashingBinaryArray<arrow::LargeBinaryType::offset_type>::Exec(arr->data().get(), result.get());
  } else {
    ARROW_LOG(FATAL) << "Unsupported Type " << arr->type()->id();
  }
  return arrow::MakeArray(result);
}

}  // namespace

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> xxhash_chunked_array(const arrow::ChunkedArray *arr) {
  ARROW_CHECK_NE(arr, NULL);
  size_t num_chunks = arr->num_chunks();
  std::vector<std::shared_ptr<arrow::Array>> result_arrays;
  for (size_t i = 0; i < num_chunks; ++i) {
    result_arrays.push_back(hash_single_array(arr->chunk(i).get()));
  }
  return arrow::ChunkedArray::Make(result_arrays, std::make_shared<arrow::UInt64Type>()).ValueOrDie();
}

}  // namespace kernels
}  // namespace daft
