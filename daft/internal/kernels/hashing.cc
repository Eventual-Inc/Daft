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

template <int64_t WordSize>
struct HashingPrimitiveArray {
  static void Exec(const arrow::ArrayData *arr, const arrow::ArrayData *seed, arrow::ArrayData *result) {
    const int64_t arr_len = arr->length;
    const int64_t bit_offset = arr->offset;
    const uint8_t *data_ptr = arr->GetValues<uint8_t>(1, bit_offset * WordSize);
    ARROW_CHECK(data_ptr != NULL);

    const bool has_nulls = arr->GetNullCount() > 0;
    const uint8_t *bitmask_ptr = arr->GetValues<uint8_t>(0, 0);

    uint64_t *result_ptr = result->GetMutableValues<uint64_t>(1);
    ARROW_CHECK(result_ptr != NULL);
    ARROW_CHECK(result->type->id() == arrow::Type::UINT64);

    ARROW_CHECK(result->GetMutableValues<uint8_t>(0) == NULL) << "bitmask should be NULL";
    const int64_t result_len = result->length;
    ARROW_CHECK(arr_len == result_len);

    const bool has_seed = seed != NULL;
    const uint64_t *seed_ptr = NULL;
    if (has_seed) {
      ARROW_CHECK(arr_len == seed->length);
      ARROW_CHECK(seed->type->id() == arrow::Type::UINT64);
      seed_ptr = seed->GetValues<uint64_t>(1);
    }

    for (int64_t idx = 0; idx < arr_len; ++idx) {
      int64_t length = WordSize;

      if (has_nulls && !bit_util::GetBit(bitmask_ptr, idx + bit_offset)) {
        length = 0;
      }
      uint64_t seed_val = 0;
      if (has_seed) {
        seed_val = seed_ptr[idx];
      }
      result_ptr[idx] = XXH3_64bits_withSeed(data_ptr + idx * WordSize, length, seed_val);
    }
  }
};

template <typename IndexType>
struct HashingBinaryArray {
  static void Exec(const arrow::ArrayData *arr, const arrow::ArrayData *seed, arrow::ArrayData *result) {
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

    const int64_t arr_len = arr->length;
    const int64_t bit_offset = arr->offset;

    const int64_t result_len = result->length;
    ARROW_CHECK(arr_len == result_len);

    const bool has_seed = seed != NULL;
    const uint64_t *seed_ptr = NULL;
    if (has_seed) {
      ARROW_CHECK(arr_len == seed->length);
      ARROW_CHECK(seed->type->id() == arrow::Type::UINT64);
      seed_ptr = seed->GetValues<uint64_t>(1);
    }

    for (int64_t idx = 0; idx < arr_len; ++idx) {
      const int64_t curr_offset = arr_index_ptr[idx];
      int64_t curr_size = arr_index_ptr[idx + 1] - curr_offset;

      if (has_nulls && !bit_util::GetBit(bitmask_ptr, idx + bit_offset)) {
        curr_size = 0;
      }
      uint64_t seed_val = 0;
      if (has_seed) {
        seed_val = seed_ptr[idx];
      }

      result_ptr[idx] = XXH3_64bits_withSeed(arr_data_ptr + curr_offset, curr_size, seed_val);
    }
  }
};

void hash_primitive_array(const arrow::ArrayData *arr, const arrow::ArrayData *seed, arrow::ArrayData *result) {
  switch (arrow::bit_width(arr->type->id()) >> 3) {
    case 1:
      return HashingPrimitiveArray<1>::Exec(arr, seed, result);
    case 2:
      return HashingPrimitiveArray<2>::Exec(arr, seed, result);
    case 4:
      return HashingPrimitiveArray<4>::Exec(arr, seed, result);
    case 8:
      return HashingPrimitiveArray<8>::Exec(arr, seed, result);
    case 16:
      return HashingPrimitiveArray<16>::Exec(arr, seed, result);
    case 32:
      return HashingPrimitiveArray<32>::Exec(arr, seed, result);
    default:
      ARROW_LOG(FATAL) << "Unknown bitwidth for arrow type" << arr->type->id();
  }
}

void hash_single_array(const arrow::ArrayData *arr, const arrow::ArrayData *seed, arrow::ArrayData *result) {
  ARROW_CHECK(arr != NULL);
  ARROW_CHECK(result != NULL);
  if (arrow::is_primitive(arr->type->id())) {
    hash_primitive_array(arr, seed, result);
  } else if (arrow::is_binary_like(arr->type->id())) {
    HashingBinaryArray<arrow::BinaryType::offset_type>::Exec(arr, seed, result);
  } else if (arrow::is_large_binary_like(arr->type->id())) {
    HashingBinaryArray<arrow::LargeBinaryType::offset_type>::Exec(arr, seed, result);
  } else {
    ARROW_LOG(FATAL) << "Unsupported Type " << arr->type->id();
  }
}

}  // namespace

namespace daft {
namespace kernels {

std::shared_ptr<arrow::ChunkedArray> xxhash_chunked_array(const arrow::ChunkedArray *arr, const arrow::ChunkedArray *seed) {
  ARROW_CHECK_NE(arr, NULL);
  const int64_t num_chunks = arr->num_chunks();

  const int64_t size = arr->length();
  std::vector<std::shared_ptr<arrow::Buffer>> result_buffers{2};
  result_buffers[1] = arrow::AllocateBuffer(sizeof(arrow::UInt64Type::c_type) * size).ValueOrDie();
  std::shared_ptr<arrow::ArrayData> result = arrow::ArrayData::Make(std::make_shared<arrow::UInt64Type>(), size, result_buffers, 0);

  if (seed != NULL) {
    ARROW_CHECK(seed->num_chunks() == 1);
    ARROW_CHECK(seed->null_count() == 0);
    ARROW_CHECK(seed->length() == size);
  }

  std::vector<std::shared_ptr<arrow::Array>> result_arrays;
  int64_t offset_so_far = 0;
  for (int64_t i = 0; i < num_chunks; ++i) {
    std::shared_ptr<arrow::ArrayData> arr_subslice = arr->chunk(i)->data();
    int64_t arr_len = arr_subslice->length;
    std::shared_ptr<arrow::ArrayData> result_subslice = result->Slice(offset_so_far, arr_len);
    arrow::ArrayData *seed_subslice = seed != NULL ? seed->chunk(0)->Slice(offset_so_far, arr_len)->data().get() : NULL;

    hash_single_array(arr_subslice.get(), seed_subslice, result_subslice.get());
    offset_so_far += arr_len;
  }
  return arrow::ChunkedArray::Make({arrow::MakeArray(result)}, std::make_shared<arrow::UInt64Type>()).ValueOrDie();
}

}  // namespace kernels
}  // namespace daft
