#pragma once
#include <arrow/array/data.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

namespace daft {
namespace kernels {
inline int strcmpNoTerminator(const uint8_t *str1, const uint8_t *str2, size_t str1len, size_t str2len) {
  // https://stackoverflow.com/questions/24770665/comparing-2-char-with-different-lengths-without-null-terminators
  // Get the length of the shorter string
  const size_t len = str1len < str2len ? str1len : str2len;
  // Compare the strings up until one ends
  const int cmp = memcmp(str1, str2, len);
  // If they weren't equal, we've got our result
  // If they are equal and the same length, they matched
  if (cmp != 0 || str1len == str2len) {
    return cmp;
  }
  // If they were equal but one continues on, the shorter string is
  // lexicographically smaller
  return str1len < str2len ? -1 : 1;
}

struct MemoryViewBase {
  MemoryViewBase(const std::shared_ptr<arrow::ArrayData> &data) : data_(data){};
  virtual ~MemoryViewBase() = default;

  inline bool isValid(const uint64_t idx) const {
#if ARROW_VERSION_MAJOR < 7
    namespace bit_util = arrow::BitUtil;
#else
    namespace bit_util = arrow::bit_util;
#endif
    const auto bit_ptr = data_->GetValues<uint8_t>(0, 0);
    if (bit_ptr == NULL) {
      return true;
    }
    return bit_util::GetBit(bit_ptr, idx + data_->offset);
  }

  virtual int Compare(const MemoryViewBase *other, const uint64_t left_idx, const uint64_t right_idx) const = 0;
  const std::shared_ptr<arrow::ArrayData> data_;
};

template <typename ArrowType>
struct PrimitiveMemoryView : public MemoryViewBase {
  PrimitiveMemoryView(const std::shared_ptr<arrow::ArrayData> &data) : MemoryViewBase(data){};
  ~PrimitiveMemoryView() = default;
  int Compare(const MemoryViewBase *other, const uint64_t left_idx, const uint64_t right_idx) const {
    using T = typename ArrowType::c_type;
    const int left_is_null = !this->isValid(left_idx);
    const int right_is_null = !other->isValid(right_idx);
    if (left_is_null || right_is_null) {
      return left_is_null - right_is_null;
    }

    const T left_val = *(this->data_->template GetValues<T>(1) + left_idx);
    const T right_val = *(other->data_->template GetValues<T>(1) + right_idx);
    const int is_less = std::less<T>{}(left_val, right_val);
    const int is_greater = std::less<T>{}(right_val, left_val);
    return is_greater - is_less;
  }
};

template <typename ArrowType>
struct BinaryMemoryView : public MemoryViewBase {
  BinaryMemoryView(const std::shared_ptr<arrow::ArrayData> &data) : MemoryViewBase(data){};
  ~BinaryMemoryView() = default;
  int Compare(const MemoryViewBase *other, const uint64_t left_idx, const uint64_t right_idx) const {
    using T = typename ArrowType::offset_type;

    const int left_is_null = !this->isValid(left_idx);
    const int right_is_null = !other->isValid(right_idx);
    if (left_is_null || right_is_null) {
      return left_is_null - right_is_null;
    }

    const T left_offset = *(this->data_->template GetValues<T>(1) + left_idx);
    const T left_size = *(this->data_->template GetValues<T>(1) + left_idx + 1) - left_offset;

    const T right_offset = *(other->data_->template GetValues<T>(1) + right_idx);
    const T right_size = *(other->data_->template GetValues<T>(1) + right_idx + 1) - right_offset;
    return strcmpNoTerminator(this->data_->template GetValues<uint8_t>(2, left_offset),
                              other->data_->template GetValues<uint8_t>(2, right_offset), left_size, right_size);
  }
};

struct CompositeView {
  CompositeView(){};

  template <typename T>
  void AddMemoryView(const std::shared_ptr<arrow::ArrayData> &data) {
    static_assert(std::is_base_of<MemoryViewBase, T>::value, "T is not derived from MemoryViewBase");
    views_.push_back(std::make_unique<T>(data));
  }

  template <typename ArrowType>
  void AddPrimitiveMemoryView(const std::shared_ptr<arrow::ArrayData> &data) {
    static_assert(std::is_base_of<arrow::DataType, ArrowType>::value, "T is not derived from MemoryViewBase");
    AddMemoryView<PrimitiveMemoryView<ArrowType>>(data);
  }

  template <typename ArrowType>
  void AddBinaryMemoryView(const std::shared_ptr<arrow::ArrayData> &data) {
    static_assert(std::is_base_of<arrow::DataType, ArrowType>::value, "T is not derived from MemoryViewBase");
    AddMemoryView<BinaryMemoryView<ArrowType>>(data);
  }

  inline bool isValid(const uint64_t idx) const {
    const size_t size = views_.size();
    for (size_t i = 0; i < size; ++i) {
      if (views_[i]->isValid(idx)) {
        return true;
      }
    }
    return false;
  }

  int Compare(const CompositeView &other, const uint64_t left_idx, const uint64_t right_idx, const std::vector<bool> &desc) const {
    int result = 0;
    const size_t size = views_.size();
    for (size_t i = 0; i < size; ++i) {
      const bool is_desc = desc[i];
      result = views_[i]->Compare(other.views_[i].get(), left_idx, right_idx);
      result = is_desc ? -result : result;
      if (result != 0) {
        break;
      }
    }
    return result;
  }

  std::vector<std::unique_ptr<MemoryViewBase>> views_;
};

}  // namespace kernels
}  // namespace daft
