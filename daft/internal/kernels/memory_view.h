#pragma once
#include <arrow/array/data.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

namespace daft {
namespace kernels {
struct MemoryViewBase {
  MemoryViewBase(const std::shared_ptr<arrow::ArrayData> &data) : data_(data){};
  virtual ~MemoryViewBase() = default;

  virtual int Compare(const MemoryViewBase *other, const uint64_t left_idx, const uint64_t right_idx) const = 0;
  const std::shared_ptr<arrow::ArrayData> data_;
};

template <typename ArrowType>
struct PrimativeMemoryView : public MemoryViewBase {
  PrimativeMemoryView(const std::shared_ptr<arrow::ArrayData> &data) : MemoryViewBase(data){};
  ~PrimativeMemoryView() = default;
  int Compare(const MemoryViewBase *other, const uint64_t left_idx, const uint64_t right_idx) const {
    using T = typename ArrowType::c_type;
    const T left_val = *(this->data_->template GetValues<T>(1) + left_idx);
    const T right_val = *(other->data_->template GetValues<T>(1) + right_idx);
    const int is_less = std::less<T>{}(left_val, right_val);
    const int is_greater = std::less<T>{}(right_val, left_val);
    return is_greater - is_less;
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
  void AddPrimativeMemoryView(const std::shared_ptr<arrow::ArrayData> &data) {
    static_assert(std::is_base_of<arrow::DataType, ArrowType>::value, "T is not derived from MemoryViewBase");
    AddMemoryView<PrimativeMemoryView<ArrowType>>(data);
  }

  int Compare(const CompositeView &other, const uint64_t left_idx, const uint64_t right_idx) const {
    int result = 0;
    const size_t size = views_.size();
    for (size_t i = 0; i < size; ++i) {
      result = views_[i]->Compare(other.views_[i].get(), left_idx, right_idx);
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
