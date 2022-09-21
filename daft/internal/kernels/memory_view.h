#include <algorithm>
#include <memory>
#include <vector>

#include <cstdint>

namespace daft {

struct MemoryViewBase {
  MemoryViewBase(void *buffer, size_t length)
      : buffer_(buffer), length_(length){};
  virtual int Compare(const MemoryViewBase &other, const uint64_t left_idx,
                      const uint64_t right_idx) const;
  void *buffer_;
  size_t length_;
};

template <typename T> struct MemoryView : public MemoryViewBase {
  int Compare(const MemoryViewBase &other, const uint64_t left_idx,
              const uint64_t right_idx) {
    const T left_val = *(((T *)buffer_) + left_idx);
    const T right_val = *(((T *)buffer_) + right_idx);
    int is_less = std::less<T>{}(left_val, right_val);
    int is_greater = std::greater<T>{}(left_val, right_val);
    return is_greater - is_less;
  }

  inline const T &getValue(const uint64_t idx) const {
    return *(((T *)buffer_) + idx);
  }
};

struct CompositeView {
  CompositeView(){};
  template <typename T>

  void AddMemoryView(void *buffer, size_t length) {
    views_.push_back(std::make_unique<MemoryView<T>>());
  }

  int compare(const CompositeView &other, const uint64_t left_idx,
              const uint64_t right_idx) {
    int result = 0;
    const size_t size = views_.size();
    for (size_t i = 0; i < size; ++i) {
      result = views_[i]->Compare(*other.views_[i].get(), left_idx, right_idx);
      if (result != 0) {
        break;
      }
    }
    return result;
  }

  std::vector<std::unique_ptr<MemoryViewBase>> views_;
};

} // namespace daft
