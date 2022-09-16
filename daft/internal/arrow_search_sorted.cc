#include <functional>  // for std::less and std::less_equal
#include <cassert>
#include <arrow/type.h>


template <class ArrowType>
static void
primative_binsearch_nonnull(const uint8_t *arr, const uint8_t *key, uint8_t *ret, size_t arr_len,
          size_t key_len, size_t arr_str, size_t key_str, size_t ret_str) {
    /*
     * Adapted from numpy binary search: https://github.com/numpy/numpy/blob/main/numpy/core/src/npysort/binsearch.cpp
     */
    using T = typename ArrowType::c_type;
    assert(sizeof(T) == arr_str);
    assert(sizeof(T) == key_str);
    auto cmp = std::less<T>{};
    size_t min_idx = 0;
    size_t max_idx = arr_len;
    T last_key_val;

    if (key_len == 0) {
        return;
    }
    last_key_val = *(const T *)key;

    for (; key_len > 0; key_len--, key += key_str, ret += ret_str) {
        const T key_val = *(const T *)key;
        /*
         * Updating only one of the indices based on the previous key
         * gives the search a big boost when keys are sorted, but slightly
         * slows down things for purely random ones.
         */
        if (cmp(last_key_val, key_val)) {
            max_idx = arr_len;
        }
        else {
            min_idx = 0;
            max_idx = (max_idx < arr_len) ? (max_idx + 1) : arr_len;
        }

        last_key_val = key_val;

        while (min_idx < max_idx) {
            const size_t mid_idx = min_idx + ((max_idx - min_idx) >> 1);
            const T mid_val = *(const T *)(arr + mid_idx * arr_str);
            if (cmp(mid_val, key_val)) {
                min_idx = mid_idx + 1;
            }
            else {
                max_idx = mid_idx;
            }
        }
        *(size_t *)ret = min_idx;
    }
}

void test_main() {
    primative_binsearch_nonnull<arrow::UInt8Type>(NULL, NULL, NULL, 0,0,0,0,0);
}
