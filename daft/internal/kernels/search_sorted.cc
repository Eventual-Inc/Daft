#include <arrow/array.h>

#include "daft/internal/kernels/memory_view.h"

#include <iostream>

int search_sorted(const arrow::Array* arr) {
    std::cout << arr->length() << std::endl;
    return 0;
}
