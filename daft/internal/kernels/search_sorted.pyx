# distutils: language=c++
# distutils: sources = daft/internal/kernels/search_sorted.cc

from pyarrow.lib cimport (
    CChunkedArray,
    pyarrow_unwrap_chunked_array,
    pyarrow_wrap_chunked_array,
    shared_ptr,
)

import cython


cdef extern from "search_sorted.h" namespace "daft::kernels" nogil :
    cdef shared_ptr[CChunkedArray] search_sorted_chunked(const CChunkedArray* arr, const CChunkedArray* keys);

def search_sorted_chunked_array(data_arr, keys):
    cdef shared_ptr[CChunkedArray] carr = pyarrow_unwrap_chunked_array(data_arr)
    if carr.get() == NULL:
        raise TypeError("not a chunked array")
    cdef shared_ptr[CChunkedArray] key_arr = pyarrow_unwrap_chunked_array(keys)
    if key_arr.get() == NULL:
        raise TypeError("not a chunked array")
    cdef shared_ptr[CChunkedArray] result = search_sorted_chunked(carr.get(), key_arr.get())
    return pyarrow_wrap_chunked_array(result)
