# distutils: language=c++
# distutils: sources = daft/internal/kernels/search_sorted.cc
import pyarrow as pa

from pyarrow.lib cimport (
    CChunkedArray,
    CTable,
    pyarrow_unwrap_chunked_array,
    pyarrow_unwrap_table,
    pyarrow_wrap_chunked_array,
    pyarrow_wrap_table,
    shared_ptr,
)

import cython


cdef extern from "search_sorted.h" namespace "daft::kernels" nogil :
    cdef shared_ptr[CChunkedArray] search_sorted_chunked_array(const CChunkedArray* arr, const CChunkedArray* keys);
    cdef shared_ptr[CChunkedArray] search_sorted_table(const CTable* arr, const CTable* keys);




def search_sorted(data, keys):

    cdef shared_ptr[CChunkedArray] result

    cdef shared_ptr[CChunkedArray] carr
    cdef shared_ptr[CChunkedArray] key_arr
    cdef shared_ptr[CTable] ctab
    cdef shared_ptr[CTable] key_table
    if isinstance(data, pa.ChunkedArray):
        assert isinstance(keys, pa.ChunkedArray), 'expected keys to be a chunked_array since data is one'
        carr = pyarrow_unwrap_chunked_array(data)
        if carr.get() == NULL:
            raise TypeError("not a chunked array")
        key_arr = pyarrow_unwrap_chunked_array(keys)
        if key_arr.get() == NULL:
            raise TypeError("not a chunked array")
        result = search_sorted_chunked_array(carr.get(), key_arr.get())

    elif isinstance(data, pa.Table):
        assert isinstance(keys, pa.Table), 'expected keys to be a table since data is one'
        ctab = pyarrow_unwrap_table(data)
        if ctab.get() == NULL:
            raise TypeError("not a table")
        key_table = pyarrow_unwrap_table(keys)
        if key_table.get() == NULL:
            raise TypeError("not a chunked array")
        result = search_sorted_table(ctab.get(), key_table.get())
    else:
        raise ValueError("Unknown type for kernel: " + str(type(data)))
    return pyarrow_wrap_chunked_array(result)
