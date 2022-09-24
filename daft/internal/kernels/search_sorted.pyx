# distutils: language=c++
# distutils: sources = daft/internal/kernels/search_sorted.cc
import pyarrow as pa

from libcpp cimport bool as cbool
from libcpp.vector cimport vector
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
    cdef shared_ptr[CChunkedArray] search_sorted_chunked_array(const CChunkedArray* arr, const CChunkedArray* keys, const cbool desc);
    cdef shared_ptr[CChunkedArray] search_sorted_table(const CTable* arr, const CTable* keys, const vector[cbool]& desc);


def search_sorted(data, keys, desc=None):

    cdef shared_ptr[CChunkedArray] result

    cdef shared_ptr[CChunkedArray] carr
    cdef shared_ptr[CChunkedArray] key_arr
    cdef shared_ptr[CTable] ctab
    cdef shared_ptr[CTable] key_table

    if isinstance(data, pa.ChunkedArray):
        assert isinstance(keys, pa.ChunkedArray), 'expected keys to be a chunked_array since data is one'
        if desc is not None:
            assert isinstance(desc, bool), 'expect desc to be a bool got : ' + type(desc)
        else:
            desc = False
        carr = pyarrow_unwrap_chunked_array(data)
        if carr.get() == NULL:
            raise TypeError("not a chunked array")
        key_arr = pyarrow_unwrap_chunked_array(keys)
        if key_arr.get() == NULL:
            raise TypeError("not a chunked array")
        result = search_sorted_chunked_array(carr.get(), key_arr.get(), desc)

    elif isinstance(data, pa.Table):
        assert isinstance(keys, pa.Table), 'expected keys to be a table since data is one'
        table_desc = []
        num_columns = data.num_columns
        assert keys.num_columns == num_columns, 'mismatch of columns'
        if desc is not None:
            if isinstance(desc, bool):
                table_desc = [desc for _ in range(num_columns)]
            elif isinstance(desc, list):
                assert all(isinstance(b, bool) for b in desc)
                assert len(desc) == num_columns
                table_desc = desc
            else:
                raise ValueError("expected `desc` to be either `bool` or list[bool] got: " + desc)
        else:
            table_desc = [False for _ in range(num_columns)]

        ctab = pyarrow_unwrap_table(data)
        if ctab.get() == NULL:
            raise TypeError("not a table")
        key_table = pyarrow_unwrap_table(keys)
        if key_table.get() == NULL:
            raise TypeError("not a chunked array")
        result = search_sorted_table(ctab.get(), key_table.get(), table_desc)
    else:
        raise ValueError("Unknown type for kernel: " + str(type(data)))
    return pyarrow_wrap_chunked_array(result)
