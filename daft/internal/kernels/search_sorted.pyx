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


cdef extern from "search_sorted.h" namespace "daft::kernels" nogil:
    cdef shared_ptr[CChunkedArray] search_sorted_chunked_array(const CChunkedArray* arr, const CChunkedArray* keys, const cbool input_reversed);
    cdef shared_ptr[CChunkedArray] search_sorted_table(const CTable* arr, const CTable* keys, const vector[cbool]& input_reversed);


def search_sorted(data, keys, input_reversed=None):

    cdef shared_ptr[CChunkedArray] result

    cdef shared_ptr[CChunkedArray] carr
    cdef shared_ptr[CChunkedArray] key_arr
    cdef shared_ptr[CTable] ctab
    cdef shared_ptr[CTable] key_table

    if isinstance(data, pa.ChunkedArray):
        assert isinstance(keys, pa.ChunkedArray), 'expected keys to be a chunked_array since data is one'
        if input_reversed is not None:
            assert isinstance(input_reversed, bool), 'expect input_reversed to be a bool got : ' + type(input_reversed)
        else:
            input_reversed = False
        carr = pyarrow_unwrap_chunked_array(data)
        if carr.get() == NULL:
            raise TypeError("not a chunked array")
        key_arr = pyarrow_unwrap_chunked_array(keys)
        if key_arr.get() == NULL:
            raise TypeError("not a chunked array")
        result = search_sorted_chunked_array(carr.get(), key_arr.get(), input_reversed)

    elif isinstance(data, pa.Table):
        assert isinstance(keys, pa.Table), 'expected keys to be a table since data is one'
        table_input_reversed = []
        num_columns = data.num_columns
        assert keys.num_columns == num_columns, 'mismatch of columns'
        if input_reversed is not None:
            if isinstance(input_reversed, bool):
                table_input_reversed = [input_reversed for _ in range(num_columns)]
            elif isinstance(input_reversed, list):
                assert all(isinstance(b, bool) for b in input_reversed), 'found wrong type in input_reversed ' + str(input_reversed)
                assert len(input_reversed) == num_columns
                table_input_reversed = input_reversed
            else:
                raise ValueError("expected `input_reversed` to be either `bool` or list[bool] got: " + input_reversed)
        else:
            table_input_reversed = [False for _ in range(num_columns)]

        ctab = pyarrow_unwrap_table(data)
        if ctab.get() == NULL:
            raise TypeError("not a table")
        key_table = pyarrow_unwrap_table(keys)
        if key_table.get() == NULL:
            raise TypeError("not a chunked array")
        result = search_sorted_table(ctab.get(), key_table.get(), table_input_reversed)
    else:
        raise ValueError("Unknown type for kernel: " + str(type(data)))
    return pyarrow_wrap_chunked_array(result)
