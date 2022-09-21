# distutils: language=c++
# distutils: sources = daft/internal/kernels/search_sorted.cc

from libc cimport math, stdint
from libc.string cimport memset
from pyarrow.lib cimport (
    AllocateBuffer,
    CArray,
    CArrayData,
    CBuffer,
    CChunkedArray,
    CDataType,
    CResult,
    CScalarFunction,
    GetPrimitiveType,
    GetResultValue,
    MakeArray,
    Type,
    make_shared,
    pyarrow_unwrap_array,
    pyarrow_unwrap_chunked_array,
    pyarrow_wrap_chunked_array,
    shared_ptr,
    to_shared,
    unique_ptr,
    vector,
)

import cython


cdef extern from "search_sorted.h" nogil:
    cdef shared_ptr[CArray] search_sorted(const CArray* arr, const CArray* keys);


def search_sorted_chunked_array(data_arr, keys):
    cdef shared_ptr[CChunkedArray] carr = pyarrow_unwrap_chunked_array(data_arr)
    if carr.get() == NULL:
        raise TypeError("not a chunked array")
    num_chunks: cython.int = carr.get().num_chunks()
    assert num_chunks == 1
    cdef shared_ptr[CDataType] ctype = carr.get().type()
    cdef Type type_id = ctype.get().id()
    is_integer: cython.bool = False
    is_string: cython.bool = False

    if (Type._Type_UINT8 <= type_id) and (type_id <= Type._Type_INT64):
        is_integer = True
        # elif type_id == Type._Type_STRING:
        #     is_string = True
    else:
        raise TypeError('excepted integer or string got neither')

    cdef shared_ptr[CChunkedArray] key_arr = pyarrow_unwrap_chunked_array(keys)
    if key_arr.get() == NULL:
        raise TypeError("not a chunked array")
    assert key_arr.get().num_chunks() == 1
    cdef shared_ptr[CDataType] key_ctype = key_arr.get().type()
    cdef Type key_type_id = key_ctype.get().id()
    assert key_type_id == type_id

    cdef shared_ptr[CArray] arr

    cdef shared_ptr[CArray] key_single_arr

    cdef vector[shared_ptr[CArray]] search_results
    for i in range(num_chunks):
        arr = carr.get().chunk(i)
        key_single_arr = key_arr.get().chunk(i)
        result__ = search_sorted(arr.get(), key_single_arr.get())

    cdef shared_ptr[CChunkedArray] result = make_shared[CChunkedArray](search_results, GetPrimitiveType(Type._Type_UINT64))
    return pyarrow_wrap_chunked_array(result)
