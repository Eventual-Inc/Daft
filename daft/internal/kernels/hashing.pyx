# distutils: language=c++
# distutils: sources = daft/internal/kernels/hashing.cc daft/internal/kernels/xxhash.cc

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


cdef extern from "hashing.h" namespace "daft::kernels" nogil:
    cdef shared_ptr[CChunkedArray] xxhash_chunked_array(const CChunkedArray* arr);

def hash_chunked_array(data):
    cdef shared_ptr[CChunkedArray] result

    cdef shared_ptr[CChunkedArray] carr
    cdef shared_ptr[CTable] ctab

    if isinstance(data, pa.ChunkedArray):
        carr = pyarrow_unwrap_chunked_array(data)
        result = xxhash_chunked_array(carr.get())
    else:
        raise NotImplementedError()
    return pyarrow_wrap_chunked_array(result)
