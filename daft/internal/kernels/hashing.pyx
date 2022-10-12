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
    cdef shared_ptr[CChunkedArray] xxhash_chunked_array(const CChunkedArray* arr, const CChunkedArray* seed);

def hash_chunked_array(data, seed=None):
    cdef shared_ptr[CChunkedArray] result

    cdef shared_ptr[CChunkedArray] carr, cseed

    assert isinstance(data, pa.ChunkedArray)
    assert seed is None or isinstance(seed, pa.ChunkedArray)

    carr = pyarrow_unwrap_chunked_array(data)
    if seed is not None:
        cseed = pyarrow_unwrap_chunked_array(seed)
    result = xxhash_chunked_array(carr.get(), cseed.get())
    return pyarrow_wrap_chunked_array(result)
