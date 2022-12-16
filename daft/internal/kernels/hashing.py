from __future__ import annotations

import pyarrow as pa

from daft.daft import kernels


def hash_chunked_array(arr, seed=None):

    assert isinstance(arr, pa.ChunkedArray)
    result_chunks = []
    offset = 0
    if seed is not None:
        assert isinstance(seed, pa.ChunkedArray)
        assert len(seed) == len(arr)
        for chunk in arr.chunks:
            result_chunks.append(
                kernels.hash_pyarrow_array(chunk, pa, seed=seed.slice(offset, len(chunk)).combine_chunks())
            )
            offset += len(chunk)
    else:
        for chunk in arr.chunks:
            result_chunks.append(kernels.hash_pyarrow_array(chunk, pa, seed=None))
    return pa.chunked_array(result_chunks, type=pa.uint64())
