from __future__ import annotations

import daft_core
import pyarrow as pa


def search_sorted(data, keys, input_reversed=None):
    if isinstance(data, pa.ChunkedArray):
        assert isinstance(keys, pa.ChunkedArray), "expected keys to be a chunked_array since data is one"
        if input_reversed is not None:
            assert isinstance(input_reversed, bool), "expect input_reversed to be a bool got : " + type(input_reversed)
        else:
            input_reversed = False
        assert data.num_chunks == 1

        result_chunks = [
            daft_core.search_sorted_pyarrow_array(data.chunk(0), key_chunk, pa) for key_chunk in keys.chunks
        ]
        return pa.chunked_array(result_chunks, type=pa.uint64())

    else:
        raise NotImplementedError("Not Implemented")
