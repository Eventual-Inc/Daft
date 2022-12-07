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
        if data.num_chunks != 1:
            data = data.combine_chunks()
        else:
            data = data.chunk(0)
        result_chunks = [
            daft_core.search_sorted_pyarrow_array(data, key_chunk, input_reversed, pa) for key_chunk in keys.chunks
        ]
        return pa.chunked_array(result_chunks, type=pa.uint64())
    elif isinstance(data, pa.Table):
        assert isinstance(keys, pa.Table), "expected keys to be a table since data is one"
        assert data.schema == keys.schema
        num_columns = data.num_columns

        if input_reversed is not None:
            if isinstance(input_reversed, bool):
                table_input_reversed = [input_reversed for _ in range(num_columns)]
            elif isinstance(input_reversed, list):
                assert all(isinstance(b, bool) for b in input_reversed), "found wrong type in input_reversed " + str(
                    input_reversed
                )
                assert len(input_reversed) == num_columns
                table_input_reversed = input_reversed
            else:
                raise ValueError("expected `input_reversed` to be either `bool` or list[bool] got: " + input_reversed)
        else:
            table_input_reversed = [False for _ in range(num_columns)]

        if num_columns == 1:
            return search_sorted(data.columns[0], keys.columns[0])

        data = data.combine_chunks()
        keys = keys.combine_chunks()

        result = daft_core.daft_core.search_sorted_multiple_pyarrow_array(
            [c.chunk(0) for c in data.columns], [c.chunk(0) for c in keys.columns], table_input_reversed, pa
        )
        return pa.chunked_array([result], type=pa.uint64())
    else:
        raise NotImplementedError("Not Implemented")
