from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft import DataType, col
from daft.logical.schema import Schema
from daft.table import Table


@pytest.mark.parametrize(
    "mp",
    [
        Table.from_pydict({"a": pa.array([], type=pa.int64())}),  # 1 empty table
        Table.empty(Schema.from_pyarrow_schema(pa.schema({"a": pa.int64()}))),  # No tables
    ],
)
def test_micropartitions_size_bytes_empty(mp) -> None:
    assert mp.size_bytes() == 0


@pytest.mark.parametrize(
    "mp",
    [
        Table.from_pydict({"a": [1, 3, 2, 4]}),  # 1 table
        Table.concat(
            [
                Table.from_pydict({"a": np.array([]).astype(np.int64)}),
                Table.from_pydict({"a": [1]}),
                Table.from_pydict({"a": [3, 2, 4]}),
            ]
        ),  # 3 tables
    ],
)
def test_micropartitions_size_bytes(mp) -> None:
    assert mp.size_bytes() == (4 * 8)


def test_table_size_bytes() -> None:

    data = Table.from_pydict({"a": [1, 2, 3, 4, None], "b": [False, True, False, True, None]}).eval_expression_list(
        [col("a").cast(DataType.int64()), col("b")]
    )
    assert data.size_bytes() == (5 * 8 + 1) + (1 + 1)


@pytest.mark.parametrize("length", [10, 200])
def test_table_size_bytes_pyobj(length) -> None:
    import pickle

    size_per_obj = len(pickle.dumps(object()))
    data = Table.from_pydict({"a": [object()] * length})
    assert data.size_bytes() == size_per_obj * length
