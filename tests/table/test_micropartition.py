from __future__ import annotations

import copy

import pyarrow as pa
import pytest

from daft.logical.schema import Schema
from daft.table.micropartition import MicroPartition


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict({"a": pa.array([1, 2, 3], type=pa.int64())}),
        MicroPartition.from_pydict({"a": pa.array([], type=pa.int64())}),  # 1 empty table
        MicroPartition.empty(Schema.from_pyarrow_schema(pa.schema({"a": pa.int64()}))),  # No tables
    ],
)
def test_pickling_loaded(mp) -> None:
    assert mp.to_arrow() == copy.deepcopy(mp).to_arrow()


def test_pickling_unloaded() -> None:
    mp = MicroPartition.read_parquet("tests/assets/parquet-data/parquet-with-schema-metadata.parquet")
    assert copy.deepcopy(mp).to_arrow() == mp.to_arrow()
