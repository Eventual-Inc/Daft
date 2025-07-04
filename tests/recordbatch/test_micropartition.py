from __future__ import annotations

import copy

import pyarrow as pa
import pytest

from daft import col
from daft.logical.schema import Schema
from daft.recordbatch.micropartition import MicroPartition


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


@pytest.mark.parametrize(
    "mp",
    [
        MicroPartition.from_pydict(
            {"a": pa.array([1, 2, 3], type=pa.int64()), "b": pa.array([4.0, 5.0, 6.0], type=pa.float64())}
        ),
        MicroPartition.empty(Schema.from_pyarrow_schema(pa.schema({"a": pa.int64(), "b": pa.float64()}))),  # No tables
    ],
)
def test_pickling_duplicate_column_names(mp) -> None:
    # To force duplicate column names
    mp = mp.eval_expression_list([col("a"), col("b").alias("a")])

    assert mp.to_arrow() == copy.deepcopy(mp).to_arrow()
