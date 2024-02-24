from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft.logical.schema import Schema
from daft.table import MicroPartition
from tests.table.table_io.test_parquet import _parquet_write_helper


@pytest.mark.parametrize(
    ["data_with_intended_schema", "data", "expected"],
    [
        # Test that a cast occurs (in this case, int64 -> int8)
        (
            pa.Table.from_pydict({"foo": pa.array([], type=pa.int8())}),
            pa.Table.from_pydict({"foo": pa.array([1, 2, 3], type=pa.int64())}),
            MicroPartition.from_pydict({"foo": daft.Series.from_arrow(pa.array([1, 2, 3], type=pa.int8()))}),
        ),
        # Test what happens if a cast should occur, but fails at runtime (in this case, a potentially bad cast from utf8->int64)
        (
            pa.Table.from_pydict({"foo": pa.array([], type=pa.int64())}),
            pa.Table.from_pydict({"foo": pa.array(["1", "2", "FAIL"], type=pa.string())}),
            # NOTE: cast failures will become a Null value
            MicroPartition.from_pydict({"foo": daft.Series.from_arrow(pa.array([1, 2, None], type=pa.int64()))}),
        ),
        # Test reordering of columns
        (
            pa.Table.from_pydict({"bar": pa.array([], type=pa.int64()), "foo": pa.array([], type=pa.int64())}),
            pa.Table.from_pydict({"foo": pa.array([1, 2, 3]), "bar": pa.array([1, 2, 3])}),
            MicroPartition.from_pydict({"bar": pa.array([1, 2, 3]), "foo": pa.array([1, 2, 3])}),
        ),
        # Test automatic insertion of null values for missing column
        (
            pa.Table.from_pydict({"bar": pa.array([], type=pa.int64()), "foo": pa.array([], type=pa.int64())}),
            pa.Table.from_pydict({"foo": pa.array([1, 2, 3])}),
            MicroPartition.from_pydict(
                {"bar": pa.array([None, None, None], type=pa.int64()), "foo": pa.array([1, 2, 3])}
            ),
        ),
    ],
)
def test_parquet_cast_at_read_time(data_with_intended_schema, data, expected):
    schema = Schema.from_pyarrow_schema(data_with_intended_schema.schema)
    with _parquet_write_helper(data) as f, _parquet_write_helper(data_with_intended_schema) as f_with_schema:
        # Place `f_with_schema` first so that it gets used for schema inference
        df = daft.read_parquet([f_with_schema, f])
        assert df.schema() == schema
        assert df.to_arrow() == expected.to_arrow()
