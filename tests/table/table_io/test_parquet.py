from __future__ import annotations

import datetime
import io
import pathlib

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.table import Table, schema_inference, table_io


def test_read_input(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    data = pa.Table.from_pydict({"foo": [1, 2, 3]})
    with open(tmpdir / "file.parquet", "wb") as f:
        papq.write_table(data, f)

    schema = Schema._from_field_name_and_types([("foo", DataType.int64())])

    # Test pathlib, str and IO
    assert table_io.read_parquet(tmpdir / "file.parquet", schema=schema).to_arrow() == data
    assert table_io.read_parquet(str(tmpdir / "file.parquet"), schema=schema).to_arrow() == data

    with open(tmpdir / "file.parquet", "rb") as f:
        assert table_io.read_parquet(f, schema=schema).to_arrow() == data


def _parquet_write_helper(data: pa.Table):
    f = io.BytesIO()
    papq.write_table(data, f)
    f.seek(0)
    return f


@pytest.mark.parametrize(
    ["data", "expected_dtype"],
    [
        ("1", DataType.string()),
        ("foo", DataType.string()),
        ("1.5", DataType.string()),
        ("True", DataType.string()),
        ("None", DataType.string()),
        (1, DataType.int64()),
        (1.5, DataType.float64()),
        (True, DataType.bool()),
        (None, DataType.null()),
        ({"foo": 1}, DataType.struct({"foo": DataType.int64()})),
        ([1, None, 2], DataType.list("item", DataType.int64())),
    ],
)
def test_parquet_infer_schema(data, expected_dtype):
    f = _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [data, data, None],
            }
        )
    )

    schema = schema_inference.from_parquet(f)
    assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


@pytest.mark.parametrize(
    ["data", "expected_data_series"],
    [
        [1, daft.Series.from_pylist([1, 1, None])],
        ["foo", daft.Series.from_pylist(["foo", "foo", None])],
        [1.5, daft.Series.from_pylist([1.5, 1.5, None])],
        (True, daft.Series.from_pylist([True, True, None])),
        (
            datetime.date(year=2021, month=1, day=1),
            daft.Series.from_pylist(
                [datetime.date(year=2021, month=1, day=1), datetime.date(year=2021, month=1, day=1), None]
            ),
        ),
    ],
)
def test_parquet_read_data(data, expected_data_series):
    f = _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [data, data, None],
            }
        )
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_data_series.datatype())])
    expected = Table.from_pydict(
        {
            "id": [1, 2, 3],
            "data": expected_data_series,
        }
    )
    table = table_io.read_parquet(f, schema)
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
