from __future__ import annotations

import io
import json
import pathlib
from typing import Any

import pytest

import daft
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.runners.partitioning import TableReadOptions
from daft.table import Table, schema_inference, table_io


def test_read_input(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    data = {"foo": [1, 2, 3]}
    with open(tmpdir / "file.json", "w") as f:
        for i in range(1, 4):
            json.dump({"foo": i}, f)
            f.write("\n")

    schema = Schema._from_field_name_and_types([("foo", DataType.int64())])

    # Test pathlib, str and IO
    assert table_io.read_json(tmpdir / "file.json", schema=schema).to_pydict() == data
    assert table_io.read_json(str(tmpdir / "file.json"), schema=schema).to_pydict() == data

    with open(tmpdir / "file.json", "rb") as f:
        assert table_io.read_json(f, schema=schema).to_pydict() == data


def _json_write_helper(data: dict[str, list[Any]]):
    first_key = list(data.keys())[0]
    data_len = len(data[first_key])
    for k in data:
        assert len(data[k]) == data_len

    f = io.StringIO()
    for i in range(data_len):
        json.dump({k: data[k][i] for k in data}, f)
        f.write("\n")
    f.seek(0)
    return io.BytesIO(f.getvalue().encode("utf-8"))


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
        ([1, None, 2], DataType.list(DataType.int64())),
    ],
)
def test_json_infer_schema(data, expected_dtype):
    f = _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [data, data, None],
        }
    )

    schema = schema_inference.from_json(f)
    assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


@pytest.mark.parametrize(
    ["data", "expected_data_series"],
    [
        [1, daft.Series.from_pylist([1, 1, None])],
        ["foo", daft.Series.from_pylist(["foo", "foo", None])],
        [1.5, daft.Series.from_pylist([1.5, 1.5, None])],
        (True, daft.Series.from_pylist([True, True, None])),
        ([[], [1, 2], None], daft.Series.from_pylist([[[], [1, 2], None], [[], [1, 2], None], None])),
        ({"foo": 1}, daft.Series.from_pylist([{"foo": 1}, {"foo": 1}, None])),
    ],
)
def test_json_read_data(data, expected_data_series):
    f = _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [data, data, None],
        }
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_data_series.datatype())])
    expected = Table.from_pydict(
        {
            "id": [1, 2, 3],
            "data": expected_data_series,
        }
    )
    table = table_io.read_json(f, schema)
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_json_read_data_limit_rows():
    f = _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [1, 2, None],
        }
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
    expected = Table.from_pydict(
        {
            "id": [1, 2],
            "data": [1, 2],
        }
    )
    table = table_io.read_json(f, schema, read_options=TableReadOptions(num_rows=2))
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_json_read_data_select_columns():
    f = _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [1, 2, None],
        }
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
    expected = Table.from_pydict(
        {
            "data": [1, 2, None],
        }
    )
    table = table_io.read_json(f, schema, read_options=TableReadOptions(column_names=["data"]))
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
