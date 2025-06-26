from __future__ import annotations

import contextlib
import json
import os
import pathlib
import tempfile
from typing import Any

import pytest

import daft
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.recordbatch import MicroPartition, recordbatch_io
from daft.runners.partitioning import TableReadOptions


def test_read_input(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    data = {"foo": [1, 2, 3]}
    with open(tmpdir / "file.json", "w") as f:
        for i in range(1, 4):
            json.dump({"foo": i}, f)
            f.write("\n")

    schema = Schema._from_field_name_and_types([("foo", DataType.int64())])

    # Test pathlib, str and IO
    assert recordbatch_io.read_json(tmpdir / "file.json", schema=schema).to_pydict() == data
    assert recordbatch_io.read_json(str(tmpdir / "file.json"), schema=schema).to_pydict() == data


@contextlib.contextmanager
def _json_write_helper(data: dict[str, list[Any]]):
    with tempfile.TemporaryDirectory() as directory_name:
        first_key = next(iter(data.keys()))
        data_len = len(data[first_key])
        for k in data:
            assert len(data[k]) == data_len
        file = os.path.join(directory_name, "tempfile")
        with open(file, "w", newline="") as f:
            for i in range(data_len):
                f.write(json.dumps({k: data[k][i] for k in data}) + "\n")
        yield file


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
    with _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [data, data, None],
        }
    ) as f:
        schema = Schema.from_json(f)
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
    with _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [data, data, None],
        }
    ) as f:
        schema = Schema._from_field_name_and_types(
            [("id", DataType.int64()), ("data", expected_data_series.datatype())]
        )
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2, 3],
                "data": expected_data_series,
            }
        )
        table = recordbatch_io.read_json(f, schema)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_json_read_data_limit_rows():
    with _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [1, 2, None],
        }
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2],
                "data": [1, 2],
            }
        )
        table = recordbatch_io.read_json(f, schema, read_options=TableReadOptions(num_rows=2))
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_json_read_data_select_columns():
    with _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [1, 2, None],
        }
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "data": [1, 2, None],
            }
        )
        table = recordbatch_io.read_json(f, schema, read_options=TableReadOptions(column_names=["data"]))
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
