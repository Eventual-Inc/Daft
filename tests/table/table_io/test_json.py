from __future__ import annotations

import io
import json
from typing import Any

import pytest

import daft
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.table import Table, schema_inference, table_io


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
        ([1, None, 2], DataType.list("item", DataType.int64())),
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
