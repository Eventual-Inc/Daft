from __future__ import annotations

import csv
import io
import pathlib

import pytest

import daft
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions, TableReadOptions
from daft.table import Table, schema_inference, table_io


def test_read_input(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    data = {"foo": [1, 2, 3]}
    with open(tmpdir / "file.csv", "w") as f:
        f.write("foo\n")
        f.write("1\n")
        f.write("2\n")
        f.write("3\n")

    schema = Schema._from_field_name_and_types([("foo", DataType.int64())])

    # Test pathlib, str and IO
    assert table_io.read_csv(tmpdir / "file.csv", schema=schema).to_pydict() == data
    assert table_io.read_csv(str(tmpdir / "file.csv"), schema=schema).to_pydict() == data

    with open(tmpdir / "file.csv", "rb") as f:
        assert table_io.read_csv(f, schema=schema).to_pydict() == data


def _csv_write_helper(header: list[str] | None, data: list[list[str | None]], delimiter: str = ","):
    f = io.StringIO()
    writer = csv.writer(f, delimiter=delimiter)
    if header is not None:
        writer.writerow(header)
    writer.writerows(data)
    f.seek(0)
    return io.BytesIO(f.getvalue().encode("utf-8"))


@pytest.mark.parametrize(
    ["data", "expected_dtype"],
    [
        ("1", DataType.int64()),
        ("foo", DataType.string()),
        ("1.5", DataType.float64()),
        ("True", DataType.bool()),
    ],
)
def test_csv_infer_schema(data, expected_dtype):
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    )

    schema = schema_inference.from_csv(f)
    assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


def test_csv_infer_schema_custom_delimiter():
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
        delimiter="|",
    )

    schema = schema_inference.from_csv(f, csv_options=TableParseCSVOptions(delimiter="|"))
    assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])


def test_csv_infer_schema_no_header():
    f = _csv_write_helper(
        header=None,
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    )

    schema = schema_inference.from_csv(f, csv_options=TableParseCSVOptions(header_index=None))
    assert schema == Schema._from_field_name_and_types([("f0", DataType.int64()), ("f1", DataType.int64())])


@pytest.mark.parametrize(
    ["data", "expected_data_series"],
    [
        ["1", daft.Series.from_pylist([1, 1, None])],
        # NOTE: Empty gets parsed as "" instead of None for string fields
        ["foo", daft.Series.from_pylist(["foo", "foo", ""])],
        ["1.5", daft.Series.from_pylist([1.5, 1.5, None])],
        ("True", daft.Series.from_pylist([True, True, None])),
    ],
)
def test_csv_read_data(data, expected_data_series):
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_data_series.datatype())])
    expected = Table.from_pydict(
        {
            "id": [1, 2, 3],
            "data": expected_data_series,
        }
    )
    table = table_io.read_csv(f, schema)
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_limit_rows():
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
    expected = Table.from_pydict(
        {
            "id": [1, 2],
            "data": [1, 2],
        }
    )
    table = table_io.read_csv(
        f,
        schema,
        read_options=TableReadOptions(num_rows=2),
    )
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_select_columns():
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
    expected = Table.from_pydict(
        {
            "data": [1, 2, None],
        }
    )
    table = table_io.read_csv(
        f,
        schema,
        read_options=TableReadOptions(column_names=["data"]),
    )
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_custom_delimiter():
    f = _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
        delimiter="|",
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
    expected = Table.from_pydict(
        {
            "id": [1, 2, 3],
            "data": [1, 2, None],
        }
    )
    table = table_io.read_csv(
        f,
        schema,
        csv_options=TableParseCSVOptions(delimiter="|"),
    )
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_no_header():
    f = _csv_write_helper(
        header=None,
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    )

    schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
    expected = Table.from_pydict(
        {
            "id": [1, 2, 3],
            "data": [1, 2, None],
        }
    )
    table = table_io.read_csv(
        f,
        schema,
        csv_options=TableParseCSVOptions(header_index=None),
    )
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
