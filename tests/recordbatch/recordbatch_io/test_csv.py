from __future__ import annotations

import contextlib
import csv
import os
import pathlib
import tempfile

import pytest

import daft
from daft.daft import CsvParseOptions
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.recordbatch import MicroPartition, recordbatch_io
from daft.runners.partitioning import TableParseCSVOptions, TableReadOptions
from daft.utils import get_arrow_version


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
    assert recordbatch_io.read_csv(tmpdir / "file.csv", schema=schema).to_pydict() == data
    assert recordbatch_io.read_csv(str(tmpdir / "file.csv"), schema=schema).to_pydict() == data


@contextlib.contextmanager
def _csv_write_helper(header: list[str] | None, data: list[list[str | None]], **kwargs):
    with tempfile.TemporaryDirectory() as directory_name:
        file = os.path.join(directory_name, "tempfile")
        with open(file, "w", newline="") as f:
            writer = csv.writer(f, **kwargs)
            if header is not None:
                writer.writerow(header)
            writer.writerows(data)
        yield file


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
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    ) as f:
        schema = Schema.from_csv(f)
        assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


def test_csv_infer_schema_custom_delimiter():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
        delimiter="|",
    ) as f:
        schema = Schema.from_csv(f, parse_options=CsvParseOptions(delimiter="|"))
        assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])


def test_csv_infer_schema_no_header():
    with _csv_write_helper(
        header=None,
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        schema = Schema.from_csv(f, parse_options=CsvParseOptions(has_header=False))
        fields = [("column_1", DataType.int64()), ("column_2", DataType.int64())]
        assert schema == Schema._from_field_name_and_types(fields)


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
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
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
        table = recordbatch_io.read_csv(f, schema)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_limit_rows():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2],
                "data": [1, 2],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            read_options=TableReadOptions(num_rows=2),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_select_columns():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "data": [1, 2, None],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            read_options=TableReadOptions(column_names=["data"]),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_custom_delimiter():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
        delimiter="|",
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [1, 2, None],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(delimiter="|"),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_no_header():
    with _csv_write_helper(
        header=None,
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [1, 2, None],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(header_index=None),
            read_options=TableReadOptions(column_names=["id", "data"]),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_csv_custom_quote():
    with _csv_write_helper(
        header=["'id'", "'data'"],
        data=[
            ["1", "'aa'"],
            ["2", "aa"],
            ["3", "aa"],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.string())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2, 3],
                "data": ["aa", "aa", "aa"],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(quote="'"),
        )

        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_custom_escape():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", 'a"a"a'],
            ["2", "aa"],
            ["3", "aa"],
        ],
        quotechar='"',
        escapechar="\\",
        doublequote=False,
        quoting=csv.QUOTE_ALL,
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.string())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2, 3],
                "data": ['a"a"a', "aa", "aa"],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(escape_char="\\", double_quote=False),
        )

        assert table.to_arrow() == expected.to_arrow(), f"Received:\n{table}\n\nExpected:\n{expected}"


def test_csv_read_data_custom_comment():
    with tempfile.TemporaryDirectory() as directory_name:
        file = os.path.join(directory_name, "tempfile")
        with open(file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "data"])
            writer.writerow(["1", "aa"])
            f.write("# comment line\n")
            writer.writerow(["3", "aa"])

        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.string())])
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 3],
                "data": ["aa", "aa"],
            }
        )
        # Skipping test for arrow < 7.0.0 as comments are not supported in pyarrow
        arrow_version = get_arrow_version()
        if arrow_version >= (7, 0, 0):
            table = recordbatch_io.read_csv(
                file,
                schema,
                csv_options=TableParseCSVOptions(comment="#"),
            )
            assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_variable_missing_columns():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1"],
            ["2", "2"],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types(
            [
                ("id", DataType.int64()),
                ("data", DataType.int64()),
            ]
        )
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2],
                "data": [None, 2],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(allow_variable_columns=True),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_variable_extra_columns():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2", "2"],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types(
            [
                ("id", DataType.int64()),
                ("data", DataType.int64()),
            ]
        )
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2],
                "data": [1, 2],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(allow_variable_columns=True),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_csv_read_data_variable_columns_with_non_matching_types():
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["a", "b"],
            ["2", "2"],
        ],
    ) as f:
        schema = Schema._from_field_name_and_types(
            [
                ("id", DataType.int64()),
                ("data", DataType.int64()),
            ]
        )
        expected = MicroPartition.from_pydict(
            {
                "id": [None, 2],
                "data": [None, 2],
            }
        )
        table = recordbatch_io.read_csv(
            f,
            schema,
            csv_options=TableParseCSVOptions(allow_variable_columns=True),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
