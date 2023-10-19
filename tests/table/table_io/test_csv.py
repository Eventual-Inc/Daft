from __future__ import annotations

import contextlib
import csv
import os
import pathlib
import tempfile

import pytest

import daft
from daft.daft import NativeStorageConfig, PythonStorageConfig, StorageConfig
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions, TableReadOptions
from daft.table import Table, schema_inference, table_io


def storage_config_from_use_native_downloader(use_native_downloader: bool) -> StorageConfig:
    if use_native_downloader:
        return StorageConfig.native(NativeStorageConfig(None))
    else:
        return StorageConfig.python(PythonStorageConfig(None))


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


@contextlib.contextmanager
def _csv_write_helper(header: list[str] | None, data: list[list[str | None]], delimiter: str = ","):
    with tempfile.TemporaryDirectory() as directory_name:
        file = os.path.join(directory_name, "tempfile")
        with open(file, "w", newline="") as f:
            writer = csv.writer(f, delimiter=delimiter)
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
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_infer_schema(data, expected_dtype, use_native_downloader):
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        schema = schema_inference.from_csv(f, storage_config=storage_config)
        assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_infer_schema_custom_delimiter(use_native_downloader):
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
        delimiter="|",
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        schema = schema_inference.from_csv(
            f, storage_config=storage_config, csv_options=TableParseCSVOptions(delimiter="|")
        )
        assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_infer_schema_no_header(use_native_downloader):
    with _csv_write_helper(
        header=None,
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        schema = schema_inference.from_csv(
            f, storage_config=storage_config, csv_options=TableParseCSVOptions(header_index=None)
        )
        fields = (
            [("column_1", DataType.int64()), ("column_2", DataType.int64())]
            if use_native_downloader
            else [("f0", DataType.int64()), ("f1", DataType.int64())]
        )
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
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_read_data(data, expected_data_series, use_native_downloader):
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", data],
            ["2", data],
            ["3", None],
        ],
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        schema = Schema._from_field_name_and_types(
            [("id", DataType.int64()), ("data", expected_data_series.datatype())]
        )
        expected = Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": expected_data_series,
            }
        )
        table = table_io.read_csv(f, schema, storage_config=storage_config)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_read_data_csv_limit_rows(use_native_downloader):
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)

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
            storage_config=storage_config,
            read_options=TableReadOptions(num_rows=2),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_read_data_csv_select_columns(use_native_downloader):
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)

        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = Table.from_pydict(
            {
                "data": [1, 2, None],
            }
        )
        table = table_io.read_csv(
            f,
            schema,
            storage_config=storage_config,
            read_options=TableReadOptions(column_names=["data"]),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_read_data_csv_custom_delimiter(use_native_downloader):
    with _csv_write_helper(
        header=["id", "data"],
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
        delimiter="|",
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)

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
            storage_config=storage_config,
            csv_options=TableParseCSVOptions(delimiter="|"),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_csv_read_data_csv_no_header(use_native_downloader):
    with _csv_write_helper(
        header=None,
        data=[
            ["1", "1"],
            ["2", "2"],
            ["3", None],
        ],
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)

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
            storage_config=storage_config,
            csv_options=TableParseCSVOptions(header_index=None),
            read_options=TableReadOptions(column_names=["id", "data"]),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
