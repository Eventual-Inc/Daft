from __future__ import annotations

import contextlib
import json
import os
import pathlib
import tempfile
from typing import Any

import pytest

import daft
from daft.daft import NativeStorageConfig, PythonStorageConfig, StorageConfig
from daft.datatype import DataType
from daft.logical.schema import Schema
from daft.runners.partitioning import TableReadOptions
from daft.table import MicroPartition, schema_inference, table_io


def storage_config_from_use_native_downloader(use_native_downloader: bool) -> StorageConfig:
    if use_native_downloader:
        return StorageConfig.native(NativeStorageConfig(True, None))
    else:
        return StorageConfig.python(PythonStorageConfig(None))


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_read_input(tmpdir, use_native_downloader):
    tmpdir = pathlib.Path(tmpdir)
    data = {"foo": [1, 2, 3]}
    with open(tmpdir / "file.json", "w") as f:
        for i in range(1, 4):
            json.dump({"foo": i}, f)
            f.write("\n")

    schema = Schema._from_field_name_and_types([("foo", DataType.int64())])
    storage_config = storage_config_from_use_native_downloader(use_native_downloader)

    # Test pathlib, str and IO
    assert table_io.read_json(tmpdir / "file.json", schema=schema, storage_config=storage_config).to_pydict() == data
    assert (
        table_io.read_json(str(tmpdir / "file.json"), schema=schema, storage_config=storage_config).to_pydict() == data
    )

    with open(tmpdir / "file.json", "rb") as f:
        if use_native_downloader:
            f = tmpdir / "file.json"
        assert table_io.read_json(f, schema=schema, storage_config=storage_config).to_pydict() == data


@contextlib.contextmanager
def _json_write_helper(data: dict[str, list[Any]]):
    with tempfile.TemporaryDirectory() as directory_name:
        first_key = list(data.keys())[0]
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
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_json_infer_schema(data, expected_dtype, use_native_downloader):
    with _json_write_helper(
        {
            "id": [1, 2, 3],
            "data": [data, data, None],
        }
    ) as f:
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        schema = schema_inference.from_json(f, storage_config=storage_config)
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
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_json_read_data(data, expected_data_series, use_native_downloader):
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
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_json(f, schema, storage_config=storage_config)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_json_read_data_limit_rows(use_native_downloader):
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
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_json(f, schema, read_options=TableReadOptions(num_rows=2), storage_config=storage_config)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_json_read_data_select_columns(use_native_downloader):
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
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_json(
            f, schema, read_options=TableReadOptions(column_names=["data"]), storage_config=storage_config
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
