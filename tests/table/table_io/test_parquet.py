from __future__ import annotations

import contextlib
import datetime
import os
import pathlib
import tempfile

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.daft import NativeStorageConfig, PythonStorageConfig, StorageConfig
from daft.datatype import DataType, TimeUnit
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseParquetOptions, TableReadOptions
from daft.table import Table, schema_inference, table_io

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)
PYARROW_GE_13_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (13, 0, 0)


def storage_config_from_use_native_downloader(use_native_downloader: bool) -> StorageConfig:
    if use_native_downloader:
        return StorageConfig.native(NativeStorageConfig(None))
    else:
        return StorageConfig.python(PythonStorageConfig(None))


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


@contextlib.contextmanager
def _parquet_write_helper(data: pa.Table, row_group_size: int = None, papq_write_table_kwargs: dict = {}):
    with tempfile.TemporaryDirectory() as directory_name:
        file = os.path.join(directory_name, "tempfile")
        papq.write_table(data, file, row_group_size=row_group_size, **papq_write_table_kwargs)
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
def test_parquet_infer_schema(data, expected_dtype, use_native_downloader):
    # HACK: Pyarrow 13 changed their schema parsing behavior so we receive DataType.list(..) instead of DataType.list(..)
    # However, our native downloader still parses DataType.list(..) regardless of PyArrow version
    if PYARROW_GE_13_0_0 and not use_native_downloader and expected_dtype == DataType.list(DataType.int64()):
        expected_dtype = DataType.list(DataType.int64())
    storage_config = storage_config_from_use_native_downloader(use_native_downloader)

    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [data, data, None],
            }
        )
    ) as f:
        schema = schema_inference.from_parquet(f, storage_config=storage_config)
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
        ([[], [1, 2], None], daft.Series.from_pylist([[[], [1, 2], None], [[], [1, 2], None], None])),
        ({"foo": 1}, daft.Series.from_pylist([{"foo": 1}, {"foo": 1}, None])),
    ],
)
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_parquet_read_data(data, expected_data_series, use_native_downloader):
    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [data, data, None],
            }
        )
    ) as f:
        schema = Schema._from_field_name_and_types(
            [("id", DataType.int64()), ("data", expected_data_series.datatype())]
        )
        expected = Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": expected_data_series,
            }
        )
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_parquet(f, schema, storage_config=storage_config)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("row_group_size", [None, 1, 3])
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_parquet_read_data_limit_rows(row_group_size, use_native_downloader):
    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [1, 2, None],
            }
        ),
        row_group_size=row_group_size,
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = Table.from_pydict(
            {
                "id": [1, 2],
                "data": [1, 2],
            }
        )
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_parquet(
            f, schema, read_options=TableReadOptions(num_rows=2), storage_config=storage_config
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_parquet_read_data_multi_row_groups():
    path = "tests/assets/parquet-data/mvp.parquet"
    table = Table.read_parquet(path)
    expected = Table.from_arrow(papq.read_table(path))
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_parquet_read_data_select_columns(use_native_downloader):
    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [1, 2, None],
            }
        )
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = Table.from_pydict(
            {
                "data": [1, 2, None],
            }
        )
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_parquet(
            f, schema, read_options=TableReadOptions(column_names=["data"]), storage_config=storage_config
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


###
# Test Parquet Int96 timestamps
###


@pytest.mark.parametrize("use_native_downloader", [True, False])
@pytest.mark.parametrize("use_deprecated_int96_timestamps", [True, False])
def test_parquet_read_int96_timestamps(use_deprecated_int96_timestamps, use_native_downloader):
    data = {
        "timestamp_ms": pa.array([1, 2, 3], pa.timestamp("ms")),
        "timestamp_us": pa.array([1, 2, 3], pa.timestamp("us")),
    }
    schema = [
        ("timestamp_ms", DataType.timestamp(TimeUnit.ms())),
        ("timestamp_us", DataType.timestamp(TimeUnit.us())),
    ]
    # int64 timestamps cannot support nanosecond resolutions
    if use_deprecated_int96_timestamps:
        data["timestamp_ns"] = pa.array([1, 2, 3], pa.timestamp("ns"))
        schema.append(("timestamp_ns", DataType.timestamp(TimeUnit.ns())))

    papq_write_table_kwargs = {
        "use_deprecated_int96_timestamps": use_deprecated_int96_timestamps,
        "coerce_timestamps": "us" if not use_deprecated_int96_timestamps else None,
    }
    if PYARROW_GE_11_0_0:
        papq_write_table_kwargs["store_schema"] = False

    with _parquet_write_helper(
        pa.Table.from_pydict(data),
        papq_write_table_kwargs=papq_write_table_kwargs,
    ) as f:
        schema = Schema._from_field_name_and_types(schema)
        expected = Table.from_pydict(data)
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_parquet(
            f,
            schema,
            read_options=TableReadOptions(column_names=schema.column_names()),
            storage_config=storage_config,
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
@pytest.mark.parametrize("coerce_to", [TimeUnit.ms(), TimeUnit.us()])
def test_parquet_read_int96_timestamps_overflow(coerce_to, use_native_downloader):
    # NOTE: datetime.datetime(3000, 1, 1) and datetime.datetime(1000, 1, 1) cannot be represented by our timestamp64(nanosecond)
    # type. However they can be written to Parquet's INT96 type. Here we test that a round-trip is possible if provided with
    # the appropriate flags.
    data = {
        "timestamp": pa.array(
            [datetime.datetime(1000, 1, 1), datetime.datetime(2000, 1, 1), datetime.datetime(3000, 1, 1)],
            pa.timestamp(str(coerce_to)),
        ),
    }
    schema = [
        ("timestamp", DataType.timestamp(coerce_to)),
    ]

    papq_write_table_kwargs = {
        "use_deprecated_int96_timestamps": True,
    }
    if PYARROW_GE_11_0_0:
        papq_write_table_kwargs["store_schema"] = False

    with _parquet_write_helper(
        pa.Table.from_pydict(data),
        papq_write_table_kwargs=papq_write_table_kwargs,
    ) as f:
        schema = Schema._from_field_name_and_types(schema)
        expected = Table.from_pydict(data)
        storage_config = storage_config_from_use_native_downloader(use_native_downloader)
        table = table_io.read_parquet(
            f,
            schema,
            read_options=TableReadOptions(column_names=schema.column_names()),
            parquet_options=TableParseParquetOptions(coerce_int96_timestamp_unit=coerce_to),
            storage_config=storage_config,
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("coerce_to", [TimeUnit.ms(), TimeUnit.us()])
@pytest.mark.parametrize("store_schema", [True, False])
def test_parquet_read_int96_timestamps_schema_inference(coerce_to, store_schema):
    dt = datetime.datetime(2000, 1, 1)
    ns_ts_array = pa.array(
        [dt, dt, dt],
        pa.timestamp("ns"),
    )
    data = {
        "timestamp": ns_ts_array,
        "nested_timestamp": pa.array([[dt], [dt], [dt]], type=pa.list_(pa.timestamp("ns"))),
        "struct_timestamp": pa.array([{"foo": dt} for _ in range(3)], type=pa.struct({"foo": pa.timestamp("ns")})),
        "struct_nested_timestamp": pa.array(
            [{"foo": [dt]} for _ in range(3)], type=pa.struct({"foo": pa.list_(pa.timestamp("ns"))})
        ),
    }
    schema = [
        ("timestamp", DataType.timestamp(coerce_to)),
        ("nested_timestamp", DataType.list(DataType.timestamp(coerce_to))),
        ("struct_timestamp", DataType.struct({"foo": DataType.timestamp(coerce_to)})),
        ("struct_nested_timestamp", DataType.struct({"foo": DataType.list(DataType.timestamp(coerce_to))})),
    ]
    expected = Schema._from_field_name_and_types(schema)

    papq_write_table_kwargs = {
        "use_deprecated_int96_timestamps": True,
    }
    if PYARROW_GE_11_0_0:
        papq_write_table_kwargs["store_schema"] = store_schema

    with _parquet_write_helper(
        pa.Table.from_pydict(data),
        papq_write_table_kwargs=papq_write_table_kwargs,
    ) as f:
        schema = Schema.from_parquet(f, coerce_int96_timestamp_unit=coerce_to)
        assert schema == expected, f"Expected:\n{expected}\n\nReceived:\n{schema}"
