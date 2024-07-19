from __future__ import annotations

import contextlib
import datetime
import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.daft import NativeStorageConfig, PythonStorageConfig, StorageConfig
from daft.datatype import DataType, TimeUnit
from daft.logical.schema import Schema
from daft.table import MicroPartition

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)
PYARROW_GE_13_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (13, 0, 0)


###
# Test Parquet Int96 timestamps
###


@contextlib.contextmanager
def _parquet_write_helper(data: pa.Table, row_group_size: int = None, papq_write_table_kwargs: dict = {}):
    with tempfile.TemporaryDirectory() as directory_name:
        file = os.path.join(directory_name, "tempfile")
        papq.write_table(data, file, row_group_size=row_group_size, **papq_write_table_kwargs)
        yield file


def storage_config_from_use_native_downloader(use_native_downloader: bool) -> StorageConfig:
    if use_native_downloader:
        return StorageConfig.native(NativeStorageConfig(True, None))
    else:
        return StorageConfig.python(PythonStorageConfig(None))


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
        expected = MicroPartition.from_pydict(data)
        df = daft.read_parquet(f, schema_hints={k: v for k, v in schema}, use_native_downloader=use_native_downloader)
        assert df.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{df.to_arrow()}"


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

    papq_write_table_kwargs = {
        "use_deprecated_int96_timestamps": True,
    }
    if PYARROW_GE_11_0_0:
        papq_write_table_kwargs["store_schema"] = False

    with _parquet_write_helper(
        pa.Table.from_pydict(data),
        papq_write_table_kwargs=papq_write_table_kwargs,
    ) as f:
        expected = MicroPartition.from_pydict(data)
        df = daft.read_parquet(f, coerce_int96_timestamp_unit=coerce_to, use_native_downloader=use_native_downloader)

        assert df.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{df}"


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
        "map_timestamp": pa.array([[("foo", dt)] for _ in range(3)], type=pa.map_(pa.string(), pa.timestamp("ns"))),
    }
    schema = [
        ("timestamp", DataType.timestamp(coerce_to)),
        ("nested_timestamp", DataType.list(DataType.timestamp(coerce_to))),
        ("struct_timestamp", DataType.struct({"foo": DataType.timestamp(coerce_to)})),
        ("struct_nested_timestamp", DataType.struct({"foo": DataType.list(DataType.timestamp(coerce_to))})),
        ("map_timestamp", DataType.map(DataType.string(), DataType.timestamp(coerce_to))),
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
        schema = daft.read_parquet(f, coerce_int96_timestamp_unit=coerce_to).schema()
        assert schema == expected, f"Expected:\n{expected}\n\nReceived:\n{schema}"


def test_row_groups():
    path = ["tests/assets/parquet-data/mvp.parquet"]

    df = daft.read_parquet(path).collect()
    assert df.count_rows() == 100
    df = daft.read_parquet(path, row_groups=[[0, 1]]).collect()
    assert df.count_rows() == 20
