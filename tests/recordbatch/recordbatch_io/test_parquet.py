from __future__ import annotations

import contextlib
import datetime
import os
import pathlib
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.datatype import DataType, TimeUnit
from daft.exceptions import DaftCoreException
from daft.logical.schema import Schema
from daft.recordbatch import (
    MicroPartition,
    read_parquet_into_pyarrow,
    read_parquet_into_pyarrow_bulk,
    recordbatch_io,
)
from daft.runners.partitioning import TableParseParquetOptions, TableReadOptions

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)
PYARROW_GE_13_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (13, 0, 0)


def test_read_input(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    data = pa.Table.from_pydict({"foo": [1, 2, 3]})
    with open(tmpdir / "file.parquet", "wb") as f:
        papq.write_table(data, f)

    schema = Schema._from_field_name_and_types([("foo", DataType.int64())])

    # Test pathlib, str and IO
    assert recordbatch_io.read_parquet(tmpdir / "file.parquet", schema=schema).to_arrow() == data
    assert recordbatch_io.read_parquet(str(tmpdir / "file.parquet"), schema=schema).to_arrow() == data


@contextlib.contextmanager
def _parquet_write_helper(data: pa.Table, row_group_size: int | None = None, papq_write_table_kwargs: dict = {}):
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
def test_parquet_infer_schema(data, expected_dtype):
    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [data, data, None],
            }
        )
    ) as f:
        schema = Schema.from_parquet(f)
        assert schema == Schema._from_field_name_and_types([("id", DataType.int64()), ("data", expected_dtype)])


def test_parquet_read_empty():
    with _parquet_write_helper(pa.Table.from_pydict({"foo": pa.array([], type=pa.int64())})) as f:
        schema = Schema._from_field_name_and_types([("foo", DataType.int64())])
        expected = MicroPartition.from_pydict({"foo": pa.array([], type=pa.int64())})
        table = recordbatch_io.read_parquet(f, schema)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


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
def test_parquet_read_data(data, expected_data_series):
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
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2, 3],
                "data": expected_data_series,
            }
        )
        table = recordbatch_io.read_parquet(f, schema)
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("row_group_size", [None, 1, 3])
def test_parquet_read_data_limit_rows(row_group_size):
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
        expected = MicroPartition.from_pydict(
            {
                "id": [1, 2],
                "data": [1, 2],
            }
        )
        table = recordbatch_io.read_parquet(f, schema, read_options=TableReadOptions(num_rows=2))
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_parquet_read_data_multi_row_groups():
    path = "tests/assets/parquet-data/mvp.parquet"
    table = MicroPartition.read_parquet(path)
    expected = MicroPartition.from_arrow(papq.read_table(path))
    assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


def test_parquet_read_data_select_columns():
    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [1, 2, None],
            }
        )
    ) as f:
        schema = Schema._from_field_name_and_types([("id", DataType.int64()), ("data", DataType.int64())])
        expected = MicroPartition.from_pydict(
            {
                "data": [1, 2, None],
            }
        )
        table = recordbatch_io.read_parquet(f, schema, read_options=TableReadOptions(column_names=["data"]))
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


###
# Test Parquet Int96 timestamps
###


@pytest.mark.parametrize("use_deprecated_int96_timestamps", [True, False])
def test_parquet_read_int96_timestamps(use_deprecated_int96_timestamps):
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
        expected = MicroPartition.from_pydict(data)
        table = recordbatch_io.read_parquet(
            f,
            schema,
            read_options=TableReadOptions(column_names=schema.column_names()),
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("coerce_to", [TimeUnit.ms(), TimeUnit.us()])
def test_parquet_read_int96_timestamps_overflow(coerce_to):
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
        expected = MicroPartition.from_pydict(data)
        table = recordbatch_io.read_parquet(
            f,
            schema,
            read_options=TableReadOptions(column_names=schema.column_names()),
            parquet_options=TableParseParquetOptions(coerce_int96_timestamp_unit=coerce_to),
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


@pytest.mark.parametrize("n_bytes", [0, 1, 2, 7])
def test_read_too_small_parquet_file(tmpdir, n_bytes):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    with open(file_path, "wb") as f:
        for _ in range(n_bytes):
            f.write(b"0")
    with pytest.raises(ValueError, match="smaller than the minimum size of 12 bytes"):
        MicroPartition.read_parquet(file_path.as_posix())


def test_read_empty_parquet_file_with_table(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = MicroPartition.read_parquet(file_path.as_posix()).to_arrow()
    assert tab == read_back


def test_read_empty_parquet_file_with_pyarrow(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = read_parquet_into_pyarrow(file_path.as_posix())
    assert tab == read_back


def test_read_parquet_file_missing_column_with_pyarrow(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = read_parquet_into_pyarrow(file_path.as_posix(), columns=["MISSING"])
    assert tab.drop("x") == read_back  # same length, but no columns, as original table


def test_read_parquet_file_missing_column_partial_read_with_pyarrow(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([1, 2, 3], type=pa.int64()), "y": pa.array([1, 2, 3], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = read_parquet_into_pyarrow(file_path.as_posix(), columns=["x", "MISSING"])
    assert tab.drop("y") == read_back  # only read "x"


def test_read_empty_parquet_file_with_pyarrow_bulk(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = read_parquet_into_pyarrow_bulk([file_path.as_posix()])
    assert len(read_back) == 1
    assert tab == read_back[0]


def test_read_parquet_file_missing_column_with_pyarrow_bulk(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = read_parquet_into_pyarrow_bulk([file_path.as_posix()], columns=["MISSING"])
    assert len(read_back) == 1
    assert tab.drop("x") == read_back[0]  # same length, but no columns, as original table


def test_read_parquet_file_missing_column_partial_read_with_pyarrow_bulk(tmpdir):
    tmpdir = pathlib.Path(tmpdir)
    file_path = tmpdir / "file.parquet"
    tab = pa.table({"x": pa.array([1, 2, 3], type=pa.int64()), "y": pa.array([1, 2, 3], type=pa.int64())})
    papq.write_table(tab, file_path.as_posix())
    read_back = read_parquet_into_pyarrow_bulk([file_path.as_posix()], columns=["x", "MISSING"])
    assert len(read_back) == 1
    assert tab.drop("y") == read_back[0]  # only read "x"


@pytest.mark.parametrize(
    "parquet_path", [Path(__file__).parents[2] / "assets" / "parquet-data" / "invalid_utf8.parquet"]
)
def test_parquet_read_string_utf8_into_binary(parquet_path: Path):
    import pyarrow as pa

    assert parquet_path.exists()

    with pytest.raises(DaftCoreException, match="invalid utf-8 sequence"):
        read_parquet_into_pyarrow(path=parquet_path.as_posix())

    read_back = read_parquet_into_pyarrow(path=parquet_path.as_posix(), string_encoding="raw")
    schema = read_back.schema
    assert len(schema) == 1
    assert schema[0].name == "invalid_string"
    assert schema[0].type == pa.binary()
    assert read_back["invalid_string"][0].as_py() == b"\x80\x80\x80"
