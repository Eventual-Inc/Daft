from __future__ import annotations

import contextlib
import datetime
import pathlib
import tempfile

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

import daft
from daft.datatype import DataType, TimeUnit
from daft.logical.schema import Schema
from daft.runners.partitioning import TableReadOptions
from daft.table import Table, schema_inference, table_io


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
    with tempfile.NamedTemporaryFile() as tmpfile:
        papq.write_table(data, tmpfile.name, row_group_size=row_group_size, **papq_write_table_kwargs)
        yield tmpfile.name


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
@pytest.mark.parametrize("use_native_downloader", [True, False])
def test_parquet_infer_schema(data, expected_dtype, use_native_downloader):
    with _parquet_write_helper(
        pa.Table.from_pydict(
            {
                "id": [1, 2, 3],
                "data": [data, data, None],
            }
        )
    ) as f:
        schema = schema_inference.from_parquet(f, use_native_downloader=use_native_downloader)
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
        table = table_io.read_parquet(f, schema, use_native_downloader=use_native_downloader)
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
        table = table_io.read_parquet(
            f, schema, read_options=TableReadOptions(num_rows=2), use_native_downloader=use_native_downloader
        )
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
        table = table_io.read_parquet(
            f, schema, read_options=TableReadOptions(column_names=["data"]), use_native_downloader=use_native_downloader
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"


@pytest.mark.parametrize("use_native_downloader", [True, False])
@pytest.mark.parametrize("use_deprecated_int96_timestamps", [True, False])
def test_parquet_read_timestamps(use_native_downloader, use_deprecated_int96_timestamps):
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

    with _parquet_write_helper(
        pa.Table.from_pydict(data),
        papq_write_table_kwargs={
            "use_deprecated_int96_timestamps": use_deprecated_int96_timestamps,
            "coerce_timestamps": "us" if not use_deprecated_int96_timestamps else None,
        },
    ) as f:
        schema = Schema._from_field_name_and_types(schema)
        expected = Table.from_pydict(data)
        table = table_io.read_parquet(
            f,
            schema,
            read_options=TableReadOptions(column_names=schema.column_names()),
            use_native_downloader=use_native_downloader,
        )
        assert table.to_arrow() == expected.to_arrow(), f"Expected:\n{expected}\n\nReceived:\n{table}"
