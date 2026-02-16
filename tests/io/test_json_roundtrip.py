from __future__ import annotations

import datetime
import decimal
import gzip
import json

import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit
from tests.conftest import get_tests_daft_runner_name

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype", "expected_inferred_dtype"],
    [
        ([1, 2, None], pa.int64(), DataType.int64(), DataType.int64()),
        (["a", "b", ""], pa.large_string(), DataType.string(), DataType.string()),
        # TODO(desmond): Arrow-rs writes binaries as strings of hexadecimals. Technically the JSON spec (RFC 8259) does not
        #                support BINARY. In practice, libraries such as SIMD-JSON expects binary to be represented as a
        #                string that's properly encoded e.g. you can encode your binary values in base64.
        #                We should make our readers and writers compatible.
        # ([b"a", b"b", b""], pa.large_binary(), DataType.binary(), DataType.string()),
        ([True, False, None], pa.bool_(), DataType.bool(), DataType.bool()),
        ([None, None, None], pa.null(), DataType.null(), DataType.null()),
        (
            [decimal.Decimal("1.23"), decimal.Decimal("1.24"), None],
            pa.decimal128(16, 8),
            DataType.decimal128(16, 8),
            DataType.float64(),
        ),
        ([datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None], pa.date32(), DataType.date(), DataType.date()),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("us"),
            DataType.time(TimeUnit.us()),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.time(1, 2, 3, 4), datetime.time(5, 6, 7, 8), None],
            pa.time64("ns"),
            DataType.time(TimeUnit.ns()),
            DataType.time(TimeUnit.us()),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("ms"),
            DataType.timestamp(TimeUnit.ms()),
            # NOTE: Seems like the inferred type is seconds because it's written with seconds resolution
            DataType.timestamp(TimeUnit.s()),
        ),
        (
            [datetime.date(1994, 1, 1), datetime.date(1995, 1, 1), None],
            pa.date64(),
            DataType.timestamp(TimeUnit.ms()),
            DataType.timestamp(TimeUnit.s()),
        ),
        (
            [datetime.datetime(1994, 1, 1), datetime.datetime(1995, 1, 1), None],
            pa.timestamp("us", tz="America/New_York"),
            DataType.timestamp(TimeUnit.us(), timezone="America/New_York"),
            # NOTE: Timezone inference uses the offset value, not the timezone string
            DataType.timestamp(TimeUnit.s(), timezone="-05:00"),
        ),
        # TODO(desmond): Arrow-rs also currently writes durations in ISO 8601 duration format while our reader expects an i64.
        #                We should make our readers and writers compatible.
        # (
        #     [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
        #     pa.duration("ms"),
        #     DataType.duration(TimeUnit.ms()),
        #     # NOTE: Duration ends up being written as int64
        #     DataType.int64(),
        # ),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype, expected_inferred_dtype):
    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_inferred_dtype
    assert before.to_arrow() == after.with_column("foo", after["foo"].cast(expected_dtype)).to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_roundtrip_struct_types(tmp_path):
    struct_data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "San Francisco"},
        None,
    ]

    pa_struct_type = pa.struct({"name": pa.string(), "age": pa.int64(), "city": pa.string()})

    expected_dtype = DataType.struct({"name": DataType.string(), "age": DataType.int64(), "city": DataType.string()})

    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "person": pa.array(struct_data, type=pa_struct_type)}))
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))

    assert before.schema()["person"].dtype == expected_dtype
    assert after.schema()["person"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native" or not PYARROW_GE_11_0_0,
    reason="JSON writes are only implemented in the native runner, and map types require PyArrow >= 11.0.0",
)
def test_roundtrip_map_types(tmp_path):
    map_data = [
        {"key1": "value1", "key2": "value2"},
        {"key1": "value3"},
        None,
    ]

    pa_map_type = pa.map_(pa.string(), pa.string())
    expected_dtype = DataType.map(DataType.string(), DataType.string())

    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "metadata": pa.array(map_data, type=pa_map_type)}))
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))

    assert before.schema()["metadata"].dtype == expected_dtype
    # In JSON we cannot determine if a type is MAP and not STRUCT.
    # However, since casting from struct to map is not implemented, we'll just verify the data is preserved
    # by checking that the struct representation matches the expected structure.
    assert after.schema()["metadata"].dtype == DataType.struct({"key1": DataType.string(), "key2": DataType.string()})
    # Verify the data length is preserved.
    assert before.count_rows() == after.count_rows()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_roundtrip_nested_struct_with_arrays(tmp_path):
    """Test JSON roundtrip with nested structs containing arrays."""
    nested_struct_data = [
        {"name": "Alice", "scores": [85, 90, 78], "tags": ["student", "active"]},
        {"name": "Bob", "scores": [92, 88, 95], "tags": ["student"]},
        None,
    ]

    pa_nested_struct_type = pa.struct(
        {"name": pa.string(), "scores": pa.list_(pa.int64()), "tags": pa.list_(pa.string())}
    )

    expected_dtype = DataType.struct(
        {"name": DataType.string(), "scores": DataType.list(DataType.int64()), "tags": DataType.list(DataType.string())}
    )

    before = daft.from_arrow(
        pa.table({"id": pa.array(range(3)), "student": pa.array(nested_struct_data, type=pa_nested_struct_type)})
    )
    before = before.concat(before)
    before.write_json(str(tmp_path))
    after = daft.read_json(str(tmp_path))
    assert before.schema()["student"].dtype == expected_dtype
    assert after.schema()["student"].dtype == expected_dtype
    assert before.to_arrow() == after.to_arrow()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_throws_error_on_duration_and_binary_types(tmp_path):
    # TODO(desmond): Binary and Duration types currently produce inconsistent behaviours between our readers and writers.
    # Our readers expect BINARY to be encoded as plain text, and DURATION to be encoded with i64. Arrow-rs expects BINARY
    # to be hexadecimal encoded and DURATION to follow the ISO 8601 duration format. Until we reconcile this difference,
    # opt for throwing an error on encountering these types.
    duration_data = [
        datetime.timedelta(days=1),
        datetime.timedelta(days=2),
        None,
    ]

    pa_duration_type = pa.duration("ms")

    before_duration = daft.from_arrow(
        pa.table({"id": pa.array(range(3)), "duration": pa.array(duration_data, type=pa_duration_type)})
    )
    before_duration = before_duration.concat(before_duration)

    # Test that writing duration types throws NotImplementedError
    with pytest.raises(
        daft.exceptions.DaftCoreException,
        match="Not Yet Implemented: JSON writes are not supported with extension, binary, or duration data types",
    ):
        before_duration.write_json(str(tmp_path))

    binary_data = [b"hello", b"world", None]
    pa_binary_type = pa.large_binary()
    before_binary = daft.from_arrow(
        pa.table({"id": pa.array(range(3)), "binary": pa.array(binary_data, type=pa_binary_type)})
    )

    with pytest.raises(
        daft.exceptions.DaftCoreException,
        match="Not Yet Implemented: JSON writes are not supported with extension, binary, or duration data types",
    ):
        before_binary.write_json(str(tmp_path))


def write_ndjson_file(path, data, compression=None):
    """Write data to NDJSON file with optional gzip compression."""
    if compression == "gzip":
        with gzip.open(path, "wt", encoding="utf-8") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")
    else:
        with open(path, "w") as f:
            for item in data:
                f.write(json.dumps(item) + "\n")


@pytest.mark.parametrize("compression", [None, "gzip"])
def test_roundtrip_ndjson_with_mismatched_schema_between_records(tmp_path, compression):
    data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "state": "California"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]

    filename = "data.ndjson.gz" if compression else "data.ndjson"
    path = tmp_path / filename

    write_ndjson_file(path, data, compression)

    df = daft.read_json(str(path))
    assert df.to_pydict() == {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
        "city": ["New York", None, "Chicago"],
        "state": [None, "California", None],
    }


@pytest.mark.parametrize("compression", [None, "gzip"])
def test_roundtrip_ndjson_with_mismatched_schema_between_files(tmp_path, compression):
    data1 = [
        {"name": "Alice", "age": 30, "city": "New York", "state": "New York"},
        {"name": "Bob", "age": 25, "city": "San Francisco", "state": "California"},
    ]

    data2 = [
        {"name": "Charlie", "age": 35, "state": "California"},
        {"name": "David", "age": 40, "state": "New York"},
    ]

    extension = ".ndjson.gz" if compression else ".ndjson"
    path1 = tmp_path / f"data{extension}"
    path2 = tmp_path / f"data2{extension}"

    write_ndjson_file(path1, data1, compression)
    write_ndjson_file(path2, data2, compression)

    df = daft.read_json([str(path1), str(path2)])
    assert df.to_pydict() == {
        "name": ["Alice", "Bob", "Charlie", "David"],
        "age": [30, 25, 35, 40],
        "city": ["New York", "San Francisco", None, None],  # second file is missing city
        "state": ["New York", "California", "California", "New York"],
    }


def _read_first_json_file_text(root: str) -> str:
    """Read the text content of the first JSON file in the directory."""
    import os

    files = [os.path.join(root, f) for f in os.listdir(root)]
    files = [f for f in files if os.path.isfile(f)]
    assert len(files) > 0
    with open(files[0], encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_custom_date_format(tmp_path):
    """Test custom date formatting when writing JSON files."""
    dates = [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31), None]
    df = daft.from_pydict({"id": [1, 2, 3], "date": dates})

    # Test with custom date format dd/MM/yyyy
    df.write_json(str(tmp_path), write_mode="overwrite", date_format="%d/%m/%Y")
    text = _read_first_json_file_text(str(tmp_path))

    # Check that dates are formatted correctly
    assert "15/01/2024" in text
    assert "31/12/2024" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_custom_timestamp_format(tmp_path):
    """Test custom timestamp formatting when writing JSON files."""
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
        None,
    ]
    df = daft.from_pydict({"id": [1, 2, 3], "timestamp": timestamps})

    # Test with custom timestamp format
    df.write_json(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S")
    text = _read_first_json_file_text(str(tmp_path))

    # Check that timestamps are formatted correctly
    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_custom_date_and_timestamp_format(tmp_path):
    """Test both custom date and timestamp formatting when writing JSON files."""
    dates = [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31), None]
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
        None,
    ]
    df = daft.from_pydict({"id": [1, 2, 3], "date": dates, "timestamp": timestamps})

    # Test with both custom formats
    df.write_json(
        str(tmp_path),
        write_mode="overwrite",
        date_format="%d/%m/%Y",
        timestamp_format="%Y-%m-%d %H:%M:%S",
    )
    text = _read_first_json_file_text(str(tmp_path))

    # Check that dates are formatted correctly
    assert "15/01/2024" in text
    assert "31/12/2024" in text

    # Check that timestamps are formatted correctly
    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_iso8601_timestamp_format(tmp_path):
    """Test ISO 8601 / RFC 3339 timestamp formatting."""
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
        None,
    ]
    df = daft.from_pydict({"id": [1, 2, 3], "timestamp": timestamps})

    # Test with ISO 8601 format (%+)
    df.write_json(str(tmp_path), write_mode="overwrite", timestamp_format="%+")
    text = _read_first_json_file_text(str(tmp_path))

    # Check that timestamps are formatted in ISO 8601 format
    # The format should be like "2024-01-15T10:30:45+00:00"
    assert "2024-01-15T10:30:45" in text
    assert "2024-12-31T23:59:59" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_timezone_aware_timestamp(tmp_path):
    """Test that timezone-aware timestamps are formatted in their timezone."""
    # Create timezone-aware timestamps
    df = daft.from_arrow(
        pa.table(
            {
                "id": [1, 2, 3],
                "timestamp": pa.array(
                    [
                        datetime.datetime(1994, 1, 1, tzinfo=datetime.timezone.utc),
                        datetime.datetime(1995, 1, 1, tzinfo=datetime.timezone.utc),
                        None,
                    ],
                    type=pa.timestamp("us", tz="America/New_York"),
                ),
            }
        )
    )

    # Write with custom format
    df.write_json(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S %Z")
    text = _read_first_json_file_text(str(tmp_path))

    # Timestamps should be formatted in New York time, not UTC
    # 1994-01-01 00:00:00 UTC = 1993-12-31 19:00:00 EST
    assert "1993-12-31 19:00:00" in text or "EST" in text or "EDT" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_invalid_timezone_raises_error(tmp_path):
    """Test that invalid timezone strings raise an error instead of silently using UTC."""
    # Create timestamps with an invalid timezone string
    df = daft.from_arrow(
        pa.table(
            {
                "id": [1, 2, 3],
                "timestamp": pa.array(
                    [
                        datetime.datetime(1994, 1, 1, tzinfo=datetime.timezone.utc),
                        datetime.datetime(1995, 1, 1, tzinfo=datetime.timezone.utc),
                        None,
                    ],
                    type=pa.timestamp("us", tz="America/New_Yrok"),  # Typo: should be New_York
                ),
            }
        )
    )

    # Should raise an error about invalid timezone
    with pytest.raises(Exception, match="Invalid timezone"):
        df.write_json(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S")


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_date64_with_timestamp_format(tmp_path):
    """Test that Date64 (which Daft converts to Timestamp[ms]) uses timestamp_format."""
    # Date64 is converted to Timestamp[ms] internally by Daft
    df = daft.from_arrow(
        pa.table(
            {
                "id": [1, 2, 3],
                "date": pa.array(
                    [
                        datetime.date(2024, 1, 15),
                        datetime.date(2024, 12, 31),
                        None,
                    ],
                    type=pa.date64(),
                ),
            }
        )
    )

    # Since Date64 becomes Timestamp[ms], we need to use timestamp_format
    df.write_json(str(tmp_path), write_mode="overwrite", timestamp_format="%d/%m/%Y")
    text = _read_first_json_file_text(str(tmp_path))

    assert "15/01/2024" in text
    assert "31/12/2024" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
@pytest.mark.parametrize("time_unit", ["s", "ms", "us", "ns"])
def test_write_json_timestamp_all_time_units(tmp_path, time_unit):
    """Test timestamp formatting works for all time units (second, millisecond, microsecond, nanosecond)."""
    df = daft.from_arrow(
        pa.table(
            {
                "id": [1, 2],
                "timestamp": pa.array(
                    [
                        datetime.datetime(2024, 1, 15, 10, 30, 45),
                        datetime.datetime(2024, 12, 31, 23, 59, 59),
                    ],
                    type=pa.timestamp(time_unit),
                ),
            }
        )
    )

    df.write_json(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S")
    text = _read_first_json_file_text(str(tmp_path))

    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_no_custom_format_preserves_default(tmp_path):
    """Test that not providing custom formats preserves default Arrow JSON behavior."""
    dates = [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31)]
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
    ]
    df = daft.from_pydict({"id": [1, 2], "date": dates, "timestamp": timestamps})

    # Write without custom formats - should use Arrow's default formatting
    df.write_json(str(tmp_path), write_mode="overwrite")
    text = _read_first_json_file_text(str(tmp_path))

    # Default format should still contain the date/timestamp data
    assert "2024-01-15" in text
    assert "2024-12-31" in text


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_invalid_date_format_raises_error(tmp_path):
    """Test that invalid date format strings raise a clear error."""
    dates = [datetime.date(2024, 1, 15)]
    df = daft.from_pydict({"date": dates})

    with pytest.raises(Exception) as exc_info:
        df.write_json(str(tmp_path), date_format="%Q")

    # Check that the error message is clear about the invalid format
    assert "Invalid date format string" in str(exc_info.value)


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "native", reason="JSON writes are only implemented in the native runner"
)
def test_write_json_invalid_timestamp_format_raises_error(tmp_path):
    """Test that invalid timestamp format strings raise a clear error."""
    timestamps = [datetime.datetime(2024, 1, 15, 10, 30, 45)]
    df = daft.from_pydict({"timestamp": timestamps})

    with pytest.raises(Exception) as exc_info:
        df.write_json(str(tmp_path), timestamp_format="%Q")

    # Check that the error message is clear about the invalid format
    assert "Invalid timestamp format string" in str(exc_info.value)
