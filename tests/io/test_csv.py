from __future__ import annotations

import datetime
import decimal

import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit

PYARROW_GE_11_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (11, 0, 0)


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
@pytest.mark.parametrize(
    ["data", "pa_type", "expected_dtype", "expected_inferred_dtype"],
    [
        ([1, 2, None], pa.int64(), DataType.int64(), DataType.int64()),
        (["a", "b", ""], pa.large_string(), DataType.string(), DataType.string()),
        ([b"a", b"b", b""], pa.large_binary(), DataType.binary(), DataType.string()),
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
        (
            [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
            pa.duration("ms"),
            DataType.duration(TimeUnit.ms()),
            # NOTE: Duration ends up being written as int64
            DataType.int64(),
        ),
    ],
)
def test_roundtrip_simple_arrow_types(tmp_path, data, pa_type, expected_dtype, expected_inferred_dtype):
    before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
    before = before.concat(before)
    before.write_csv(str(tmp_path))
    after = daft.read_csv(str(tmp_path))
    assert before.schema()["foo"].dtype == expected_dtype
    assert after.schema()["foo"].dtype == expected_inferred_dtype
    assert before.to_arrow() == after.with_column("foo", after["foo"].cast(expected_dtype)).to_arrow()


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
@pytest.mark.parametrize(
    ["data", "pa_type"],
    [
        ([[1, 2, 3], [], None], pa.large_list(pa.int64())),
        ([[1, 2, 3], [4, 5, 6], None], pa.list_(pa.int64(), list_size=3)),
        ([{"bar": 1}, {"bar": None}, None], pa.struct([("bar", pa.int64())])),
        ([{"k": 1}, {"k": None}, None], pa.map_(pa.large_string(), pa.int64())),
        ([b"\xff\xfe", b"\x80\x81", None], pa.large_binary()),
    ],
)
def test_write_csv_unsupported_types_raise(tmp_path, data, pa_type):
    with pytest.raises(Exception):
        before = daft.from_arrow(pa.table({"id": pa.array(range(3)), "foo": pa.array(data, type=pa_type)}))
        before.write_csv(str(tmp_path))


def test_write_and_read_empty_csv(tmp_path_factory):
    empty_csv_files = str(tmp_path_factory.mktemp("empty_csv"))
    df = daft.from_pydict({"a": []})
    df.write_csv(empty_csv_files, write_mode="overwrite")

    assert daft.read_csv(empty_csv_files).to_pydict() == {"a": []}


def _read_first_file_text(root: str) -> str:
    import os

    files = [os.path.join(root, f) for f in os.listdir(root)]
    files = [f for f in files if os.path.isfile(f)]
    assert len(files) > 0
    with open(files[0], encoding="utf-8") as fh:
        return fh.read()


@pytest.mark.parametrize(
    ["delimiter", "header", "quote", "escape_char", "foo_values", "expected_text"],
    [
        ("|", True, None, None, ["a", "b|c", "d"], 'id|foo\n1|a\n2|"b|c"\n3|d\n'),
        (";", True, None, None, ["a", "b;c", "d"], 'id;foo\n1;a\n2;"b;c"\n3;d\n'),
        ("\t", True, None, None, ["a", "b\tc", "d"], 'id\tfoo\n1\ta\n2\t"b\tc"\n3\td\n'),
        (",", False, None, None, ["a", "b|c", "d"], "1,a\n2,b|c\n3,d\n"),
        (",", True, "'", None, ["a", "b,c", "d"], "id,foo\n1,a\n2,'b,c'\n3,d\n"),
        (",", True, '"', None, ["a", "b,c", "d"], 'id,foo\n1,a\n2,"b,c"\n3,d\n'),
        (",", True, "'", "\\", ["a", 'He said "hi,yo"', "d"], "id,foo\n1,a\n2,'He said \"hi,yo\"'\n3,d\n"),
        (",", True, "'", "\\", ["a", 'Say "hello"', "d"], 'id,foo\n1,a\n2,Say "hello"\n3,d\n'),
    ],
)
def test_write_csv_parametrized(tmp_path, delimiter, header, quote, escape_char, foo_values, expected_text):
    df = daft.from_pydict({"id": [1, 2, 3], "foo": foo_values})

    write_kwargs = {
        "write_mode": "overwrite",
        "delimiter": delimiter,
        "quote": quote,
        "escape": escape_char,
        "header": header,
    }
    df.write_csv(str(tmp_path), **write_kwargs)
    text = _read_first_file_text(str(tmp_path))
    assert text == expected_text

    if header:
        read_kwargs = {
            "delimiter": delimiter,
            "quote": quote,
            "escape_char": escape_char,
            "has_headers": header,
        }
        read_back = daft.read_csv(str(tmp_path), **read_kwargs)
        assert df.to_arrow() == read_back.to_arrow()


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_custom_date_format(tmp_path):
    """Test custom date formatting when writing CSV files."""
    dates = [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31), None]
    df = daft.from_pydict({"id": [1, 2, 3], "date": dates})

    # Test with custom date format dd/MM/yyyy
    df.write_csv(str(tmp_path), write_mode="overwrite", date_format="%d/%m/%Y")
    text = _read_first_file_text(str(tmp_path))

    # Check that dates are formatted correctly
    assert "15/01/2024" in text
    assert "31/12/2024" in text
    # Check header is present
    assert "id,date" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_custom_timestamp_format(tmp_path):
    """Test custom timestamp formatting when writing CSV files."""
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
        None,
    ]
    df = daft.from_pydict({"id": [1, 2, 3], "timestamp": timestamps})

    # Test with custom timestamp format
    df.write_csv(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S")
    text = _read_first_file_text(str(tmp_path))

    # Check that timestamps are formatted correctly
    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text
    # Check header is present
    assert "id,timestamp" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_custom_date_and_timestamp_format(tmp_path):
    """Test both custom date and timestamp formatting when writing CSV files."""
    dates = [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31), None]
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
        None,
    ]
    df = daft.from_pydict({"id": [1, 2, 3], "date": dates, "timestamp": timestamps})

    # Test with both custom formats
    df.write_csv(
        str(tmp_path),
        write_mode="overwrite",
        date_format="%d/%m/%Y",
        timestamp_format="%Y-%m-%d %H:%M:%S",
    )
    text = _read_first_file_text(str(tmp_path))

    # Check that dates are formatted correctly
    assert "15/01/2024" in text
    assert "31/12/2024" in text

    # Check that timestamps are formatted correctly
    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text

    # Check header is present
    assert "id,date,timestamp" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_iso8601_timestamp_format(tmp_path):
    """Test ISO 8601 / RFC 3339 timestamp formatting."""
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
        None,
    ]
    df = daft.from_pydict({"id": [1, 2, 3], "timestamp": timestamps})

    # Test with ISO 8601 format (%+)
    df.write_csv(str(tmp_path), write_mode="overwrite", timestamp_format="%+")
    text = _read_first_file_text(str(tmp_path))

    # Check that timestamps are formatted in ISO 8601 format
    # The format should be like "2024-01-15T10:30:45+00:00"
    assert "2024-01-15T10:30:45" in text
    assert "2024-12-31T23:59:59" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_timezone_aware_timestamp(tmp_path):
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
    df.write_csv(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S %Z")
    text = _read_first_file_text(str(tmp_path))

    # Timestamps should be formatted in New York time, not UTC
    # 1994-01-01 00:00:00 UTC = 1993-12-31 19:00:00 EST
    assert "1993-12-31 19:00:00" in text or "EST" in text or "EDT" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_invalid_timezone_raises_error(tmp_path):
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
        df.write_csv(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S")


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_date64_with_timestamp_format(tmp_path):
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
    df.write_csv(str(tmp_path), write_mode="overwrite", timestamp_format="%d/%m/%Y")
    text = _read_first_file_text(str(tmp_path))

    assert "15/01/2024" in text
    assert "31/12/2024" in text
    assert "id,date" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
@pytest.mark.parametrize("time_unit", ["s", "ms", "us", "ns"])
def test_write_csv_timestamp_all_time_units(tmp_path, time_unit):
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

    df.write_csv(str(tmp_path), write_mode="overwrite", timestamp_format="%Y-%m-%d %H:%M:%S")
    text = _read_first_file_text(str(tmp_path))

    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_write_csv_no_custom_format_preserves_default(tmp_path):
    """Test that not providing custom formats preserves default Arrow CSV behavior."""
    dates = [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31)]
    timestamps = [
        datetime.datetime(2024, 1, 15, 10, 30, 45),
        datetime.datetime(2024, 12, 31, 23, 59, 59),
    ]
    df = daft.from_pydict({"id": [1, 2], "date": dates, "timestamp": timestamps})

    # Write without custom formats - should use Arrow's default formatting
    df.write_csv(str(tmp_path), write_mode="overwrite")
    text = _read_first_file_text(str(tmp_path))

    # Default format should still contain the date/timestamp data
    assert "2024-01-15" in text
    assert "2024-12-31" in text
    assert "id,date,timestamp" in text


def test_write_csv_invalid_date_format_raises_error(tmp_path):
    """Test that invalid date format strings raise a clear error."""
    dates = [datetime.date(2024, 1, 15)]
    df = daft.from_pydict({"date": dates})

    with pytest.raises(Exception) as exc_info:
        df.write_csv(str(tmp_path), date_format="%Q")

    # Check that the error message is clear about the invalid format
    assert "Invalid date format string" in str(exc_info.value)


def test_write_csv_invalid_timestamp_format_raises_error(tmp_path):
    """Test that invalid timestamp format strings raise a clear error."""
    timestamps = [datetime.datetime(2024, 1, 15, 10, 30, 45)]
    df = daft.from_pydict({"timestamp": timestamps})

    with pytest.raises(Exception) as exc_info:
        df.write_csv(str(tmp_path), timestamp_format="%Q")

    # Check that the error message is clear about the invalid format
    assert "Invalid timestamp format string" in str(exc_info.value)


@pytest.mark.skipif(
    not PYARROW_GE_11_0_0,
    reason="PyArrow writing to CSV does not have good coverage for all types for versions <11.0.0",
)
def test_pyarrow_csv_writer_custom_formats(tmp_path):
    """Test that the Python CSVFileWriter correctly applies custom date/timestamp formats.

    This tests the PyArrow fallback path to ensure custom formatting is preserved
    when the native CSV writer cannot be used.
    """
    from daft.io.writer import CSVFileWriter
    from daft.recordbatch.micropartition import MicroPartition

    # Create a MicroPartition with date and timestamp columns
    table = pa.table(
        {
            "id": [1, 2, 3],
            "date": pa.array(
                [datetime.date(2024, 1, 15), datetime.date(2024, 12, 31), None],
                type=pa.date32(),
            ),
            "timestamp": pa.array(
                [
                    datetime.datetime(2024, 1, 15, 10, 30, 45),
                    datetime.datetime(2024, 12, 31, 23, 59, 59),
                    None,
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    mp = MicroPartition.from_arrow(table)

    # Create a CSVFileWriter with custom formats
    writer = CSVFileWriter(
        root_dir=str(tmp_path),
        file_idx=0,
        date_format="%d/%m/%Y",
        timestamp_format="%Y-%m-%d %H:%M:%S",
    )

    # Write the data
    writer.write(mp)
    writer.close()

    # Read the output and verify formatting
    text = _read_first_file_text(str(tmp_path))

    # Check date formatting (DD/MM/YYYY)
    assert "15/01/2024" in text
    assert "31/12/2024" in text

    # Check timestamp formatting (YYYY-MM-DD HH:MM:SS)
    assert "2024-01-15 10:30:45" in text
    assert "2024-12-31 23:59:59" in text
