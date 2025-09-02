"""Tests for Duration type Parquet roundtrip fix."""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

import daft
from daft import DataType, TimeUnit

PYARROW_GE_8_0_0 = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) >= (8, 0, 0)


class TestDurationRoundtripFix:
    """Test Duration type Parquet roundtrip functionality."""

    @pytest.mark.skipif(
        not PYARROW_GE_8_0_0,
        reason="PyArrow writing to Parquet does not have good coverage for all types for versions <8.0.0",
    )
    @pytest.mark.parametrize(
        ["data", "pa_type", "expected_dtype"],
        [
            # Test Duration types with different time units
            (
                [datetime.timedelta(days=1), datetime.timedelta(days=2), None],
                pa.duration("ms"),
                DataType.duration(TimeUnit.ms()),
            ),
            (
                [datetime.timedelta(seconds=30), datetime.timedelta(seconds=60), None],
                pa.duration("s"),
                DataType.duration(TimeUnit.s()),
            ),
            (
                [datetime.timedelta(microseconds=1000), datetime.timedelta(microseconds=2000), None],
                pa.duration("us"),
                DataType.duration(TimeUnit.us()),
            ),
            (
                [datetime.timedelta(microseconds=1), datetime.timedelta(microseconds=2), None],
                pa.duration("ns"),
                DataType.duration(TimeUnit.ns()),
            ),
        ],
    )
    def test_duration_roundtrip_fixed(self, tmp_path, data, pa_type, expected_dtype):
        """Test that Duration types now properly roundtrip through Parquet."""
        # Create DataFrame with Duration data
        before = daft.from_arrow(pa.table({"duration_col": pa.array(data, type=pa_type)}))
        before = before.concat(before)

        # Write to Parquet
        parquet_path = str(tmp_path / "duration_test.parquet")
        before.write_parquet(parquet_path)

        # Read back from Parquet
        after = daft.read_parquet(parquet_path)

        # Verify schema preservation
        assert before.schema()["duration_col"].dtype == expected_dtype
        assert after.schema()["duration_col"].dtype == expected_dtype

        # Verify data integrity
        assert before.to_arrow() == after.to_arrow()

    def test_duration_mixed_with_other_types(self, tmp_path):
        """Test Duration types mixed with other temporal types."""
        data = {
            "duration_ms": pa.array(
                [datetime.timedelta(days=1), datetime.timedelta(hours=2), None], type=pa.duration("ms")
            ),
            "timestamp_ms": pa.array(
                [datetime.datetime(2023, 1, 1), datetime.datetime(2023, 1, 2), None], type=pa.timestamp("ms")
            ),
            "date": pa.array([datetime.date(2023, 1, 1), datetime.date(2023, 1, 2), None], type=pa.date32()),
        }

        before = daft.from_arrow(pa.table(data))
        before = before.concat(before)

        # Write and read back
        parquet_path = str(tmp_path / "mixed_temporal_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        # Verify all types are preserved
        assert after.schema()["duration_ms"].dtype == DataType.duration(TimeUnit.ms())
        assert after.schema()["timestamp_ms"].dtype == DataType.timestamp(TimeUnit.ms())
        assert after.schema()["date"].dtype == DataType.date()

        # Verify data integrity
        assert before.to_arrow() == after.to_arrow()

    def test_duration_edge_cases(self, tmp_path):
        """Test Duration type edge cases."""
        # Test with zero durations
        zero_data = [datetime.timedelta(0), datetime.timedelta(days=0, seconds=0), None]

        before = daft.from_arrow(pa.table({"zero_durations": pa.array(zero_data, type=pa.duration("ms"))}))

        parquet_path = str(tmp_path / "zero_duration_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        assert after.schema()["zero_durations"].dtype == DataType.duration(TimeUnit.ms())
        assert before.to_arrow() == after.to_arrow()

    def test_duration_large_values(self, tmp_path):
        """Test Duration type with large values."""
        # Test with large duration values
        large_data = [
            datetime.timedelta(days=365 * 10),  # 10 years
            datetime.timedelta(days=365 * 100),  # 100 years
            None,
        ]

        before = daft.from_arrow(pa.table({"large_durations": pa.array(large_data, type=pa.duration("ms"))}))

        parquet_path = str(tmp_path / "large_duration_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        assert after.schema()["large_durations"].dtype == DataType.duration(TimeUnit.ms())
        assert before.to_arrow() == after.to_arrow()

    def test_duration_negative_values(self, tmp_path):
        """Test Duration type with negative values."""
        # Test with negative duration values
        negative_data = [datetime.timedelta(days=-1), datetime.timedelta(hours=-2), None]

        before = daft.from_arrow(pa.table({"negative_durations": pa.array(negative_data, type=pa.duration("ms"))}))

        parquet_path = str(tmp_path / "negative_duration_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        assert after.schema()["negative_durations"].dtype == DataType.duration(TimeUnit.ms())
        assert before.to_arrow() == after.to_arrow()

    @pytest.mark.parametrize("time_unit", ["s", "ms", "us", "ns"])
    def test_duration_all_time_units(self, tmp_path, time_unit):
        """Test Duration roundtrip for all supported time units."""
        # Create appropriate timedelta values based on time unit
        if time_unit == "s":
            data = [datetime.timedelta(seconds=1), datetime.timedelta(seconds=2), None]
        elif time_unit == "ms":
            data = [datetime.timedelta(milliseconds=1), datetime.timedelta(milliseconds=2), None]
        elif time_unit == "us":
            data = [datetime.timedelta(microseconds=1), datetime.timedelta(microseconds=2), None]
        else:  # ns
            # Note: Python timedelta doesn't support nanoseconds directly
            # so we use microseconds and let PyArrow handle the conversion
            data = [datetime.timedelta(microseconds=1), datetime.timedelta(microseconds=2), None]

        before = daft.from_arrow(pa.table({f"duration_{time_unit}": pa.array(data, type=pa.duration(time_unit))}))

        parquet_path = str(tmp_path / f"duration_{time_unit}_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        # Map time unit string to TimeUnit enum
        time_unit_mapping = {
            "s": TimeUnit.s(),
            "ms": TimeUnit.ms(),
            "us": TimeUnit.us(),
            "ns": TimeUnit.ns(),
        }

        expected_dtype = DataType.duration(time_unit_mapping[time_unit])
        assert after.schema()[f"duration_{time_unit}"].dtype == expected_dtype
        assert before.to_arrow() == after.to_arrow()

    def test_duration_in_complex_schema(self, tmp_path):
        """Test Duration type within complex nested schemas."""
        # Create a complex schema with Duration in a struct
        struct_data = pa.array(
            [
                {"duration_field": datetime.timedelta(days=1), "int_field": 42},
                {"duration_field": datetime.timedelta(hours=2), "int_field": 84},
                None,
            ],
            type=pa.struct([("duration_field", pa.duration("ms")), ("int_field", pa.int64())]),
        )

        before = daft.from_arrow(pa.table({"complex_struct": struct_data}))

        parquet_path = str(tmp_path / "complex_duration_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        # Verify the nested Duration type is preserved
        expected_struct_dtype = DataType.struct(
            {"duration_field": DataType.duration(TimeUnit.ms()), "int_field": DataType.int64()}
        )
        assert after.schema()["complex_struct"].dtype == expected_struct_dtype
        assert before.to_arrow() == after.to_arrow()

    def test_regression_duration_not_int64(self, tmp_path):
        """Regression test: ensure Duration is not converted to plain Int64."""
        duration_data = [datetime.timedelta(days=1), datetime.timedelta(days=2), None]

        before = daft.from_arrow(pa.table({"duration_col": pa.array(duration_data, type=pa.duration("ms"))}))

        parquet_path = str(tmp_path / "regression_test.parquet")
        before.write_parquet(parquet_path)
        after = daft.read_parquet(parquet_path)

        # This should NOT be Int64 anymore
        assert after.schema()["duration_col"].dtype != DataType.int64()
        # This SHOULD be Duration
        assert after.schema()["duration_col"].dtype == DataType.duration(TimeUnit.ms())

        # Data should be identical
        assert before.to_arrow() == after.to_arrow()
