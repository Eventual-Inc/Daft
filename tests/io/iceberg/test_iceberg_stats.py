"""Tests for Iceberg scan task statistics injection."""

from __future__ import annotations

from unittest.mock import Mock

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.conversions import to_bytes
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

from daft.io.iceberg.iceberg_scan import _build_iceberg_data_source_task_stats


def _make_data_file(lower_bounds=None, upper_bounds=None):
    """Create a mock DataFile with given bounds."""
    file = Mock()
    file.lower_bounds = lower_bounds
    file.upper_bounds = upper_bounds
    return file


def _make_top_level_fields(schema: IcebergSchema) -> list[tuple[int, str, object, bool, pa.DataType]]:
    """Build the precomputed top-level field list matching IcebergDataSource.__init__."""
    return [
        (
            field.field_id,
            field.name,
            field.field_type,
            isinstance(field.field_type, PrimitiveType),
            schema_to_pyarrow(field.field_type),
        )
        for field in schema.fields
    ]


def _make_task_schema(iceberg_schema: IcebergSchema):
    """Build a minimal Daft Schema for assertions."""
    from daft.logical.schema import Schema

    pyarrow_schema = pa.schema(
        [
            pa.field(
                field.name,
                schema_to_pyarrow(field.field_type),
                nullable=not field.required,
            )
            for field in iceberg_schema.fields
        ]
    )
    return Schema.from_pyarrow_schema(pyarrow_schema)


class TestBuildIcebergDataSourceTaskStats:
    """Test suite for _build_iceberg_data_source_task_stats."""

    def test_none_stats(self):
        """Returns None when both bounds are None."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        file = _make_data_file(lower_bounds=None, upper_bounds=None)
        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is None

    def test_empty_stats(self):
        """Returns None when both bounds are empty dicts."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        file = _make_data_file(lower_bounds={}, upper_bounds={})
        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is None

    def test_long_type(self):
        """Long type min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        lower = {1: to_bytes(LongType(), 10)}
        upper = {1: to_bytes(LongType(), 20)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.num_rows == 2
        assert arrow_table.column("id")[0].as_py() == 10
        assert arrow_table.column("id")[1].as_py() == 20

    def test_integer_type(self):
        """Integer type min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="val", field_type=IntegerType(), required=True),
        )
        lower = {1: to_bytes(IntegerType(), 5)}
        upper = {1: to_bytes(IntegerType(), 100)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("val")[0].as_py() == 5
        assert arrow_table.column("val")[1].as_py() == 100

    def test_string_type(self):
        """String type min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="name", field_type=StringType(), required=True),
        )
        lower = {1: to_bytes(StringType(), "aaa")}
        upper = {1: to_bytes(StringType(), "zzz")}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("name")[0].as_py() == "aaa"
        assert arrow_table.column("name")[1].as_py() == "zzz"

    def test_truncated_string_type(self):
        """Truncated string bounds (from Iceberg truncate(N) metrics) decode without error.

        When Iceberg's MetricsMode is truncate(N), bounds are shorter than the
        actual column values. The decoder must handle these partial UTF-8 sequences
        gracefully.
        """
        schema = IcebergSchema(
            NestedField(field_id=1, name="name", field_type=StringType(), required=True),
        )
        # Simulate a truncated bound: actual value might be "abcdefghij" but
        # the bound is truncated to "abcde". The raw bytes are still valid UTF-8.
        truncated_lower = to_bytes(StringType(), "abcde")  # truncated from "abcdefghij"
        truncated_upper = to_bytes(StringType(), "uvwxy")  # truncated from "uvwxyz1234"
        lower = {1: truncated_lower}
        upper = {1: truncated_upper}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        # Truncated bounds decode to the truncated prefix, not the original value
        assert arrow_table.column("name")[0].as_py() == "abcde"
        assert arrow_table.column("name")[1].as_py() == "uvwxy"

    def test_date_type(self):
        """Date type min/max decoded correctly."""
        import datetime

        schema = IcebergSchema(
            NestedField(field_id=1, name="dt", field_type=DateType(), required=True),
        )
        # 2020-01-01 = day 18262, 2020-12-31 = day 18627
        lower = {1: to_bytes(DateType(), 18262)}
        upper = {1: to_bytes(DateType(), 18627)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("dt")[0].as_py() == datetime.date(2020, 1, 1)
        assert arrow_table.column("dt")[1].as_py() == datetime.date(2020, 12, 31)

    def test_timestamp_type(self):
        """TimestampType (no timezone) min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="ts", field_type=TimestampType(), required=True),
        )
        # microseconds since epoch
        min_us = 1577836800000000  # 2020-01-01 00:00:00 UTC
        max_us = 1609459200000000  # 2021-01-01 00:00:00 UTC
        lower = {1: to_bytes(TimestampType(), min_us)}
        upper = {1: to_bytes(TimestampType(), max_us)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        # Compare underlying microsecond values to avoid datetime-timezone lint
        assert arrow_table.column("ts")[0].cast(pa.int64()).as_py() == min_us
        assert arrow_table.column("ts")[1].cast(pa.int64()).as_py() == max_us

    def test_timestamptz_type(self):
        """TimestamptzType (with timezone) min/max decoded correctly."""
        import datetime

        schema = IcebergSchema(
            NestedField(field_id=1, name="ts_tz", field_type=TimestamptzType(), required=True),
        )
        min_us = 1577836800000000
        max_us = 1609459200000000
        lower = {1: to_bytes(TimestamptzType(), min_us)}
        upper = {1: to_bytes(TimestamptzType(), max_us)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        # Arrow returns datetime objects for timestamps with timezone
        assert arrow_table.column("ts_tz")[0].as_py() == datetime.datetime(
            2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )
        assert arrow_table.column("ts_tz")[1].as_py() == datetime.datetime(
            2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
        )

    def test_decimal_type(self):
        """Decimal type min/max decoded with correct scale/precision."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="amount", field_type=DecimalType(10, 2), required=True),
        )
        from decimal import Decimal

        lower = {1: to_bytes(DecimalType(10, 2), Decimal("10.50"))}
        upper = {1: to_bytes(DecimalType(10, 2), Decimal("999.99"))}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("amount")[0].as_py() == Decimal("10.50")
        assert arrow_table.column("amount")[1].as_py() == Decimal("999.99")

    def test_binary_type(self):
        """Binary type min/max decoded correctly (logical value consistency)."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="data", field_type=BinaryType(), required=True),
        )
        min_bytes = b"\x00\x01\x02"
        max_bytes = b"\xff\xfe\xfd"
        lower = {1: to_bytes(BinaryType(), min_bytes)}
        upper = {1: to_bytes(BinaryType(), max_bytes)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("data")[0].as_py() == min_bytes
        assert arrow_table.column("data")[1].as_py() == max_bytes

    def test_uuid_type(self):
        """UUID type min/max decoded correctly (logical value consistency)."""
        import uuid

        schema = IcebergSchema(
            NestedField(field_id=1, name="uid", field_type=UUIDType(), required=True),
        )

        min_uuid = uuid.UUID("00000000-0000-0000-0000-000000000000")
        max_uuid = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        lower = {1: to_bytes(UUIDType(), min_uuid)}
        upper = {1: to_bytes(UUIDType(), max_uuid)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("uid")[0].as_py() == min_uuid
        assert arrow_table.column("uid")[1].as_py() == max_uuid
        assert result.schema() == _make_task_schema(schema)

    def test_missing_upper_bound(self):
        """Returns None when upper bound is missing for all valid fields."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        lower = {1: to_bytes(LongType(), 10)}
        upper = {}  # missing upper
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        # The function now checks field_id in lower AND field_id in upper, so
        # a field with only one side produces no decoded bounds, and the
        # full-schema batch is non-empty but has no decoded values → None.
        assert result is None

    def test_unknown_field_id(self):
        """Returns None when no field ids match the schema."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        # field_id=999 doesn't exist in schema
        lower = {999: to_bytes(LongType(), 10)}
        upper = {999: to_bytes(LongType(), 20)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is None

    def test_nested_field_skipped(self):
        """Skips nested fields (struct), only processes top-level primitives."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="address",
                field_type=StructType(
                    NestedField(field_id=3, name="city", field_type=StringType(), required=True),
                ),
                required=False,
            ),
        )
        # Only provide stats for nested leaf (should be skipped — is_primitive=False)
        lower = {3: to_bytes(StringType(), "a")}
        upper = {3: to_bytes(StringType(), "z")}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is None

    def test_multiple_columns(self):
        """Correctly handles multiple columns with stats."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=True),
        )
        lower = {1: to_bytes(LongType(), 1), 2: to_bytes(StringType(), "alice")}
        upper = {1: to_bytes(LongType(), 100), 2: to_bytes(StringType(), "zack")}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.num_rows == 2
        assert arrow_table.column("id")[0].as_py() == 1
        assert arrow_table.column("id")[1].as_py() == 100
        assert arrow_table.column("name")[0].as_py() == "alice"
        assert arrow_table.column("name")[1].as_py() == "zack"
        assert result.schema().column_names() == ["id", "name"]

    def test_float_type(self):
        """Float type min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="score", field_type=FloatType(), required=True),
        )
        lower = {1: to_bytes(FloatType(), 1.5)}
        upper = {1: to_bytes(FloatType(), 9.9)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert abs(arrow_table.column("score")[0].as_py() - 1.5) < 0.01
        assert abs(arrow_table.column("score")[1].as_py() - 9.9) < 0.01

    def test_double_type(self):
        """Double type min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="price", field_type=DoubleType(), required=True),
        )
        lower = {1: to_bytes(DoubleType(), 0.01)}
        upper = {1: to_bytes(DoubleType(), 99999.99)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert abs(arrow_table.column("price")[0].as_py() - 0.01) < 0.001
        assert abs(arrow_table.column("price")[1].as_py() - 99999.99) < 0.01

    def test_mixed_valid_and_invalid_fields(self):
        """When some fields fail to decode, others still succeed."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="bad", field_type=IntegerType(), required=True),
        )
        lower = {1: to_bytes(LongType(), 1), 2: b"\xff\xff"}  # invalid bytes for IntegerType
        upper = {1: to_bytes(LongType(), 100), 2: b"\xff\xff"}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        # Both columns present with full schema
        assert set(arrow_table.column_names) == {"id", "bad"}
        # id was valid
        assert arrow_table.column("id")[0].as_py() == 1
        assert arrow_table.column("id")[1].as_py() == 100
        # bad was invalid → typed null
        assert arrow_table.column("bad")[0].as_py() is None
        assert arrow_table.column("bad")[1].as_py() is None
        # Schema order preserved (id before bad as per schema definition)
        assert result.schema().column_names() == ["id", "bad"]

    def test_reversed_field_order(self):
        """Stats output follows schema order, not bounds dict iteration order."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="a", field_type=LongType(), required=True),
            NestedField(field_id=2, name="b", field_type=StringType(), required=True),
            NestedField(field_id=3, name="c", field_type=FloatType(), required=True),
        )
        # Provide bounds in reverse order of schema
        lower = {
            3: to_bytes(FloatType(), 3.0),
            2: to_bytes(StringType(), "b_val"),
            1: to_bytes(LongType(), 1),
        }
        upper = {
            3: to_bytes(FloatType(), 30.0),
            2: to_bytes(StringType(), "b_upper"),
            1: to_bytes(LongType(), 10),
        }
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        # Column order must be [a, b, c] as per schema, not [c, b, a]
        assert result.schema().column_names() == ["a", "b", "c"]

    def test_partial_bounds_full_schema(self):
        """Return full-schema batch with typed nulls for columns missing bounds."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="x", field_type=LongType(), required=True),
            NestedField(field_id=2, name="y", field_type=DoubleType(), required=True),
        )
        lower = {1: to_bytes(LongType(), 100)}
        upper = {1: to_bytes(LongType(), 200)}
        # y has no bounds at all
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        assert result.schema().column_names() == ["x", "y"]
        assert result.to_arrow_table().column("y")[0].as_py() is None
        assert result.to_arrow_table().column("y")[1].as_py() is None

    def test_complex_top_level_field_is_typed_null(self):
        """Struct/map/list top-level fields receive typed nulls, not skipped."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(
                field_id=2,
                name="meta",
                field_type=StructType(
                    NestedField(field_id=3, name="key", field_type=StringType()),
                ),
                required=False,
            ),
        )
        lower = {1: to_bytes(LongType(), 1)}
        upper = {1: to_bytes(LongType(), 10)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        assert result.schema().column_names() == ["id", "meta"]
        # meta is complex → typed null
        assert result.to_arrow_table().column("meta")[0].as_py() is None
        assert result.to_arrow_table().column("meta")[1].as_py() is None


class TestBuildIcebergDataSourceTaskStatsSchemaContract:
    """Tests that verify the schema contract: output matches task schema exactly."""

    def test_stats_schema_equals_task_schema(self):
        """Stats RecordBatch schema columns, names, and order match task_schema."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="name", field_type=StringType(), required=True),
            NestedField(field_id=3, name="score", field_type=FloatType(), required=True),
        )
        task_schema = _make_task_schema(schema)
        lower = {
            1: to_bytes(LongType(), 1),
            2: to_bytes(StringType(), "a"),
            3: to_bytes(FloatType(), 1.0),
        }
        upper = {
            1: to_bytes(LongType(), 100),
            2: to_bytes(StringType(), "z"),
            3: to_bytes(FloatType(), 10.0),
        }
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            task_schema,
        )
        assert result is not None
        assert result.schema() == task_schema

    def test_stats_schema_matches_column_order(self):
        """Output column order matches schema, even with shuffled bounds keys."""
        schema = IcebergSchema(
            NestedField(field_id=3, name="c", field_type=LongType(), required=True),
            NestedField(field_id=1, name="a", field_type=LongType(), required=True),
            NestedField(field_id=2, name="b", field_type=StringType(), required=True),
        )
        task_schema = _make_task_schema(schema)
        # bounds keys are in natural id order, which is NOT schema order
        lower = {
            1: to_bytes(LongType(), 1),
            2: to_bytes(StringType(), "b"),
            3: to_bytes(LongType(), 3),
        }
        upper = {
            1: to_bytes(LongType(), 10),
            2: to_bytes(StringType(), "B"),
            3: to_bytes(LongType(), 30),
        }
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            task_schema,
        )
        assert result is not None
        # Must be [c, a, b] as per schema, not [1, 2, 3]
        assert result.schema().column_names() == ["c", "a", "b"]


class TestBuildIcebergDataSourceTaskStatsEdgeCases:
    """Edge-case coverage required by the test plan but not covered above."""

    def test_boolean_type(self):
        """Boolean type min/max decoded correctly."""
        from pyiceberg.types import BooleanType

        schema = IcebergSchema(
            NestedField(field_id=1, name="flag", field_type=BooleanType(), required=True),
        )
        lower = {1: to_bytes(BooleanType(), False)}
        upper = {1: to_bytes(BooleanType(), True)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("flag")[0].as_py() is False
        assert arrow_table.column("flag")[1].as_py() is True

    def test_time_type(self):
        """TimeType min/max decoded correctly."""
        import datetime

        from pyiceberg.types import TimeType

        schema = IcebergSchema(
            NestedField(field_id=1, name="t", field_type=TimeType(), required=True),
        )
        # microseconds since midnight → datetime.time objects via Arrow
        lower = {1: to_bytes(TimeType(), 0)}
        upper = {1: to_bytes(TimeType(), 86_400_000_000 - 1)}  # 23:59:59.999999
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.column("t")[0].as_py() == datetime.time(0, 0)
        assert arrow_table.column("t")[1].as_py() == datetime.time(23, 59, 59, 999999)

    def test_missing_lower_bound(self):
        """Returns None when lower bound is missing for all valid fields."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        lower = {}  # missing lower
        upper = {1: to_bytes(LongType(), 100)}  # only upper
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is None

    def test_type_promotion_int_to_long(self):
        """Old file bounds encoded as int, but current schema has long — decode fails."""
        # This simulates an Iceberg type promotion: the file was written with
        # IntegerType bounds but the schema now has LongType. The bounds bytes
        # are 4 bytes (int) but from_bytes expects 8 bytes (long) → ValueError.
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        # Encode as IntegerType (4 bytes), but schema says LongType (8 bytes)
        import struct as _struct

        lower = {1: _struct.pack("<i", 42)}
        upper = {1: _struct.pack("<i", 999)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        # Decode fails → returned batch is all typed nulls → None
        assert result is None

    def test_type_promotion_float_to_double(self):
        """Old file bounds encoded as float, current schema has double — decode fails."""
        # 4-byte float bytes cannot be decoded by from_bytes(DoubleType, ...)
        # (struct.error). The conservative contract: decode failure → None.
        schema = IcebergSchema(
            NestedField(field_id=1, name="val", field_type=DoubleType(), required=True),
        )
        lower = {1: to_bytes(FloatType(), 1.5)}
        upper = {1: to_bytes(FloatType(), 9.9)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        # Decode fails → no usable bounds → None (never crashes)
        assert result is None

    def test_schema_evolution_added_column(self):
        """New column added by schema evolution — old file has no bounds for it."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="new_col", field_type=StringType(), required=False),
        )
        # Only field_id=1 has bounds; new_col has none
        lower = {1: to_bytes(LongType(), 1)}
        upper = {1: to_bytes(LongType(), 100)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_data_source_task_stats(
            file,
            _make_top_level_fields(schema),
            _make_task_schema(schema),
        )
        assert result is not None
        assert result.schema().column_names() == ["id", "new_col"]
        arrow_table = result.to_arrow_table()
        # id has values
        assert arrow_table.column("id")[0].as_py() == 1
        assert arrow_table.column("id")[1].as_py() == 100
        # new_col is typed null
        assert arrow_table.column("new_col")[0].as_py() is None
        assert arrow_table.column("new_col")[1].as_py() is None
