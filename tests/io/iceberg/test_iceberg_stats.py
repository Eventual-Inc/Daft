"""Tests for Iceberg scan task statistics injection."""

from __future__ import annotations

from unittest.mock import Mock

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.conversions import to_bytes
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.schema import visit
from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
    UUIDType,
)

from daft.daft import PyField
from daft.io.iceberg.iceberg_scan import _build_iceberg_scan_task_stats
from daft.io.iceberg.schema_field_id_mapping_visitor import SchemaFieldIdMappingVisitor


def _make_data_file(lower_bounds=None, upper_bounds=None):
    """Create a mock DataFile with given bounds."""
    file = Mock()
    file.lower_bounds = lower_bounds
    file.upper_bounds = upper_bounds
    return file


def _build_field_id_mapping(schema: IcebergSchema) -> dict[int, PyField]:
    return visit(schema, SchemaFieldIdMappingVisitor())


def _top_level_primitive_ids(schema: IcebergSchema) -> set[int]:
    from pyiceberg.types import PrimitiveType

    return {field.field_id for field in schema.fields if isinstance(field.field_type, PrimitiveType)}


class TestBuildIcebergScanTaskStats:
    """Test suite for _build_iceberg_scan_task_stats."""

    def test_none_stats(self):
        """Returns None when both bounds are None."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        file = _make_data_file(lower_bounds=None, upper_bounds=None)
        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
        )
        assert result is None

    def test_empty_stats(self):
        """Returns None when both bounds are empty dicts."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        file = _make_data_file(lower_bounds={}, upper_bounds={})
        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        # Arrow returns uuid.UUID objects for UUID type
        assert arrow_table.column("uid")[0].as_py() == min_uuid
        assert arrow_table.column("uid")[1].as_py() == max_uuid
        # Verify dtype is UUID-compatible (extension type)
        assert pa.types.is_fixed_size_binary(arrow_table.schema.field("uid").type) or str(
            arrow_table.schema.field("uid").type
        ).startswith("extension")

    def test_missing_upper_bound(self):
        """Skips column when upper bound is missing."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        lower = {1: to_bytes(LongType(), 10)}
        upper = {}  # missing upper
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
        )
        assert result is None

    def test_unknown_field_id(self):
        """Skips field_id not in schema (schema evolution)."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        )
        # field_id=999 doesn't exist in schema
        lower = {999: to_bytes(LongType(), 10)}
        upper = {999: to_bytes(LongType(), 20)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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
        # Only provide stats for nested field (should be skipped)
        lower = {3: to_bytes(StringType(), "a")}
        upper = {3: to_bytes(StringType(), "z")}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        assert arrow_table.num_rows == 2
        assert arrow_table.column("id")[0].as_py() == 1
        assert arrow_table.column("id")[1].as_py() == 100
        assert arrow_table.column("name")[0].as_py() == "alice"
        assert arrow_table.column("name")[1].as_py() == "zack"

    def test_float_type(self):
        """Float type min/max decoded correctly."""
        schema = IcebergSchema(
            NestedField(field_id=1, name="score", field_type=FloatType(), required=True),
        )
        lower = {1: to_bytes(FloatType(), 1.5)}
        upper = {1: to_bytes(FloatType(), 9.9)}
        file = _make_data_file(lower_bounds=lower, upper_bounds=upper)

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
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

        result = _build_iceberg_scan_task_stats(
            file, schema, _build_field_id_mapping(schema), _top_level_primitive_ids(schema)
        )
        assert result is not None
        arrow_table = result.to_arrow_table()
        # Only the valid column should be present
        assert "id" in arrow_table.column_names
        assert arrow_table.column("id")[0].as_py() == 1
