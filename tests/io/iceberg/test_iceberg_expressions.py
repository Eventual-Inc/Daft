from __future__ import annotations

from datetime import date, datetime
from typing import Any

import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotNull,
    Or,
)
from pyiceberg.expressions.literals import DateLiteral, StringLiteral, literal
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    IntegerType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

import daft
from daft.datatype import DataType
from daft.io.iceberg._expressions import convert_expression_to_iceberg
from daft.io.iceberg._visitors import IcebergPredicateVisitor

try:
    from pyiceberg.types import TimestampNanoType, TimestamptzNanoType

    _HAS_NANO_TYPES = True
except ImportError:
    _HAS_NANO_TYPES = False


def _make_schema() -> Schema:
    fields = [
        NestedField(1, "x", IntegerType()),
        NestedField(2, "name", StringType()),
        NestedField(3, "ts", TimestampType()),
        NestedField(4, "tstz", TimestamptzType()),
        NestedField(5, "d", DateType()),
    ]
    if _HAS_NANO_TYPES:
        fields += [
            NestedField(6, "ts_ns", TimestampNanoType()),
            NestedField(7, "tstz_ns", TimestamptzNanoType()),
        ]
    return Schema(*fields)


@pytest.fixture
def schema() -> Schema:
    return _make_schema()


@pytest.mark.parametrize(
    "expr, expected_type",
    [
        (daft.col("x") == daft.lit(5), EqualTo),
        (daft.col("x") != daft.lit(5), NotEqualTo),
        (daft.col("x") < daft.lit(5), LessThan),
        (daft.col("x") <= daft.lit(5), LessThanOrEqual),
        (daft.col("x") > daft.lit(5), GreaterThan),
        (daft.col("x") >= daft.lit(5), GreaterThanOrEqual),
    ],
    ids=["eq", "ne", "lt", "lte", "gt", "gte"],
)
def test_comparison_operators(expr: Any, expected_type: type) -> None:
    result = convert_expression_to_iceberg(expr)
    assert isinstance(result, expected_type)
    assert result.term.name == "x"


@pytest.mark.parametrize(
    "expr, expected_type",
    [
        (daft.lit(5) < daft.col("x"), GreaterThan),
        (daft.lit(5) > daft.col("x"), LessThan),
        (daft.lit(5) <= daft.col("x"), GreaterThanOrEqual),
        (daft.lit(5) >= daft.col("x"), LessThanOrEqual),
        (daft.lit(5) == daft.col("x"), EqualTo),
    ],
    ids=["lt_swapped", "gt_swapped", "lte_swapped", "gte_swapped", "eq_swapped"],
)
def test_swapped_comparisons(expr: Any, expected_type: type) -> None:
    result = convert_expression_to_iceberg(expr)
    assert isinstance(result, expected_type)
    assert result.term.name == "x"


# ---------------------------------------------------------------------------
def test_and() -> None:
    expr = (daft.col("x") > daft.lit(1)) & (daft.col("x") < daft.lit(10))
    result = convert_expression_to_iceberg(expr)
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThan)
    assert isinstance(result.right, LessThan)


def test_or() -> None:
    expr = (daft.col("x") < daft.lit(1)) | (daft.col("x") > daft.lit(10))
    result = convert_expression_to_iceberg(expr)
    assert isinstance(result, Or)
    assert isinstance(result.left, LessThan)
    assert isinstance(result.right, GreaterThan)


def test_not() -> None:
    result = convert_expression_to_iceberg(~daft.col("x").is_null())
    assert isinstance(result, Not)
    assert isinstance(result.child, IsNull)


def test_nested_logical() -> None:
    expr = (daft.col("x") == daft.lit(1)) & ((daft.col("name") == daft.lit("a")) | daft.col("x").is_null())
    result = convert_expression_to_iceberg(expr)
    assert isinstance(result, And)
    assert isinstance(result.right, Or)


def test_is_null() -> None:
    result = convert_expression_to_iceberg(daft.col("x").is_null())
    assert isinstance(result, IsNull)
    assert result.term.name == "x"


def test_not_null() -> None:
    result = convert_expression_to_iceberg(daft.col("x").not_null())
    assert isinstance(result, NotNull)
    assert result.term.name == "x"


def test_is_in() -> None:
    result = convert_expression_to_iceberg(daft.col("x").is_in([1, 2, 3]))
    assert isinstance(result, In)
    assert result.term.name == "x"


def test_between() -> None:
    result = convert_expression_to_iceberg(daft.col("x").between(1, 10))
    assert isinstance(result, And)
    assert isinstance(result.left, GreaterThanOrEqual)
    assert isinstance(result.right, LessThanOrEqual)
    assert result.left.term.name == "x"
    assert result.right.term.name == "x"


def test_alias_ignored() -> None:
    result = convert_expression_to_iceberg(daft.col("x").alias("y") == daft.lit(5))
    assert isinstance(result, EqualTo)
    assert result.term.name == "x"


def test_cast_ignored() -> None:
    result = convert_expression_to_iceberg(daft.col("x").cast(DataType.int64()) == daft.lit(5))
    assert isinstance(result, EqualTo)
    assert result.term.name == "x"


def test_function_raises() -> None:
    visitor = IcebergPredicateVisitor()
    with pytest.raises(ValueError, match="not support function"):
        visitor.visit_function("my_func", [])


def test_coalesce_raises() -> None:
    visitor = IcebergPredicateVisitor()
    with pytest.raises(ValueError, match="coalesce"):
        visitor.visit_coalesce([])


def test_two_refs_raises() -> None:
    visitor = IcebergPredicateVisitor()
    with pytest.raises(ValueError):
        visitor.visit_lhs_rhs(daft.col("x"), daft.col("y"))


def test_two_lits_raises() -> None:
    visitor = IcebergPredicateVisitor()
    with pytest.raises(ValueError):
        visitor.visit_lhs_rhs(daft.lit(1), daft.lit(2))


def test_visit_as_ref_non_ref_raises() -> None:
    visitor = IcebergPredicateVisitor()
    with pytest.raises(ValueError, match="column reference"):
        visitor.visit_as_ref(daft.lit(1))


def test_visit_as_lit_non_lit_raises() -> None:
    visitor = IcebergPredicateVisitor()
    with pytest.raises(ValueError, match="literal"):
        visitor.visit_as_lit(daft.col("x"))


def test_int_no_coerce(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("x") == daft.lit(5), schema)
    assert isinstance(result, EqualTo)
    assert result.term.name == "x"


def test_string_no_coerce(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("name") == daft.lit("hello"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "hello"


def test_date_on_date_col_no_coerce(schema: Schema) -> None:
    # DateLiteral on a DateType column should not be coerced
    result = convert_expression_to_iceberg(daft.col("d") == daft.lit(date(2023, 1, 1)), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, DateLiteral)


def test_date_str_to_timestamp(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit("2023-01-01"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01T00:00:00"


def test_date_str_to_timestamptz(schema: Schema) -> None:
    # Timezone-aware columns require an offset so pyiceberg can bind the literal.
    result = convert_expression_to_iceberg(daft.col("tstz") == daft.lit("2023-01-01"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01T00:00:00+00:00"


def test_non_date_str_unchanged(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit("not-a-date"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "not-a-date"


def test_full_iso_str_unchanged(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit("2023-01-01T12:30:00"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01T12:30:00"


def test_date_literal_coerced_to_timestamp(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit(date(2023, 1, 1)), schema)
    assert isinstance(result, EqualTo)
    # Coerced away from DateLiteral — value should be microseconds, not days
    assert not isinstance(result.literal, DateLiteral)
    expected = literal(datetime(2023, 1, 1, 0, 0, 0))
    assert result.literal.value == expected.value


def test_date_literal_coerced_to_timestamptz(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("tstz") == daft.lit(date(2023, 1, 1)), schema)
    assert isinstance(result, EqualTo)
    assert not isinstance(result.literal, DateLiteral)
    expected = literal(datetime(2023, 1, 1, 0, 0, 0))
    assert result.literal.value == expected.value


def test_coerce_missing_column_unchanged(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("nonexistent") == daft.lit("2023-01-01"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01"


def test_coerce_in_between(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts").between(date(2023, 1, 1), date(2023, 12, 31)), schema)
    assert isinstance(result, And)
    # Both bounds coerced from DateLiteral to TimestampLiteral
    assert not isinstance(result.left.literal, DateLiteral)
    assert not isinstance(result.right.literal, DateLiteral)


def test_coerce_in_is_in(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts").is_in([date(2023, 1, 1), date(2023, 6, 1)]), schema)
    assert isinstance(result, In)
    assert result.term.name == "ts"


@pytest.mark.skipif(not _HAS_NANO_TYPES, reason="pyiceberg version lacks TimestampNanoType")
def test_date_str_to_timestamp_nano(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts_ns") == daft.lit("2023-01-01"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01T00:00:00"


@pytest.mark.skipif(not _HAS_NANO_TYPES, reason="pyiceberg version lacks TimestamptzNanoType")
def test_date_str_to_timestamptz_nano(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("tstz_ns") == daft.lit("2023-01-01"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01T00:00:00+00:00"


@pytest.mark.skipif(not _HAS_NANO_TYPES, reason="pyiceberg version lacks nano types")
def test_date_literal_to_timestamp_nano(schema: Schema) -> None:
    result = convert_expression_to_iceberg(daft.col("ts_ns") == daft.lit(date(2023, 1, 1)), schema)
    assert isinstance(result, EqualTo)
    assert not isinstance(result.literal, DateLiteral)


def test_date_str_no_schema() -> None:
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit("2023-01-01"))
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-01-01"


def test_date_literal_no_schema() -> None:
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit(date(2023, 1, 1)))
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, DateLiteral)


def test_accepts_daft_expression() -> None:
    expr = daft.col("x") == daft.lit(5)
    result = convert_expression_to_iceberg(expr)
    assert isinstance(result, EqualTo)


def test_accepts_pyexpr() -> None:
    expr = daft.col("x") == daft.lit(5)
    result = convert_expression_to_iceberg(expr._expr)
    assert isinstance(result, EqualTo)
    assert result.term.name == "x"


def test_schema_forwarded_to_coerce(schema: Schema) -> None:
    # With schema, date string should be coerced; without it would stay as-is
    result = convert_expression_to_iceberg(daft.col("ts") == daft.lit("2023-06-15"), schema)
    assert isinstance(result, EqualTo)
    assert isinstance(result.literal, StringLiteral)
    assert result.literal.value == "2023-06-15T00:00:00"
