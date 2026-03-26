"""Tests for partition filter pushdown correctness with different PartitionTransforms.

Specifically verifies that:
- Identity transform: strict inequalities (< and >) are NOT relaxed to <= / >=
- Lossy transforms (Year, Month, Day, Hour, IcebergTruncate): strict inequalities ARE relaxed

Uses the DataSource API (daft.io.source) instead of the legacy ScanOperator,
and a PredicateVisitor to structurally verify the pushed-down expressions
rather than relying on fragile string matching.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from daft.expressions import Expression, col, lit
from daft.expressions.visitor import PredicateVisitor
from daft.io.partitioning import PartitionField, PartitionTransform
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import DataType
from daft.schema import Field, Schema, TimeUnit

# ---------------------------------------------------------------------------
# PredicateVisitor - extracts the top-level comparison operator name
# ---------------------------------------------------------------------------


class ComparisonExtractor(PredicateVisitor[dict[str, Any]]):
    """Extracts the top-level comparison info from a partition filter expression.

    Returns a dict like:
        {"op": "less_than", "left": {"op": "col", "name": "p_col"}, "right": {"op": "lit", "value": 5}}
    For compound predicates (and/or), it recurses into children.
    """

    def visit_col(self, name: str) -> dict[str, Any]:
        return {"op": "col", "name": name}

    def visit_lit(self, value: Any) -> dict[str, Any]:
        return {"op": "lit", "value": value}

    def visit_alias(self, expr: Expression, alias: str) -> dict[str, Any]:
        return self.visit(expr)

    def visit_cast(self, expr: Expression, dtype: DataType) -> dict[str, Any]:
        return self.visit(expr)

    def visit_function(self, name: str, args: list[Expression]) -> dict[str, Any]:
        return {"op": "function", "name": name, "args": [self.visit(a) for a in args]}

    def visit_and(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "and", "left": self.visit(left), "right": self.visit(right)}

    def visit_or(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "or", "left": self.visit(left), "right": self.visit(right)}

    def visit_not(self, expr: Expression) -> dict[str, Any]:
        return {"op": "not", "child": self.visit(expr)}

    def visit_equal(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "equal", "left": self.visit(left), "right": self.visit(right)}

    def visit_not_equal(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "not_equal", "left": self.visit(left), "right": self.visit(right)}

    def visit_less_than(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "less_than", "left": self.visit(left), "right": self.visit(right)}

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "less_than_or_equal", "left": self.visit(left), "right": self.visit(right)}

    def visit_greater_than(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "greater_than", "left": self.visit(left), "right": self.visit(right)}

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> dict[str, Any]:
        return {"op": "greater_than_or_equal", "left": self.visit(left), "right": self.visit(right)}

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> dict[str, Any]:
        return {
            "op": "between",
            "expr": self.visit(expr),
            "lower": self.visit(lower),
            "upper": self.visit(upper),
        }

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> dict[str, Any]:
        return {"op": "is_in", "expr": self.visit(expr), "items": [self.visit(i) for i in items]}

    def visit_is_null(self, expr: Expression) -> dict[str, Any]:
        return {"op": "is_null", "child": self.visit(expr)}

    def visit_not_null(self, expr: Expression) -> dict[str, Any]:
        return {"op": "not_null", "child": self.visit(expr)}

    def visit_list(self, items: list[Expression]) -> dict[str, Any]:
        return {"op": "list", "items": [self.visit(i) for i in items]}


EXTRACTOR = ComparisonExtractor()


def extract_comparison(expr: Expression) -> dict[str, Any]:
    """Extract the top-level comparison structure from an expression."""
    return EXTRACTOR.visit(expr)


# ---------------------------------------------------------------------------
# DataSource - captures pushdowns for inspection
# ---------------------------------------------------------------------------


class _CapturePushdownsDataSource(DataSource):
    """A minimal DataSource that captures the pushdowns it receives for inspection."""

    def __init__(self, source_schema: Schema, partition_fields: list[PartitionField]) -> None:
        self._schema = source_schema
        self._partition_fields = partition_fields
        self.captured_pushdowns: list[Pushdowns] = []

    @property
    def name(self) -> str:
        return "CapturePushdownsDataSource"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        return self._partition_fields

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        self.captured_pushdowns.append(pushdowns)
        # Yield nothing - we only care about the pushdowns, not actual data.
        return
        yield  # type: ignore[misc]  # makes this an async generator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_identity_partition_field(partition_col: str, source_col: str, dtype: DataType) -> PartitionField:
    """Build a PartitionField with Identity transform."""
    return PartitionField.create(
        field=Field.create(partition_col, dtype),
        source_field=Field.create(source_col, dtype),
        transform=PartitionTransform.identity(),
    )


def _make_year_partition_field(partition_col: str, source_col: str) -> PartitionField:
    """Build a PartitionField with Year transform."""
    return PartitionField.create(
        field=Field.create(partition_col, DataType.int32()),
        source_field=Field.create(source_col, DataType.timestamp(timeunit=TimeUnit.from_str("us"))),
        transform=PartitionTransform.year(),
    )


def _build_df_and_capture(
    source_col: str,
    dtype: DataType,
    partition_fields: list[PartitionField],
    filter_expr,
) -> Pushdowns:
    """Build a DataFrame via DataSource.read(), apply filter_expr, collect, and return captured pushdowns."""
    source_schema = Schema._from_field_name_and_types([(source_col, dtype)])
    source = _CapturePushdownsDataSource(source_schema, partition_fields)
    df = source.read().filter(filter_expr)
    df.collect()
    assert len(source.captured_pushdowns) == 1, "Expected exactly one call to get_tasks"
    return source.captured_pushdowns[0]


# ---------------------------------------------------------------------------
# Identity transform: strict inequalities must NOT be relaxed
# ---------------------------------------------------------------------------


def test_identity_lt_is_not_relaxed_to_lteq():
    """source_col < 5 with Identity transform -> partition_filter must be `p_col < 5`, not `p_col <= 5`."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") < lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "less_than", f"Identity Lt must NOT be relaxed, got op={result['op']}"
    assert result["left"]["name"] == "p_col", f"Expected partition column 'p_col', got: {result['left']}"


def test_identity_gt_is_not_relaxed_to_gteq():
    """source_col > 5 with Identity transform -> partition_filter must be `p_col > 5`, not `p_col >= 5`."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") > lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "greater_than", f"Identity Gt must NOT be relaxed, got op={result['op']}"
    assert result["left"]["name"] == "p_col", f"Expected partition column 'p_col', got: {result['left']}"


def test_identity_lteq_stays_lteq():
    """source_col <= 5 with Identity transform -> partition_filter must be `p_col <= 5` (already inclusive)."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") <= lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "less_than_or_equal", f"Expected less_than_or_equal, got op={result['op']}"


def test_identity_gteq_stays_gteq():
    """source_col >= 5 with Identity transform -> partition_filter must be `p_col >= 5` (already inclusive)."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") >= lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "greater_than_or_equal", f"Expected greater_than_or_equal, got op={result['op']}"


# ---------------------------------------------------------------------------
# Non-identity (lossy) transforms: strict inequalities MUST be relaxed
# ---------------------------------------------------------------------------


def test_year_lt_is_relaxed_to_lteq():
    """ts_col < value with Year transform -> partition_filter must be `p_year <= value` (relaxed)."""
    pfield = _make_year_partition_field("p_year", "ts_col")
    pushdowns = _build_df_and_capture(
        source_col="ts_col",
        dtype=DataType.timestamp(timeunit=TimeUnit.from_str("us")),
        partition_fields=[pfield],
        filter_expr=col("ts_col") < lit("2024-01-01").cast(DataType.timestamp(timeunit=TimeUnit.from_str("us"))),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "less_than_or_equal", f"Year Lt must be relaxed to LtEq, got op={result['op']}"


def test_year_gt_is_relaxed_to_gteq():
    """ts_col > value with Year transform -> partition_filter must be `p_year >= value` (relaxed)."""
    pfield = _make_year_partition_field("p_year", "ts_col")
    pushdowns = _build_df_and_capture(
        source_col="ts_col",
        dtype=DataType.timestamp(timeunit=TimeUnit.from_str("us")),
        partition_fields=[pfield],
        filter_expr=col("ts_col") > lit("2024-01-01").cast(DataType.timestamp(timeunit=TimeUnit.from_str("us"))),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "greater_than_or_equal", f"Year Gt must be relaxed to GtEq, got op={result['op']}"
