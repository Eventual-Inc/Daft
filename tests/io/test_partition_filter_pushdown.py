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

import pyarrow as pa
import pytest

from daft.expressions import Expression, col, lit
from daft.expressions.visitor import PredicateVisitor
from daft.functions import partition_iceberg_bucket
from daft.io.partitioning import PartitionField, PartitionTransform
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import DataType
from daft.recordbatch import RecordBatch
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

    def visit_try_cast(self, expr: Expression, dtype: DataType) -> dict[str, Any]:
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

    def visit_coalesce(self, args: list[Expression]) -> dict[str, Any]:
        return {"op": "coalesce", "args": [self.visit(arg) for arg in args]}


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


class _ProjectedDataSourceTask(DataSourceTask):
    def __init__(self, table: pa.Table, filters: Expression | None) -> None:
        self._table = table
        self._filters = filters

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        batch = RecordBatch.from_arrow_table(self._table)
        if self._filters is not None:
            batch = batch.filter([self._filters])
        yield batch


class _ProjectingDataSource(_CapturePushdownsDataSource):
    """Reads data columns directly, evaluating partition metadata separately."""

    def __init__(
        self, table: pa.Table, partition_fields: list[PartitionField], partition_expr: Expression | None
    ) -> None:
        super().__init__(Schema.from_pyarrow_schema(table.schema), partition_fields)
        self._table = table
        self._partition_values = (
            RecordBatch.from_arrow_table(table).eval_expression_list([partition_expr])
            if partition_expr is not None
            else None
        )

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        self.captured_pushdowns.append(pushdowns)
        # Do not sanitize pushdowns: hidden metadata names must never reach
        # this data projection through the optimizer/Python scan wrapper.
        table = self._table.select(pushdowns.columns) if pushdowns.columns is not None else self._table
        for i in range(table.num_rows):
            if pushdowns.partition_filters is not None:
                assert self._partition_values is not None
                metadata = self._partition_values.slice(i, i + 1)
                if len(metadata.filter([pushdowns.partition_filters])) == 0:
                    continue
            yield _ProjectedDataSourceTask(table.slice(i, 1), pushdowns.filters)


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
    columns: list[tuple[str, DataType]],
    partition_fields: list[PartitionField],
    filter_expr,
) -> Pushdowns:
    """Build a DataFrame via DataSource.read(), apply filter_expr, collect, and return captured pushdowns."""
    source_schema = Schema._from_field_name_and_types(columns)
    source = _CapturePushdownsDataSource(source_schema, partition_fields)
    df = source.read().filter(filter_expr)
    df.collect()
    assert len(source.captured_pushdowns) == 1, "Expected exactly one call to get_tasks"
    return source.captured_pushdowns[0]


@pytest.mark.parametrize("partition_kind", ["hidden_bucket", "visible_identity", "unpartitioned"])
@pytest.mark.parametrize("count_rows", [False, True], ids=["select", "count"])
def test_data_source_projection_keeps_only_data_filter_columns(partition_kind, count_rows):
    table = pa.table({"a": [0, 1, 2, 3], "b": [0, 1, 2, 1], "c": [4, 5, 6, 7], "d": [8, 9, 10, 11]})
    if partition_kind == "hidden_bucket":
        partition_col = "b_bucket16"
        partition_fields = [
            PartitionField.create(
                field=Field.create(partition_col, DataType.int32()),
                source_field=Field.create("b", DataType.int64()),
                transform=PartitionTransform.iceberg_bucket(16),
            )
        ]
        partition_expr = partition_iceberg_bucket(col("b"), 16).alias(partition_col)
    elif partition_kind == "visible_identity":
        partition_col = "b"
        partition_fields = [_make_identity_partition_field("b", "b", DataType.int64())]
        partition_expr = col("b")
    else:
        partition_fields = []
        partition_expr = None

    source = _ProjectingDataSource(table, partition_fields, partition_expr)
    query = source.read().filter(col("b") == 1).select("a")
    if count_rows:
        assert query.count_rows() == 2
    else:
        result = query.to_pydict()
        assert list(result) == ["a"]
        assert sorted(result["a"]) == [1, 3]

    assert len(source.captured_pushdowns) == 1
    pushdowns = source.captured_pushdowns[0]
    assert pushdowns.columns is not None
    assert "b" in pushdowns.columns
    assert set(pushdowns.columns) <= {"a", "b"}
    if not count_rows:
        assert set(pushdowns.columns) == {"a", "b"}
    if partition_kind == "unpartitioned":
        assert pushdowns.partition_filters is None
    else:
        assert pushdowns.partition_filters is not None
        partition_filter = extract_comparison(pushdowns.partition_filters)
        assert partition_filter["op"] == "equal"
        assert partition_filter["left"]["name"] == partition_col
    if partition_kind == "visible_identity":
        assert pushdowns.filters is None
    else:
        # Bucket pruning is lossy, so the original data predicate must survive.
        assert pushdowns.filters is not None
        assert extract_comparison(pushdowns.filters)["left"]["name"] == "b"


# ---------------------------------------------------------------------------
# Identity transform: strict inequalities must NOT be relaxed
# ---------------------------------------------------------------------------


def test_identity_lt_is_not_relaxed_to_lteq():
    """source_col < 5 with Identity transform -> partition_filter must be `p_col < 5`, not `p_col <= 5`."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        columns=[("source_col", DataType.int32())],
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
        columns=[("source_col", DataType.int32())],
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
        columns=[("source_col", DataType.int32())],
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
        columns=[("source_col", DataType.int32())],
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
        columns=[("ts_col", DataType.timestamp(timeunit=TimeUnit.from_str("us")))],
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
        columns=[("ts_col", DataType.timestamp(timeunit=TimeUnit.from_str("us")))],
        partition_fields=[pfield],
        filter_expr=col("ts_col") > lit("2024-01-01").cast(DataType.timestamp(timeunit=TimeUnit.from_str("us"))),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    result = extract_comparison(pushdowns.partition_filters)
    assert result["op"] == "greater_than_or_equal", f"Year Gt must be relaxed to GtEq, got op={result['op']}"


# ---------------------------------------------------------------------------
# Identity partition predicates must survive when combined with ScalarFn predicates
# ---------------------------------------------------------------------------


_COMPARISON_OPS = frozenset(
    ["less_than", "less_than_or_equal", "greater_than", "greater_than_or_equal", "equal", "not_equal"]
)


def _collect_ops(tree: dict, ops: list[str] | None = None) -> list[str]:
    """Recursively collect all comparison operator names from an extracted predicate tree."""
    if ops is None:
        ops = []
    op = tree.get("op")
    if op in ("and", "or"):
        _collect_ops(tree["left"], ops)
        _collect_ops(tree["right"], ops)
    elif op in _COMPARISON_OPS:
        ops.append(op)
    return ops


def test_identity_partition_pred_preserved_with_scalar_fn_sibling():
    """Regression: identity partition predicates must not be dropped when a sibling predicate contains a ScalarFn.

    Before the fix, `source_col < '5' AND source_col > cast(abs(1) as string)` would
    only push down the ScalarFn predicate as a partition filter, losing the simple
    `source_col < '5'` predicate entirely.
    """
    pfield = _make_identity_partition_field("source_col", "source_col", DataType.string())
    pushdowns = _build_df_and_capture(
        columns=[("source_col", DataType.string()), ("user_id", DataType.int32())],
        partition_fields=[pfield],
        filter_expr="source_col < '5' and source_col > cast(abs(1) as string)",
    )

    assert pushdowns.partition_filters is not None, "Expected partition filters to be pushed down, but got None"
    result = extract_comparison(pushdowns.partition_filters)
    ops = _collect_ops(result)
    assert "less_than" in ops, f"Expected 'less_than' in partition_filters, got ops: {ops}"
    assert "greater_than" in ops, f"Expected 'greater_than' in partition_filters, got ops: {ops}"


def test_identity_pred_with_cast_sibling_both_pushed_down():
    """An identity partition predicate combined with a ScalarFn-containing predicate must both survive.

    Uses cast(abs(1) as string) instead of cast(1 as string) because the latter has no column
    references and may be constant-folded to a literal before rewrite_predicate_for_partitioning
    runs, which would make both conjuncts plain identity predicates and never trigger the
    ScalarFn/has_udf code path.
    """
    pfield = _make_identity_partition_field("source_col", "source_col", DataType.string())
    pushdowns = _build_df_and_capture(
        columns=[("source_col", DataType.string()), ("user_id", DataType.int32())],
        partition_fields=[pfield],
        filter_expr="source_col < '5' and source_col > cast(abs(1) as string)",
    )

    assert pushdowns.partition_filters is not None, "Expected partition filters to be pushed down, but got None"
    result = extract_comparison(pushdowns.partition_filters)
    ops = _collect_ops(result)
    assert len(ops) >= 2, f"Expected at least 2 comparison predicates in partition_filters, got {len(ops)}: {result}"


def test_identity_pred_with_scalar_fn_both_directions():
    """Swapped order: ScalarFn predicate first, then simple lit predicate must both appear."""
    pfield = _make_identity_partition_field("source_col", "source_col", DataType.string())
    pushdowns = _build_df_and_capture(
        columns=[("source_col", DataType.string()), ("user_id", DataType.int32())],
        partition_fields=[pfield],
        filter_expr="source_col > cast(abs(1) as string) and source_col < '5'",
    )

    assert pushdowns.partition_filters is not None, "Expected partition filters to be pushed down, but got None"
    result = extract_comparison(pushdowns.partition_filters)
    ops = _collect_ops(result)
    assert "greater_than" in ops, f"Expected 'greater_than' in partition_filters, got ops: {ops}"
    assert "less_than" in ops, f"Expected 'less_than' in partition_filters, got ops: {ops}"
