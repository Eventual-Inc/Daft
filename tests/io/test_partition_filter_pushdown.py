"""Tests for partition filter pushdown correctness with different PartitionTransforms.

Specifically verifies that:
- Identity transform: strict inequalities (< and >) are NOT relaxed to <= / >=
- Lossy transforms (Year, Month, Day, Hour, IcebergTruncate): strict inequalities ARE relaxed
"""

from __future__ import annotations

from collections.abc import Iterator

from daft.daft import PyPartitionField, PyPartitionTransform, PyPushdowns, ScanOperatorHandle, ScanTask
from daft.dataframe import DataFrame
from daft.expressions import col, lit
from daft.io.pushdowns import Pushdowns
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.schema import DataType, Field, TimeUnit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_identity_partition_field(partition_col: str, source_col: str, dtype: DataType) -> PyPartitionField:
    """Build a PyPartitionField with Identity transform."""
    partition_field = Field.create(partition_col, dtype)
    source_field = Field.create(source_col, dtype)
    transform = PyPartitionTransform.identity()
    return PyPartitionField(partition_field._field, source_field._field, transform)


def _make_year_partition_field(partition_col: str, source_col: str) -> PyPartitionField:
    """Build a PyPartitionField with Year transform."""
    partition_field = Field.create(partition_col, DataType.int32())
    source_field = Field.create(source_col, DataType.timestamp(timeunit=TimeUnit.from_str("us")))
    transform = PyPartitionTransform.year()
    return PyPartitionField(partition_field._field, source_field._field, transform)


class _CapturePushdownsScanOperator(ScanOperator):
    """A minimal ScanOperator that captures the pushdowns it receives for inspection."""

    def __init__(self, schema: Schema, partition_fields: list[PyPartitionField]) -> None:
        self._schema = schema
        self._partition_fields = partition_fields
        # Captured pushdowns will be stored here after to_scan_tasks is called.
        self.captured_pushdowns: list[Pushdowns] = []

    def name(self) -> str:
        return "CapturePushdownsScanOperator"

    def display_name(self) -> str:
        return self.name()

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partition_fields

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [self.display_name()]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        self.captured_pushdowns.append(Pushdowns._from_pypushdowns(pushdowns))
        # Return an empty iterator – we only care about the pushdowns, not actual data.
        return iter([])


def _build_df_and_capture(
    source_col: str,
    dtype: DataType,
    partition_fields: list[PyPartitionField],
    filter_expr,
) -> Pushdowns:
    """Build a DataFrame with the given partition fields, apply filter_expr, collect, and return captured pushdowns."""
    schema = Schema._from_field_name_and_types([(source_col, dtype)])
    operator = _CapturePushdownsScanOperator(schema, partition_fields)
    handle = ScanOperatorHandle.from_python_scan_operator(operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    df = DataFrame(builder).filter(filter_expr)
    df.collect()
    assert len(operator.captured_pushdowns) == 1, "Expected exactly one call to to_scan_tasks"
    return operator.captured_pushdowns[0]


# ---------------------------------------------------------------------------
# Identity transform: strict inequalities must NOT be relaxed
# ---------------------------------------------------------------------------


def test_identity_lt_is_not_relaxed_to_lteq():
    """source_col < 5 with Identity transform → partition_filter must be `p_col < 5`, not `p_col <= 5`."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") < lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    partition_filter_str = str(pushdowns.partition_filters)
    # The rewritten filter should reference the partition column with the original strict operator.
    assert "p_col" in partition_filter_str, f"Expected partition column in filter, got: {partition_filter_str}"
    assert "<=" not in partition_filter_str, f"Identity Lt must NOT be relaxed to LtEq, but got: {partition_filter_str}"
    assert "<" in partition_filter_str, f"Expected '<' in partition filter, got: {partition_filter_str}"


def test_identity_gt_is_not_relaxed_to_gteq():
    """source_col > 5 with Identity transform → partition_filter must be `p_col > 5`, not `p_col >= 5`."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") > lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    partition_filter_str = str(pushdowns.partition_filters)
    assert "p_col" in partition_filter_str, f"Expected partition column in filter, got: {partition_filter_str}"
    assert ">=" not in partition_filter_str, f"Identity Gt must NOT be relaxed to GtEq, but got: {partition_filter_str}"
    assert ">" in partition_filter_str, f"Expected '>' in partition filter, got: {partition_filter_str}"


def test_identity_lteq_stays_lteq():
    """source_col <= 5 with Identity transform → partition_filter must be `p_col <= 5` (already inclusive)."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") <= lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    partition_filter_str = str(pushdowns.partition_filters)
    assert "<=" in partition_filter_str, f"Expected '<=' in partition filter, got: {partition_filter_str}"


def test_identity_gteq_stays_gteq():
    """source_col >= 5 with Identity transform → partition_filter must be `p_col >= 5` (already inclusive)."""
    pfield = _make_identity_partition_field("p_col", "source_col", DataType.int32())
    pushdowns = _build_df_and_capture(
        source_col="source_col",
        dtype=DataType.int32(),
        partition_fields=[pfield],
        filter_expr=col("source_col") >= lit(5),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    partition_filter_str = str(pushdowns.partition_filters)
    assert ">=" in partition_filter_str, f"Expected '>=' in partition filter, got: {partition_filter_str}"


# ---------------------------------------------------------------------------
# Non-identity (lossy) transforms: strict inequalities MUST be relaxed
# ---------------------------------------------------------------------------


def test_year_lt_is_relaxed_to_lteq():
    """ts_col < value with Year transform → partition_filter must be `p_year <= value` (relaxed)."""
    pfield = _make_year_partition_field("p_year", "ts_col")
    pushdowns = _build_df_and_capture(
        source_col="ts_col",
        dtype=DataType.timestamp(timeunit=TimeUnit.from_str("us")),
        partition_fields=[pfield],
        filter_expr=col("ts_col") < lit(2024),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    partition_filter_str = str(pushdowns.partition_filters)
    assert "<=" in partition_filter_str, f"Year Lt must be relaxed to LtEq, but got: {partition_filter_str}"


def test_year_gt_is_relaxed_to_gteq():
    """ts_col > value with Year transform → partition_filter must be `p_year >= value` (relaxed)."""
    pfield = _make_year_partition_field("p_year", "ts_col")
    pushdowns = _build_df_and_capture(
        source_col="ts_col",
        dtype=DataType.timestamp(timeunit=TimeUnit.from_str("us")),
        partition_fields=[pfield],
        filter_expr=col("ts_col") > lit(2024),
    )

    assert pushdowns.partition_filters is not None, "Expected a partition filter to be pushed down"
    partition_filter_str = str(pushdowns.partition_filters)
    assert ">=" in partition_filter_str, f"Year Gt must be relaxed to GtEq, but got: {partition_filter_str}"
