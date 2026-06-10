"""Tests for DataSource.get_clustering_keys() with ClusteringKeys.range() and asof join shuffle elision.

A custom ``DataSource`` can declare that its output is range-partitioned and sorted by declaring
``ClusteringKeys.range(*cols)``. When both sides of an asof join carry a matching range clustering,
the distributed planner skips the expensive sample + range-repartition step and pairs files
directly by index.

The optimization only applies to the distributed (Flotilla / Ray) planner, so these tests run
only under the ray runner.

Detection: the physical plan's ``AsofJoin`` node emits a ``"Needs range repartition: false"``
line when the hint fires. We grep for that line rather than looking for a ``Shuffle`` node,
because the asof join's range repartition is internal to the node and never produces a separate
pipeline node.
"""

from __future__ import annotations

import io
from collections.abc import AsyncIterator

import pyarrow as pa
import pytest

from daft import col
from daft.exceptions import DaftCoreException
from daft.io.clustering import ClusteringKeys
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="range clustering shuffle elision only applies to the distributed planner (ray runner)",
)

# Tests that rely on ClusteringKeys.range() are skipped until the feature is implemented.
range_hint_not_implemented = pytest.mark.skip(reason="ClusteringKeys.range() is not yet implemented")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _InMemoryTask(DataSourceTask):
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        yield RecordBatch.from_arrow_table(self._table)


class RangeClusteredSource(DataSource):
    """Emits two tasks with non-overlapping, sorted time ranges and an optional clustering hint.

    Always emits exactly two tasks regardless of data size, so there are always two partitions
    at plan time and the repartition decision is observable via explain() regardless of row count.
    """

    def __init__(self, table: pa.Table, clustering: ClusteringKeys | None) -> None:
        self._table = table
        self._clustering = clustering

    @property
    def name(self) -> str:
        return "RangeClusteredSource"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    def get_clustering_keys(self) -> ClusteringKeys | None:
        return self._clustering

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        mid = self._table.num_rows // 2
        yield _InMemoryTask(self._table.slice(0, mid))
        yield _InMemoryTask(self._table.slice(mid))


def _needs_repartition(df) -> bool:
    """Returns True if the physical plan shows the asof join will range-repartition its inputs."""
    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    physical = buf.getvalue().split("== Physical Plan ==")[-1]
    return "Needs range repartition: false" not in physical


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def left_table() -> pa.Table:
    return pa.table(
        {
            "ts": [1, 2, 3, 4, 5, 6],
            "a": ["x", "x", "x", "y", "y", "y"],
            "val": [10, 20, 30, 40, 50, 60],
        }
    )


@pytest.fixture
def right_table() -> pa.Table:
    return pa.table(
        {
            "ts": [1, 3, 5],
            "a": ["x", "x", "y"],
            "label": ["p", "q", "r"],
        }
    )


# ---------------------------------------------------------------------------
# Baseline — verifies the default path (no hint) always repartitions.
# This test does not depend on ClusteringKeys.range() and should always pass.
# ---------------------------------------------------------------------------


def test_no_declaration_repartitions_by_default(left_table, right_table):
    """Without a clustering hint the asof join always range-repartitions its inputs.

    This is the baseline that confirms _needs_repartition() correctly detects the
    default behaviour before any optimisation is applied.
    """
    left = RangeClusteredSource(left_table, None).read()
    right = RangeClusteredSource(right_table, None).read()
    df = left.join_asof(right, on="ts", by=[])
    assert _needs_repartition(df)


# ---------------------------------------------------------------------------
# Positive tests — repartition should be skipped (requires ClusteringKeys.range())
# ---------------------------------------------------------------------------


@range_hint_not_implemented
def test_exact_key_match_no_repartition():
    """Both sides declare range on the join key → repartition skipped and result is correct."""
    hint = ClusteringKeys.range("ts")
    left = RangeClusteredSource(pa.table({"ts": [1, 2, 3, 4, 5, 6], "val": [10, 20, 30, 40, 50, 60]}), hint).read()
    right = RangeClusteredSource(
        pa.table({"ts": [4, 5, 6, 7, 8, 9], "label": ["a", "b", "c", "d", "e", "f"]}), hint
    ).read()
    df = left.join_asof(right, on="ts", by=[])
    assert not _needs_repartition(df)
    result = df.sort(["ts"]).to_pydict()
    # left ts=1,2,3 have no right match (right starts at 4); left ts=4,5,6 match right ts=4,5,6
    assert result["ts"] == [1, 2, 3, 4, 5, 6]
    assert result["label"] == [None, None, None, "a", "b", "c"]


@range_hint_not_implemented
def test_hint_persists_through_projection(left_table, right_table):
    """A with_column between the source and the asof join must not drop the range hint."""
    hint = ClusteringKeys.range("ts")
    left = RangeClusteredSource(left_table, hint).read().with_column("doubled", col("val") * 2)
    right = RangeClusteredSource(right_table, hint).read().with_column("upper", col("label"))
    df = left.join_asof(right, on="ts", by=[])
    assert not _needs_repartition(df)
    result = df.sort(["ts"]).to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]
    assert result["label"] == ["p", "p", "q", "q", "r", "r"]


# ---------------------------------------------------------------------------
# Negative tests — repartition must still fire, or planning must raise
# ---------------------------------------------------------------------------


@range_hint_not_implemented
def test_wrong_column_still_repartitions(left_table, right_table):
    """Declaring range on a column that doesn't match the join key → repartition still needed."""
    hint = ClusteringKeys.range("val")  # 'val' is not the join key
    left = RangeClusteredSource(left_table, hint).read()
    right = RangeClusteredSource(right_table, hint).read()
    df = left.join_asof(right, on="ts", by=[])
    assert _needs_repartition(df)


@range_hint_not_implemented
def test_range_key_subset_of_composite_still_repartitions(left_table, right_table):
    """Source range key [ts] is a subset of the composite join key [a, ts] → repartition still needed.

    Range partitioning by [ts] gives non-overlapping ts ranges per partition, but rows with
    different `a` values can span partitions. The join with by=["a"] needs each a-group's rows
    to be co-located, which the ts-only range hint does not guarantee.
    """
    hint = ClusteringKeys.range("ts")
    left = RangeClusteredSource(left_table, hint).read()
    right = RangeClusteredSource(right_table, hint).read()
    df = left.join_asof(right, on="ts", by=["a"])
    assert _needs_repartition(df)


@range_hint_not_implemented
def test_range_key_superset_of_composite_still_repartitions(left_table, right_table):
    """Source range key [a, ts] is a superset of the join key [ts] → repartition still needed.

    Lexicographic range partitioning by [a, ts] gives non-overlapping [a, ts] ranges, but ts
    ranges can overlap across partitions (partition 1 has a="x" ts=1-3, partition 2 has a="y"
    ts=1-3). A join on ts alone would miss matches across partitions.
    """
    hint = ClusteringKeys.range("a", "ts")
    left = RangeClusteredSource(left_table, hint).read()
    right = RangeClusteredSource(right_table, hint).read()
    df = left.join_asof(right, on="ts", by=[])
    assert _needs_repartition(df)


@range_hint_not_implemented
def test_only_one_side_hinted_still_repartitions(left_table, right_table):
    """Only the left side has a range hint; both sides must be hinted to skip repartition."""
    hint = ClusteringKeys.range("ts")
    left = RangeClusteredSource(left_table, hint).read()
    right = RangeClusteredSource(right_table, None).read()
    df = left.join_asof(right, on="ts", by=[])
    assert _needs_repartition(df)


@range_hint_not_implemented
def test_misdeclared_column_raises(left_table, right_table):
    """Declaring a clustering key that doesn't exist in the schema raises at plan time."""
    hint = ClusteringKeys.range("not_a_column")
    left = RangeClusteredSource(left_table, hint).read()
    right = RangeClusteredSource(right_table, hint).read()
    df = left.join_asof(right, on="ts", by=[])
    with pytest.raises(DaftCoreException, match='Column "not_a_column" not found in schema'):
        df.explain(show_all=True, file=io.StringIO())
