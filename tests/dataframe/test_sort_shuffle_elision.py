"""Tests for ClusteringKeys.range() shuffle elision in distributed sorts.

A custom ``DataSource`` can declare that its output is range-partitioned by declaring
``ClusteringKeys.range(*cols)``. When a distributed sort is applied over such a source using the
same key(s), the planner skips the expensive sample + range-repartition step and sorts each
partition locally in place.

The optimization only applies to the distributed (Flotilla / Ray) planner, so these tests run
only under the ray runner.

Detection: the physical plan's ``Sort`` node emits a ``"Needs range repartition: false"`` line
when the hint fires.
"""

from __future__ import annotations

import io
from collections.abc import AsyncIterator

import pyarrow as pa
import pytest

import daft
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
    """Emits two tasks with non-overlapping ranges and an optional clustering hint."""

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


def _sort_needs_repartition(df) -> bool:
    """Returns True if any Sort node in the physical plan will range-repartition its inputs."""
    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    physical = buf.getvalue().split("== Physical Plan ==")[-1]
    return "Needs range repartition: false" not in physical


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def source_table() -> pa.Table:
    # Values are shuffled within each partition (rows 0-2 → ts [1,3], rows 3-5 → ts [4,6])
    # so tests verify non-overlapping range partitioning, not pre-sorted order.
    return pa.table(
        {
            "ts": [2, 1, 3, 5, 4, 6],
            "val": [20, 10, 30, 50, 40, 60],
        }
    )


# ---------------------------------------------------------------------------
# Baseline — verifies the default path (no hint) always repartitions.
# ---------------------------------------------------------------------------


def test_no_declaration_repartitions_by_default(source_table):
    """Without a clustering hint a sort always range-repartitions its inputs."""
    df = RangeClusteredSource(source_table, None).read().sort("ts")
    assert _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [1, 2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# Positive tests — repartition should be skipped
# ---------------------------------------------------------------------------


def test_exact_key_match_skips_repartition(source_table):
    """Declaring range on the sort key causes the sort to skip its repartition."""
    hint = ClusteringKeys.range("ts")
    df = RangeClusteredSource(source_table, hint).read().sort("ts")
    assert not _sort_needs_repartition(df)
    result = df.to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]


def test_col_expression_key_skips_repartition(source_table):
    """ClusteringKeys.range accepts col() expressions, not just string names."""
    hint = ClusteringKeys.range(col("ts"))
    df = RangeClusteredSource(source_table, hint).read().sort("ts")
    assert not _sort_needs_repartition(df)
    result = df.to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]


def test_hint_persists_through_projection(source_table):
    """A with_column between the range-clustered source and the sort must not drop the hint."""
    hint = ClusteringKeys.range("ts")
    df = RangeClusteredSource(source_table, hint).read().with_column("doubled", col("val") * 2).sort("ts")
    assert not _sort_needs_repartition(df)
    result = df.to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]


def test_sort_on_sort_skips_repartition():
    """A sort whose input is a prior sort by the same key skips its own repartition."""
    df = daft.from_pydict({"ts": [4, 2, 6, 1, 3, 5], "val": [40, 20, 60, 10, 30, 50]}).repartition(2).sort("ts")
    df2 = df.sort("ts")
    assert not _sort_needs_repartition(df2)
    result = df2.to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]


def test_descending_declared_descending_sort_skips_repartition():
    """Declaring descending range and sorting descending → direction matches, skip repartition.

    Partition 0 holds ts [4,5,6] (higher range) and partition 1 holds ts [1,2,3] (lower range)
    """
    hint = ClusteringKeys.range("ts", descending=True)
    table = pa.table({"ts": [5, 4, 6, 2, 1, 3], "val": [50, 40, 60, 20, 10, 30]})
    df = RangeClusteredSource(table, hint).read().sort("ts", desc=True)
    assert not _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [6, 5, 4, 3, 2, 1]


# ---------------------------------------------------------------------------
# Negative tests — repartition must still fire, or planning must raise
# ---------------------------------------------------------------------------


def test_misdeclared_column_raises(source_table):
    """Declaring a clustering key that doesn't exist in the schema raises at plan time."""
    hint = ClusteringKeys.range("not_a_column")
    df = RangeClusteredSource(source_table, hint).read().sort("ts")
    with pytest.raises(DaftCoreException, match='Column "not_a_column" not found in schema'):
        df.explain(show_all=True, file=io.StringIO())


def test_wrong_column_still_repartitions(source_table):
    """Declaring range on a column that exists but isn't the sort key → repartition still needed."""
    hint = ClusteringKeys.range("val")
    df = RangeClusteredSource(source_table, hint).read().sort("ts")
    assert _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [1, 2, 3, 4, 5, 6]


def test_sort_on_sort_different_key_repartitions():
    """A sort by a different key than the input's range clustering triggers repartition."""
    df = daft.from_pydict({"ts": [4, 2, 6, 1, 3, 5], "val": [40, 20, 60, 10, 30, 50]}).repartition(2).sort("ts")
    df2 = df.sort("val")
    assert _sort_needs_repartition(df2)
    assert df2.to_pydict()["val"] == [10, 20, 30, 40, 50, 60]


def test_multi_column_partial_key_match_repartitions(source_table):
    """Declaring range on [ts, val] but sorting only by [ts] → repartition still needed.

    The clustering keys must exactly match the sort keys; a superset does not qualify.
    """
    hint = ClusteringKeys.range("ts", "val")
    df = RangeClusteredSource(source_table, hint).read().sort("ts")
    assert _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [1, 2, 3, 4, 5, 6]


def test_multi_column_exact_match_skips_repartition(source_table):
    """Declaring range on both sort keys causes the sort to skip its repartition."""
    hint = ClusteringKeys.range("ts", "val")
    df = RangeClusteredSource(source_table, hint).read().sort(["ts", "val"])
    assert not _sort_needs_repartition(df)
    result = df.to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]


def test_ascending_declared_descending_sort_repartitions(source_table):
    """Declaring ascending range but sorting descending → direction mismatch, must repartition."""
    hint = ClusteringKeys.range("ts")  # descending=False (default)
    df = RangeClusteredSource(source_table, hint).read().sort("ts", desc=True)
    assert _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [6, 5, 4, 3, 2, 1]


def test_multi_column_mixed_direction_repartitions(source_table):
    """Declaring all-ascending range but sorting with mixed directions → must repartition.

    A single descending=False flag expands to [False, False] for both keys. A sort with
    descending=[False, True] doesn't match, so the shuffle cannot be skipped.
    """
    hint = ClusteringKeys.range("ts", "val")  # descending=False → [False, False]
    df = RangeClusteredSource(source_table, hint).read().sort(["ts", "val"], desc=[False, True])
    assert _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [1, 2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# Null placement — nulls_first is part of the range spec
# ---------------------------------------------------------------------------


def test_sort_on_sort_different_nulls_first_repartitions():
    """Same key and direction but different nulls_first must repartition.

    The nulls physically live in the wrong partition for the new null order.
    """
    df = daft.from_pydict({"a": [5, None, 3, 8, None, 1, 9, 4, 7, 2]}).repartition(3).sort("a")
    df2 = df.sort("a", nulls_first=True)
    assert _sort_needs_repartition(df2)
    pytest.xfail(
        "pre-existing: the distributed sort's range shuffle routes nulls via search_sorted, "
        "which has no nulls_first support, so nulls land in the wrong partition"
    )
    assert df2.to_pydict()["a"] == [None, None, 1, 2, 3, 4, 5, 7, 8, 9]


def test_sort_on_sort_same_nulls_first_skips_repartition():
    """Matching nulls_first (non-default) still skips the repartition."""
    df = daft.from_pydict({"a": [5, None, 3, 8, None, 1, 9, 4, 7, 2]}).repartition(3).sort("a", nulls_first=True)
    df2 = df.sort("a", nulls_first=True)
    assert not _sort_needs_repartition(df2)
    pytest.xfail(
        "pre-existing: the first sort's own shuffle already places nulls incorrectly "
        "(search_sorted has no nulls_first support)"
    )
    assert df2.to_pydict()["a"] == [None, None, 1, 2, 3, 4, 5, 7, 8, 9]


def test_hint_nulls_first_mismatch_repartitions():
    """Hint defaults to nulls-last (ascending); a nulls-first sort must repartition."""
    table = pa.table({"ts": [1, 2, 3, None, None, None], "val": [10, 20, 30, 40, 50, 60]})
    hint = ClusteringKeys.range("ts")  # nulls_first defaults to descending = False
    df = RangeClusteredSource(table, hint).read().sort("ts", nulls_first=True)
    assert _sort_needs_repartition(df)
    pytest.xfail(
        "pre-existing: the distributed sort's range shuffle routes nulls via search_sorted, "
        "which has no nulls_first support, so nulls land in the wrong partition"
    )
    assert df.to_pydict()["ts"] == [None, None, None, 1, 2, 3]


def test_hint_nulls_first_match_skips_repartition():
    """Declaring nulls in the first partition lets a nulls-first sort skip its shuffle."""
    table = pa.table({"ts": [None, None, None, 1, 2, 3], "val": [10, 20, 30, 40, 50, 60]})
    hint = ClusteringKeys.range("ts", nulls_first=True)
    df = RangeClusteredSource(table, hint).read().sort("ts", nulls_first=True)
    assert not _sort_needs_repartition(df)
    assert df.to_pydict()["ts"] == [None, None, None, 1, 2, 3]
