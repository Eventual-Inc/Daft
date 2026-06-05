"""Tests for ClusteringKeys.range() shuffle elision in distributed sorts.

A custom ``DataSource`` can declare that its output is range-partitioned and sorted by declaring
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
    """Emits two tasks with non-overlapping, sorted ranges and an optional clustering hint.

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
    return pa.table(
        {
            "ts": [1, 2, 3, 4, 5, 6],
            "val": [10, 20, 30, 40, 50, 60],
        }
    )


# ---------------------------------------------------------------------------
# Baseline — verifies the default path (no hint) always repartitions.
# ---------------------------------------------------------------------------


def test_no_declaration_repartitions_by_default(source_table):
    """Without a clustering hint a sort always range-repartitions its inputs."""
    df = RangeClusteredSource(source_table, None).read().sort("ts")
    assert _sort_needs_repartition(df)


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


def test_sort_on_sort_different_key_repartitions():
    """A sort by a different key than the input's range clustering triggers repartition."""
    df = daft.from_pydict({"ts": [4, 2, 6, 1, 3, 5], "val": [40, 20, 60, 10, 30, 50]}).repartition(2).sort("ts")
    df2 = df.sort("val")
    assert _sort_needs_repartition(df2)


def test_multi_column_partial_key_match_repartitions():
    """Declaring range on [ts, val] but sorting only by [ts] → repartition still needed.

    The clustering keys must exactly match the sort keys; a superset does not qualify.
    """
    hint = ClusteringKeys.range("ts", "val")
    table = pa.table({"ts": [1, 2, 3, 4, 5, 6], "val": [10, 20, 30, 40, 50, 60]})
    df = RangeClusteredSource(table, hint).read().sort("ts")
    assert _sort_needs_repartition(df)


def test_multi_column_exact_match_skips_repartition():
    """Declaring range on both sort keys causes the sort to skip its repartition."""
    hint = ClusteringKeys.range("ts", "val")
    table = pa.table({"ts": [1, 2, 3, 4, 5, 6], "val": [10, 20, 30, 40, 50, 60]})
    df = RangeClusteredSource(table, hint).read().sort(["ts", "val"])
    assert not _sort_needs_repartition(df)
    result = df.to_pydict()
    assert result["ts"] == [1, 2, 3, 4, 5, 6]
