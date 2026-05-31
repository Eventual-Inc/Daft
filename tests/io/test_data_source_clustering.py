"""Tests for DataSource.get_clustering_spec() and shuffle elision.

A custom ``DataSource`` can declare how its output is hash-clustered at execution
time. When the keys a downstream ``groupby`` / ``Window.partition_by`` / ``distinct``
requires are covered by the declared clustering, the distributed planner skips the
shuffle it would otherwise insert.

The optimization only applies to the distributed (Flotilla) planner — the native
single-node runner never inserts a shuffle for these operators — so these tests run
only under the ray runner, like the other distributed shuffle tests.
"""

from __future__ import annotations

import io
from collections.abc import AsyncIterator, Iterator

import pyarrow as pa
import pytest

from daft import Window, col
from daft.functions import row_number
from daft.io.clustering import ClusteringSpec
from daft.io.source import DataSource, DataSourceTask
from daft.recordbatch import MicroPartition
from daft.schema import Schema
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="clustering-aware shuffle elision applies to the distributed planner (ray runner)",
)


def _hour_bucket(e: col) -> col:
    """A deterministic, expression-valued partition key: ``id // 3_600_000``."""
    return e // 3_600_000


class _InMemoryTask(DataSourceTask):
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        yield MicroPartition.from_arrow(self._table)


class ClusteredSource(DataSource):
    """In-memory source that emits two tasks and optionally declares its clustering."""

    def __init__(self, table: pa.Table, clustering: ClusteringSpec | None) -> None:
        self._table = table
        self._clustering = clustering

    @property
    def name(self) -> str:
        return "ClusteredSource"

    @property
    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._table.schema)

    def get_clustering_spec(self) -> ClusteringSpec | None:
        return self._clustering

    async def get_tasks(self, pushdowns) -> AsyncIterator[DataSourceTask]:
        # Two tasks => two execution partitions, so an inserted shuffle is observable.
        mid = self._table.num_rows // 2
        yield _InMemoryTask(self._table.slice(0, mid))
        yield _InMemoryTask(self._table.slice(mid))


def _has_shuffle(df) -> bool:
    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    physical = buf.getvalue().split("== Physical Plan ==")[-1]
    return "Shuffle" in physical


@pytest.fixture
def table() -> pa.Table:
    return pa.table({"a": [1, 1, 2, 2], "b": [1, 2, 1, 2], "c": [10, 20, 30, 40]})


def test_no_declaration_still_shuffles(table: pa.Table):
    """A source that declares nothing keeps today's behavior: the groupby shuffles."""
    df = ClusteredSource(table, None).read().groupby("a", "b").sum("c")
    assert _has_shuffle(df)


def test_groupby_exact_match_no_shuffle(table: pa.Table):
    df = ClusteredSource(table, ClusteringSpec.hash("a", "b")).read().groupby("a", "b").sum("c")
    assert not _has_shuffle(df)


def test_window_exact_match_no_shuffle(table: pa.Table):
    source = ClusteredSource(table, ClusteringSpec.hash("a", "b"))
    df = source.read().with_column("rn", row_number().over(Window().partition_by("a", "b").order_by("c")))
    assert not _has_shuffle(df)


def test_distinct_exact_match_no_shuffle(table: pa.Table):
    df = ClusteredSource(table, ClusteringSpec.hash("a", "b")).read().distinct()
    assert not _has_shuffle(df)


def test_window_subset_no_shuffle(table: pa.Table):
    """partition_by ⊇ clustering keys: the operator groups refine the input distribution."""
    source = ClusteredSource(table, ClusteringSpec.hash("a", "b"))
    df = source.read().with_column("rn", row_number().over(Window().partition_by("a", "b", "c").order_by("c")))
    assert not _has_shuffle(df)


def test_groupby_subset_no_shuffle(table: pa.Table):
    df = ClusteredSource(table, ClusteringSpec.hash("a", "b")).read().groupby("a", "b", "c").sum("c")
    assert not _has_shuffle(df)


def test_unsound_inverse_still_shuffles(table: pa.Table):
    """Input clustered by a richer key set than the operator partitions by must still shuffle."""
    df = ClusteredSource(table, ClusteringSpec.hash("a", "b", "c")).read().groupby("a", "b").sum("c")
    assert _has_shuffle(df)


def test_expression_clustering_through_project_no_shuffle():
    """An expression-valued clustering key follows the projection that materializes it."""
    table = pa.table(
        {
            "producer": ["x", "x", "y", "y"],
            "id": [0, 3_600_000, 7_200_000, 10_800_000],
            "ts": [1, 2, 3, 4],
        }
    )
    source = ClusteredSource(table, ClusteringSpec.hash(col("producer"), _hour_bucket(col("id"))))
    # Materialize the hour bucket as a derived column, then group by it.
    df = source.read().with_column("__h", _hour_bucket(col("id"))).groupby("producer", "__h").sum("ts")
    assert not _has_shuffle(df)


def test_clustering_query_is_correct(table: pa.Table):
    """The shuffle-free plan still computes the correct result."""
    df = ClusteredSource(table, ClusteringSpec.hash("a", "b")).read().groupby("a", "b").sum("c")
    result = df.sort(["a", "b"]).to_pydict()
    assert result == {"a": [1, 1, 2, 2], "b": [1, 2, 1, 2], "c": [10, 20, 30, 40]}


def test_subset_groupby_is_correct():
    """A group-by whose keys are a strict superset of the clustering computes correct groups.

    The source is clustered by ``a`` (each ``a`` value lives entirely within one task), and we
    group by ``(a, b)`` — a superset. Each ``(a, b)`` group is therefore contained within a single
    partition, so the single-stage local aggregation must still produce complete groups. Multiple
    rows share an ``(a, b)`` group here to exercise the local accumulation.
    """
    # Two tasks split at the midpoint: task 0 holds all a=1 rows, task 1 all a=2 rows.
    table = pa.table(
        {
            "a": [1, 1, 1, 2, 2, 2],
            "b": [1, 1, 2, 1, 2, 2],
            "c": [10, 5, 20, 30, 40, 1],
        }
    )
    df = ClusteredSource(table, ClusteringSpec.hash("a")).read().groupby("a", "b").sum("c")
    assert not _has_shuffle(df)  # group_by (a, b) ⊇ clustering (a) => no shuffle
    result = df.sort(["a", "b"]).to_pydict()
    assert result == {"a": [1, 1, 2, 2], "b": [1, 2, 1, 2], "c": [15, 20, 30, 41]}
