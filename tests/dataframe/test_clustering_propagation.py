"""Clustering propagation through projections in the distributed planner.

A projection (e.g. ``with_column``) between a clustering-producing operator (like
``repartition``) and a clustering-consuming operator (``groupby`` / ``Window`` / ``distinct``)
must preserve the input's clustering. Otherwise the consumer needlessly inserts a *second*
shuffle on top of the one the repartition already performed.

These tests assert on the number of shuffle nodes in the distributed physical plan, so they run
only under the ray runner (the single-node runner does not insert shuffles for these operators).

Regression guard: before clustering propagated through bound projection expressions, each of
these chains planned two shuffles instead of one.
"""

from __future__ import annotations

import io

import pytest

import daft
from daft import Window, col
from daft.functions import row_number
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="clustering propagation affects the distributed planner (ray runner)",
)


def _num_shuffles(df: daft.DataFrame) -> int:
    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    physical = buf.getvalue().split("== Physical Plan ==")[-1]
    return physical.count("Shuffle")


@pytest.fixture
def clustered():
    # An explicit repartition is itself one shuffle; downstream operators should reuse its
    # clustering rather than adding another.
    return daft.from_pydict({"a": [1, 1, 2, 2], "b": [1, 2, 1, 2], "c": [10, 20, 30, 40]}).repartition(2, "a", "b")


def test_repartition_alone_has_one_shuffle(clustered):
    assert _num_shuffles(clustered) == 1


def test_projection_then_groupby_adds_no_shuffle(clustered):
    df = clustered.with_column("x", col("a") + 1).groupby("a", "b").sum("c")
    assert _num_shuffles(df) == 1


def test_projection_then_window_adds_no_shuffle(clustered):
    df = clustered.with_column("x", col("a") + 1).with_column(
        "rn", row_number().over(Window().partition_by("a", "b").order_by("c"))
    )
    assert _num_shuffles(df) == 1


def test_projection_then_distinct_adds_no_shuffle(clustered):
    # distinct on exactly the clustering keys, after a projection that passes them through.
    df = clustered.with_column("x", col("a") + 1).distinct("a", "b")
    assert _num_shuffles(df) == 1


def test_projection_then_groupby_is_correct(clustered):
    df = clustered.with_column("x", col("a") + 1).groupby("a", "b").sum("c")
    assert df.sort(["a", "b"]).to_pydict() == {"a": [1, 1, 2, 2], "b": [1, 2, 1, 2], "c": [10, 20, 30, 40]}


def test_expression_projection_preserves_clustering():
    """A derived-column projection whose passthrough columns cover the clustering keeps it intact."""
    df = (
        daft.from_pydict({"a": [1, 1, 2, 2], "b": [1, 2, 1, 2], "c": [10, 20, 30, 40]})
        .repartition(2, "a", "b")
        .with_column("bucket", col("c") % 4)  # a computed column that does not touch (a, b)
        .groupby("a", "b")
        .sum("c")
    )
    assert _num_shuffles(df) == 1
