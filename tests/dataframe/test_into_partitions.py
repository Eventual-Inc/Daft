from __future__ import annotations

import tempfile
from contextlib import contextmanager

import pytest

import daft
from daft import col
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="IntoPartitions requires Ray runner to be in use",
)


def test_into_partitions_some_empty(make_df) -> None:
    data = {"foo": [1, 2, 3]}
    df = make_df(data).into_partitions(32).collect()
    partitions = list(df.iter_partitions())

    if get_tests_daft_runner_name() == "ray":
        import ray

        partitions = ray.get(partitions)
    values = list(v.to_pydict() for v in partitions)
    assert values[0] == {"foo": [1]}
    assert values[1] == {"foo": [2]}
    assert values[2] == {"foo": [3]}
    for i in range(3, 32):
        assert values[i] == {"foo": []}


def test_into_partitions_split(make_df) -> None:
    data = {"foo": list(range(100))}
    parts = list(make_df(data).into_partitions(20).iter_partitions())
    assert len(parts) == 20
    if get_tests_daft_runner_name() == "ray":
        import ray

        parts = ray.get(parts)
    values = set(v for p in parts for v in p.to_pydict()["foo"])
    assert values == set(range(100))


def test_into_partitions_coalesce() -> None:
    df = daft.range(100, partitions=10).into_partitions(2)
    parts = list(df.iter_partitions())
    assert len(parts) == 2
    if get_tests_daft_runner_name() == "ray":
        import ray

        parts = ray.get(parts)
    values = set(v for p in parts for v in p.to_pydict()["id"])
    assert values == set(range(100))


def test_into_partitions_split_and_coalesce(make_df) -> None:
    data = {"foo": list(range(100))}
    df = make_df(data).into_partitions(20).into_partitions(1).collect()
    assert df.to_pydict() == data


def test_into_partitions_some_no_split(make_df) -> None:
    data = {"foo": [1, 2, 3]}

    # Materialize as 3 partitions
    df = make_df(data).into_partitions(3).collect()

    # Attempt to split into 4 partitions, so only 1 split occurs
    df = df.into_partitions(4).collect()

    assert df.to_pydict() == data


@pytest.fixture(scope="function")
def flight_shuffle_ctx():
    """Run the body with the disk-based flight shuffle backend and a temp spill dir."""

    @contextmanager
    def _ctx():
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            daft.execution_config_ctx(shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[temp_dir]) as ctx,
        ):
            yield ctx

    return _ctx


# Covers all three branches of IntoPartitionsNode (coalesce / equal / split) on the
# flight backend. Coalescing used to panic in the flight read path with
# "expected flight partition ref": the branch materializes its child's output as
# plain in-memory refs, which the flight reader cannot address.
@pytest.mark.parametrize("num_partitions", [1, 3, 7, 8, 9, 16])
def test_into_partitions_under_flight_shuffle(flight_shuffle_ctx, num_partitions) -> None:
    with flight_shuffle_ctx():
        df = daft.range(10_000, partitions=8).into_partitions(num_partitions)
        parts = list(df.iter_partitions())

        import ray

        values = set(v for p in ray.get(parts) for v in p.to_pydict()["id"])
        assert values == set(range(10_000))

        if num_partitions <= 8:
            # Coalescing and the equal case land on exactly `num_partitions`.
            # Splitting does not, on either backend: the per-task split factor is
            # not part of the plan fingerprint, so same-fingerprint tasks sharing a
            # worker pipeline all use whichever factor was built first (8 -> 9 has
            # been observed as both 16 and 8). Only the row set is checked there.
            assert len(parts) == num_partitions


# The coalesce output must be consumable by whatever the parent node stacks on top,
# so the read task may not end in a flight write (which emits partition refs instead
# of rows).
@pytest.mark.parametrize("num_partitions", [1, 3, 7])
def test_into_partitions_coalesce_under_flight_shuffle_with_downstream_ops(flight_shuffle_ctx, num_partitions) -> None:
    with flight_shuffle_ctx():
        df = daft.range(10_000, partitions=8).into_partitions(num_partitions)
        assert df.with_column("plus_one", col("id") + 1).count_rows() == 10_000
        assert df.agg(col("id").count().alias("n")).to_pydict() == {"n": [10_000]}
        assert df.groupby(col("id") % 4).agg(col("id").count().alias("n")).count_rows() == 4
