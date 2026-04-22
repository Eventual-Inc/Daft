from __future__ import annotations

import io
import random
import tempfile
from collections.abc import Callable
from contextlib import contextmanager
from functools import partial

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft.io._generator import read_generator
from daft.recordbatch.recordbatch import RecordBatch
from tests.conftest import get_tests_daft_runner_name


def generate(partition_id: int, num_rows: int, bytes_per_row: int):
    data = {
        "ids": np.arange(num_rows) + partition_id * num_rows,
        "ints": np.random.randint(0, num_rows, num_rows, dtype=np.uint64),
        "bytes": pa.array(
            [np.random.bytes(bytes_per_row) for _ in range(num_rows)],
            type=pa.binary(bytes_per_row),
        ),
    }
    yield RecordBatch.from_pydict(data)


def generator(
    num_partitions: int,
    num_rows_fn: Callable[[], int],
    bytes_per_row_fn: Callable[[], int],
):
    for partition_id in range(num_partitions):
        num_rows = num_rows_fn()
        bytes_per_row = bytes_per_row_fn()
        yield partial(generate, partition_id, num_rows, bytes_per_row)


@pytest.fixture(scope="function")
def pre_shuffle_merge_ctx():
    """Fixture that provides a context manager for pre-shuffle merge testing."""

    def _ctx(threshold: int | None = None):
        return daft.execution_config_ctx(shuffle_algorithm="pre_shuffle_merge", pre_shuffle_merge_threshold=threshold)

    return _ctx


@pytest.fixture(scope="function")
def flight_shuffle_ctx():
    """Fixture that provides a context manager for flight shuffle testing with a temporary directory."""

    @contextmanager
    def _ctx():
        # Create a temporary directory that automatically cleans up
        with tempfile.TemporaryDirectory() as temp_dir:
            # Use the temporary directory for flight shuffle
            with daft.execution_config_ctx(shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[temp_dir]) as ctx:
                yield ctx

    return _ctx


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_pre_shuffle_merge_small_partitions(pre_shuffle_merge_ctx, input_partitions, output_partitions):
    """Test that pre-shuffle merge is working for small partitions less than the memory threshold."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return 1

    threshold = None

    with pre_shuffle_merge_ctx(threshold):
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ids", daft.DataType.uint64()),
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.binary()),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_pre_shuffle_merge_big_partitions(pre_shuffle_merge_ctx, input_partitions, output_partitions):
    """Test that pre-shuffle merge is working for big partitions greater than the threshold."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return 200

    threshold = 1

    with pre_shuffle_merge_ctx(threshold):
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ids", daft.DataType.uint64()),
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.binary()),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_pre_shuffle_merge_randomly_sized_partitions(pre_shuffle_merge_ctx, input_partitions, output_partitions):
    """Test that pre-shuffle merge is working for randomly sized partitions."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return random.randint(1, output_partitions // 2 + 1)

    # We want some partitions that are small, and some that are big. We want to cap the big ones to be around half of the threshold.
    threshold = output_partitions * (8 + output_partitions)

    with pre_shuffle_merge_ctx(threshold):
        df = (
            read_generator(
                generator(input_partitions, num_rows_fn, bytes_per_row_fn),
                schema=daft.Schema._from_field_name_and_types(
                    [
                        ("ids", daft.DataType.uint64()),
                        ("ints", daft.DataType.uint64()),
                        ("bytes", daft.DataType.binary()),
                    ]
                ),
            )
            .repartition(output_partitions, "ints")
            .collect()
        )
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_random_shuffle_uses_ray_shuffle_path_under_flight_shuffle_config(flight_shuffle_ctx):
    with flight_shuffle_ctx():
        df = daft.from_pydict({"id": list(range(32))}).repartition(4, "id")
        shuffled = df.shuffle(seed=0).to_pydict()["id"]

    assert sorted(shuffled) == list(range(32))
    assert shuffled != list(range(32))


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(100, 100), (100, 1), (100, 50), (100, 200)],
)
def test_flight_shuffle(flight_shuffle_ctx, input_partitions, output_partitions):
    """Test that flight shuffle is working."""

    def num_rows_fn():
        return output_partitions

    def bytes_per_row_fn():
        return 200

    with flight_shuffle_ctx():
        base_df = read_generator(
            generator(input_partitions, num_rows_fn, bytes_per_row_fn),
            schema=daft.Schema._from_field_name_and_types(
                [
                    ("ids", daft.DataType.uint64()),
                    ("ints", daft.DataType.uint64()),
                    ("bytes", daft.DataType.fixed_size_binary(200)),
                ]
            ),
        ).collect()

        df = base_df.repartition(output_partitions, "ints").collect()

        assert base_df.to_arrow().sort_by("ids") == df.to_arrow().sort_by("ids")
        assert len(df) == input_partitions * output_partitions


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    # Equal, coalesce (N > M), split (N < M), and single-output coalesce.
    [(4, 4), (8, 2), (2, 8), (7, 1)],
)
def test_flight_into_partitions(flight_shuffle_ctx, input_partitions, output_partitions):
    """Exercises IntoPartitionsNode under `shuffle_algorithm="flight_shuffle"`.

    Verifies that both upstream materialized outputs (FlightPartitionRefs) are
    consumed and downstream outputs are re-emitted as FlightPartitionRefs, across
    the equal / coalesce / split branches.
    """
    rows = 1000
    with flight_shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .into_partitions(output_partitions)
        )

        # Plan-shape assertion: a regression that routes around IntoPartitionsNode
        # or silently falls back to Ray would still yield correct row counts, so
        # verify the Flight variant is in the plan explicitly.
        buf = io.StringIO()
        df.explain(show_all=True, file=buf)
        plan = buf.getvalue()
        assert "IntoPartitions(Flight)" in plan, f"expected IntoPartitions(Flight) in plan:\n{plan}"
        assert "IntoPartitions(Ray)" not in plan, f"unexpected Ray IntoPartitions in plan:\n{plan}"

        collected = df.collect()
        # The core contract of into_partitions: exactly N output partitions.
        assert len(list(collected.iter_partitions())) == output_partitions
        assert sorted(collected.to_pydict()["x"]) == list(range(rows))


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoPartitions tests require the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(4, 4), (8, 2), (2, 8), (7, 1)],
)
def test_ray_into_partitions(input_partitions, output_partitions):
    """IntoPartitions over the Ray backend (no flight_shuffle_ctx).

    Mirrors test_flight_into_partitions but exercises the Ray IntoPartitionsNode
    path and RayIntoPartitionsState finalize.
    """
    rows = 1000
    df = daft.from_pydict({"x": list(range(rows))}).into_partitions(input_partitions).into_partitions(output_partitions)

    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    plan = buf.getvalue()
    assert "IntoPartitions(Ray)" in plan, f"expected IntoPartitions(Ray) in plan:\n{plan}"
    assert "IntoPartitions(Flight)" not in plan, f"unexpected Flight IntoPartitions in plan:\n{plan}"

    collected = df.collect()
    assert len(list(collected.iter_partitions())) == output_partitions
    assert sorted(collected.to_pydict()["x"]) == list(range(rows))


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_into_partitions_mixed_empty_inputs(flight_shuffle_ctx):
    """IntoPartitions (Flight) must still emit exactly N output partitions when some pre-filter partitions are empty.

    FlightIntoPartitionsState opens N caches in `make_state` and `push` short-
    circuits on empty inputs. As long as the rotation distributes surviving rows
    across every cache, close() succeeds and we get N FlightPartitionRefs.
    """
    rows = 1000
    input_partitions = 8
    output_partitions = 5
    filter_threshold = 500  # roughly half the data survives
    with flight_shuffle_ctx():
        partial = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .filter(daft.col("x") < filter_threshold)
            .into_partitions(output_partitions)
            .collect()
        )
        assert len(list(partial.iter_partitions())) == output_partitions
        assert sorted(partial.to_pydict()["x"]) == list(range(filter_threshold))


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize("input_partitions", [1, 8, 32])
def test_flight_gather(flight_shuffle_ctx, input_partitions):
    """Gather is triggered by top_n and ungrouped agg on multi-partition inputs.

    Under `shuffle_algorithm="flight_shuffle"` this exercises the GatherWrite
    blocking sink → ShuffleRead path (rather than the in-memory-scan shortcut).
    """
    rows = 1000
    with flight_shuffle_ctx():
        df = daft.from_pydict({"x": list(range(rows))}).into_partitions(input_partitions)

        agg_df = df.sum("x")
        top_df = df.sort("x", desc=True).limit(5)

        # Plan-shape assertion: a regression that routes around GatherNode (or
        # trips the num_partitions==1 short-circuit when it shouldn't) would
        # still produce correct values, so verify the plan explicitly.
        for plan_df in (agg_df, top_df):
            buf = io.StringIO()
            plan_df.explain(show_all=True, file=buf)
            plan = buf.getvalue()
            if input_partitions > 1:
                assert "FlightGather" in plan, f"expected FlightGather in plan:\n{plan}"
            else:
                assert "Gather" not in plan, f"unexpected Gather for single-partition input:\n{plan}"

        # UnGroupedAggregate → gather
        total = agg_df.collect().to_pydict()["x"]
        assert total == [sum(range(rows))]

        # TopN → gather
        top = top_df.collect().to_pydict()["x"]
        assert top == [rows - 1, rows - 2, rows - 3, rows - 4, rows - 5]


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed gather tests require the ray runner",
)
@pytest.mark.parametrize("input_partitions", [1, 8, 32])
def test_ray_gather(input_partitions):
    """Gather over the Ray backend (no flight_shuffle_ctx).

    Mirrors test_flight_gather but exercises GatherSink::Ray → emit_read_tasks.
    """
    rows = 1000
    df = daft.from_pydict({"x": list(range(rows))}).into_partitions(input_partitions)

    agg_df = df.sum("x")
    top_df = df.sort("x", desc=True).limit(5)

    for plan_df in (agg_df, top_df):
        buf = io.StringIO()
        plan_df.explain(show_all=True, file=buf)
        plan = buf.getvalue()
        if input_partitions > 1:
            assert "Gather" in plan, f"expected Gather in plan:\n{plan}"
            assert "FlightGather" not in plan, f"expected Ray gather, got Flight:\n{plan}"
        else:
            assert "Gather" not in plan, f"unexpected Gather for single-partition input:\n{plan}"

    total = agg_df.collect().to_pydict()["x"]
    assert total == [sum(range(rows))]

    top = top_df.collect().to_pydict()["x"]
    assert top == [rows - 1, rows - 2, rows - 3, rows - 4, rows - 5]


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_gather_mixed_empty_inputs(flight_shuffle_ctx):
    """Gather handles a mix of empty and non-empty input partitions.

    Exercises the FlightGatherState empty-input short-circuit: some input
    partitions have rows after the filter, others don't. Regressions in
    the short-circuit would either miscount refs or skip data.

    Note: the all-empty case (every partition filtered away) hangs in the
    downstream ShuffleRead today and is intentionally not covered here.
    """
    rows = 1000
    input_partitions = 8
    filter_threshold = 500  # roughly half the data survives
    with flight_shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .filter(daft.col("x") < filter_threshold)
        )

        expected_rows = [v for v in range(rows) if v < filter_threshold]

        total = df.sum("x").collect().to_pydict()["x"]
        assert total == [sum(expected_rows)]

        top = df.sort("x", desc=True).limit(5).collect().to_pydict()["x"]
        assert top == sorted(expected_rows, reverse=True)[:5]
