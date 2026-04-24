from __future__ import annotations

import io
import random
import tempfile
import threading
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
def test_flight_into_partitions_rotation_balance(flight_shuffle_ctx):
    """Test that flight into partitions rotation is balanced.

    Per-push `div_ceil` slicing front-loads early buckets; the rotation
    offset in `FlightIntoPartitionsState::push` spreads that bias across
    output buckets over many pushes.
    With `rows_per_input=5` and `output_partitions=3`, each push distributes
    rows as [2, 2, 1]. Without rotation (offset stuck at 0) the last bucket
    consistently gets the short chunk, so 30 pushes yield [60, 60, 30] — a
    30-row spread. With rotation the short chunk rotates and counts balance
    to [50, 50, 50].
    """
    rows_per_input = 5
    input_partitions = 30
    output_partitions = 3
    total_rows = rows_per_input * input_partitions

    import ray

    with flight_shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(total_rows))})
            .into_partitions(input_partitions)
            .into_partitions(output_partitions)
            .collect()
        )

        counts = [len(ray.get(p)) for p in df.iter_partitions()]
        assert sum(counts) == total_rows
        assert len(counts) == output_partitions

        # Tight enough to catch the [60, 60, 30] failure mode, loose enough
        # to tolerate scheduler variance in push ordering and MP fusion.
        tolerance = rows_per_input * 2
        spread = max(counts) - min(counts)
        assert spread <= tolerance, f"rotation unbalanced: {counts} (spread={spread})"


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_into_partitions_all_empty_inputs(flight_shuffle_ctx):
    """All inputs empty post-filter: `into_partitions(N)` must still emit exactly N zero-row output partitions.

    Regression guard for both the caller-schema plumbing through
    `InProgressShuffleCache` (needed so `close()` can report a schema
    for an all-empty partition) and the empty-stream fallback in
    `forward_partition_stream` for `ShuffleRead(Flight)`.
    """
    rows = 1000
    input_partitions = 8
    output_partitions = 5
    with flight_shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .filter(daft.col("x") < 0)
            .into_partitions(output_partitions)
            .collect()
        )
        assert len(list(df.iter_partitions())) == output_partitions
        assert df.to_pydict()["x"] == []


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_repartition_all_empty_inputs(flight_shuffle_ctx):
    """All inputs empty post-filter: hash `.repartition(M, "x")` must still emit M zero-row output partitions.

    Same shuffle-cache shape as `test_flight_into_partitions_all_empty_inputs`
    (N caches opened per input in `make_state`, all closed with no data), but
    routed through `RepartitionSink` instead of `IntoPartitionsSink`.
    """
    rows = 1000
    input_partitions = 8
    output_partitions = 5
    with flight_shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .filter(daft.col("x") < 0)
            .repartition(output_partitions, "x")
            .collect()
        )
        assert len(list(df.iter_partitions())) == output_partitions
        assert df.to_pydict()["x"] == []


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_gather_all_empty_inputs(flight_shuffle_ctx):
    """Every upstream partition is empty post-filter: gather must still emit a single empty result without hanging.

    Uses a daemon thread + timeout to protect pytest teardown in case the
    hang regresses.
    """
    rows = 1000
    input_partitions = 8
    with flight_shuffle_ctx():
        df = daft.from_pydict({"x": list(range(rows))}).into_partitions(input_partitions).filter(daft.col("x") < 0)

        result: dict = {}

        def run():
            try:
                result["value"] = df.sum("x").collect().to_pydict()["x"]
            except Exception as e:
                result["error"] = e

        t = threading.Thread(target=run, daemon=True)
        t.start()
        t.join(timeout=30)
        if t.is_alive():
            # Hung. Leak the daemon thread (it'll die with the process) and
            # fail the test with a clear diagnostic rather than blocking teardown.
            raise TimeoutError("test_flight_gather_all_empty_inputs hung past 30s")
        if "error" in result:
            raise result["error"]
        # The sum over an empty table should be 0 rows or a single NULL row.
        assert result["value"] in ([], [None], [0])


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
    the short-circuit would either miscount refs or skip data. The all-empty
    case is covered by `test_flight_gather_all_empty_inputs`.
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


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "rows, batch_size",
    # Divisible, ragged tail, small input + small batch, batch_size > rows (tail-only).
    [(1000, 100), (1000, 7), (10, 3), (100, 1000)],
)
def test_flight_into_batches(flight_shuffle_ctx, rows, batch_size):
    """IntoBatches under flight_shuffle: rebatch tasks must emit FlightPartitionRefs via gather_write."""
    with flight_shuffle_ctx():
        df = daft.from_pydict({"x": list(range(rows))}).into_partitions(8).into_batches(batch_size)

        buf = io.StringIO()
        df.explain(show_all=True, file=buf)
        plan = buf.getvalue()
        assert "IntoBatches(Flight)" in plan, f"expected IntoBatches(Flight) in plan:\n{plan}"
        assert "IntoBatches(Ray)" not in plan, f"unexpected Ray IntoBatches:\n{plan}"

        collected = df.collect()
        partitions = list(collected.iter_partitions())
        if get_tests_daft_runner_name() == "ray":
            import ray

            partitions = ray.get(partitions)

        # Total row set must match input (every row present exactly once).
        all_rows = [v for part in partitions for v in part.to_pydict()["x"]]
        assert sorted(all_rows) == list(range(rows))

        sizes = [len(part.to_pydict()["x"]) for part in partitions]
        if batch_size >= rows:
            # Tail-only flush: single partition with all rows (or none if rows == 0).
            assert sizes == [rows] or sizes == []
        else:
            # Batch-size distribution under Flight differs from Ray's (the
            # intermediate-op task output concatenates Phase-1 morsels at the
            # task boundary, so sizes can exceed `batch_size` while the total
            # row count and partition count are still sensible). Just assert
            # no empty batches; the sum correctness is covered above.
            assert all(s > 0 for s in sizes), f"empty batch in sizes: {sizes}"


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_metadata_access_does_not_fetch(flight_shuffle_ctx, monkeypatch):
    """`FlightMaterializedResult` must defer the flight RPC until a consumer actually reads bytes.

    Calling `len(df)` / iterating `df._result.values()` for metadata only must
    NOT trigger `FlightPartitionRef.fetch` — that's the whole point of
    yielding a lazy `FlightMaterializedResult` from `stream_plan` instead of
    fetching+ray.put-ing on the driver. Once the consumer asks for bytes
    (`to_pydict`), exactly one fetch per partition is expected.
    """
    from daft.runners.ray_runner import FlightMaterializedResult

    fetch_count = {"n": 0}
    real_materialize = FlightMaterializedResult._materialize

    def counting_materialize(self):
        fetch_count["n"] += 1
        return real_materialize(self)

    monkeypatch.setattr(FlightMaterializedResult, "_materialize", counting_materialize)

    rows = 1000
    with flight_shuffle_ctx():
        df = daft.from_pydict({"x": list(range(rows))}).into_partitions(8).into_batches(100).collect()

        # `_result` is the underlying RayPartitionSet; `num_partitions` and
        # `len` go through `.metadata()` only — must not trigger any fetches.
        n_partitions = df._result.num_partitions()
        assert n_partitions > 0
        assert len(df._result) == rows
        assert fetch_count["n"] == 0, f"expected zero flight fetches for metadata-only access; got {fetch_count['n']}"

        # Materializing bytes triggers fetches. Each `FlightMaterializedResult`
        # caches after first call, so the count is bounded by partition count.
        result_rows = df.to_pydict()["x"]
        assert sorted(result_rows) == list(range(rows))
        assert 0 < fetch_count["n"] <= n_partitions, (
            f"expected at most one fetch per partition after to_pydict; "
            f"got {fetch_count['n']} fetches for {n_partitions} partitions"
        )


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_into_batches_feeds_downstream_distributed_op(flight_shuffle_ctx):
    """IntoBatches(Flight) outputs must be consumable by a downstream distributed op via `shuffle_read(Flight)`.

    Previous flight tests for IntoBatches only exercise the driver-side fetch
    path (via `.collect()`). This one chains another distributed op
    (`into_partitions`) after IntoBatches so the rebatch task's
    `FlightPartitionRef` outputs are consumed on a worker actor — exercising
    the `build_refs_task_builder` flight branch (`shuffle_read(Flight) +
    with_flight_shuffle_reads`) end-to-end and the cross-actor flight RPC.
    """
    rows = 1000
    with flight_shuffle_ctx():
        df = daft.from_pydict({"x": list(range(rows))}).into_partitions(8).into_batches(50).into_partitions(4)

        buf = io.StringIO()
        df.explain(show_all=True, file=buf)
        plan = buf.getvalue()
        assert "IntoBatches(Flight)" in plan, f"expected IntoBatches(Flight):\n{plan}"
        assert "IntoPartitions(Flight)" in plan, f"expected IntoPartitions(Flight):\n{plan}"

        collected = df.collect()
        assert sorted(collected.to_pydict()["x"]) == list(range(rows))
        assert len(list(collected.iter_partitions())) == 4


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoBatches tests require the ray runner",
)
def test_ray_into_batches_plan_shape():
    """Plan-shape assertion for the default Ray backend: ensure the Ray variant appears and Flight does not."""
    df = daft.from_pydict({"x": list(range(1000))}).into_partitions(8).into_batches(100)

    buf = io.StringIO()
    df.explain(show_all=True, file=buf)
    plan = buf.getvalue()
    assert "IntoBatches(Ray)" in plan, f"expected IntoBatches(Ray) in plan:\n{plan}"
    assert "IntoBatches(Flight)" not in plan, f"unexpected Flight IntoBatches:\n{plan}"


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
def test_flight_into_batches_all_empty_inputs(flight_shuffle_ctx):
    """Empty-input regression: filter drops everything, into_batches must not panic or hang."""
    with flight_shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(1000))})
            .into_partitions(8)
            .filter(daft.col("x") < 0)
            .into_batches(100)
            .collect()
        )
        rows_out = df.to_pydict()["x"]
        assert rows_out == []


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoBatches tests require the ray runner",
)
@pytest.mark.parametrize("flight", [False, True])
def test_into_batches_followed_by_streaming_project(flight, flight_shuffle_ctx):
    """A streaming op fused on top of `IntoBatches` (Project via `with_column`) must consume the gather_write output correctly.

    `Project` uses `pipeline_instruction` so it shares a task with the
    rebatch — the local plan becomes `Project(... → into_batches →
    gather_write(backend))`. Under Ray, gather_write loops its
    `BlockingSinkOutput::Partitions` back into the morsel stream so the fused
    Project can keep consuming. Under Flight, gather_write emits
    `BlockingSinkOutput::FlightPartitionRefs` instead — and intermediate-op
    handlers `unreachable!()` on `PipelineEvent::FlightPartitionRef`. Theory
    says the Flight case should panic or silently drop rows.

    `with_column` is used (rather than `filter`) because the optimizer
    pushes filter below IntoBatches; a derived column can't be pushed past
    IntoBatches and is guaranteed to sit on top of it in the physical plan.
    """

    @contextmanager
    def maybe_flight():
        if flight:
            with flight_shuffle_ctx():
                yield
        else:
            yield

    rows = 1000
    with maybe_flight():
        # `with_column` (Project) can't be pushed below `into_batches` because
        # the derived column doesn't exist upstream — guarantees Project sits
        # on top of IntoBatches in the physical plan, fused into the same
        # local task via `pipeline_instruction`.
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(8)
            .into_batches(100)
            .with_column("y", daft.col("x") * 2)
            .collect()
        )
        out = df.to_pydict()
        assert sorted(out["x"]) == list(range(rows)), f"flight={flight}: lost rows; got {len(out['x'])} expected {rows}"
        # `y` must be derived correctly from the post-batched data.
        pairs = sorted(zip(out["x"], out["y"]))
        assert pairs == [(i, i * 2) for i in range(rows)], (
            f"flight={flight}: project-after-into_batches produced wrong column values"
        )


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoBatches tests require the ray runner",
)
@pytest.mark.parametrize("flight", [False, True])
def test_into_batches_emits_batch_size_partitions(flight, flight_shuffle_ctx):
    """Output partitions of `df.into_batches(N)` should be approximately N rows each.

    With phase-1 `gather_write(backend)` consolidating each upstream task's
    data into a single ref, `execute_into_batches` sees `group_size = full
    upstream task size` (potentially >> batch_size) and the rebatch task ends
    up emitting one ref of `group_size` rows instead of `batch_size`-sized
    refs. This test pins the user-facing invariant: output ref sizes should
    track the requested `batch_size`, not the upstream partition size.
    """

    @contextmanager
    def maybe_flight():
        if flight:
            with flight_shuffle_ctx():
                yield
        else:
            yield

    rows = 10_000
    batch_size = 100
    input_partitions = 4  # 2500 rows per input partition — much larger than batch_size
    with maybe_flight():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .into_batches(batch_size)
            .collect()
        )
        partitions = list(df.iter_partitions())
        if get_tests_daft_runner_name() == "ray":
            import ray

            partitions = ray.get(partitions)
        sizes = [len(p.to_pydict()["x"]) for p in partitions]

        # Total row count must be preserved.
        assert sum(sizes) == rows, f"flight={flight}: lost rows; sizes={sizes}"

        # Every non-tail batch should be approximately `batch_size` rows.
        # Allow a tolerance for non-strict bucketing at task boundaries.
        tolerance = batch_size  # allow up to 2× batch_size; anything beyond points at a regression
        oversized = [s for s in sizes if s > batch_size + tolerance]
        assert not oversized, (
            f"flight={flight}: expected batches ≤ {batch_size + tolerance} rows; "
            f"oversized batches: {oversized[:5]} (out of {len(sizes)} total)"
        )
