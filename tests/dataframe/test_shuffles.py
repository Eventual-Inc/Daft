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


@pytest.fixture(params=[False, True], ids=["ray", "flight"])
def flight(request) -> bool:
    """Parametrizes a test on `flight=False` (Ray backend) and `flight=True` (Flight backend).

    Tests that depend on this fixture run twice — once per backend — without
    needing a per-test `@pytest.mark.parametrize("flight", ...)` decorator.
    """
    return request.param


@pytest.fixture
def shuffle_ctx(flight, flight_shuffle_ctx):
    """Context-manager fixture that enters `flight_shuffle_ctx` when flight=True, else no-op.

    Pair with the `flight` fixture to write tests that exercise both shuffle
    backends with `with shuffle_ctx(): ...`.
    """

    @contextmanager
    def _ctx():
        if flight:
            with flight_shuffle_ctx():
                yield
        else:
            yield

    return _ctx


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle tests are meant for the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    [(10, 10), (10, 1), (10, 5), (10, 20)],
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
    [(10, 10), (10, 1), (10, 5), (10, 20)],
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
    [(10, 10), (10, 1), (10, 5), (10, 20)],
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
    [(10, 10), (10, 1), (10, 5), (10, 20)],
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
    reason="distributed IntoPartitions tests require the ray runner",
)
@pytest.mark.parametrize(
    "input_partitions, output_partitions",
    # Equal, coalesce (N > M), split (N < M), and single-output coalesce.
    [(4, 4), (8, 2), (2, 8), (7, 1)],
)
def test_into_partitions(flight, shuffle_ctx, input_partitions, output_partitions):
    """Exercises IntoPartitionsNode across the equal / coalesce / split branches under both backends."""
    rows = 1000
    with shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(input_partitions)
            .into_partitions(output_partitions)
        )

        # Plan-shape assertion guards against routing regressions that would
        # still yield correct row counts (e.g. silent fallback to the wrong backend).
        buf = io.StringIO()
        df.explain(show_all=True, file=buf)
        plan = buf.getvalue()
        expected, other = ("Flight", "Ray") if flight else ("Ray", "Flight")
        assert f"IntoPartitions({expected})" in plan, f"expected IntoPartitions({expected}) in plan:\n{plan}"
        assert f"IntoPartitions({other})" not in plan, f"unexpected IntoPartitions({other}) in plan:\n{plan}"

        collected = df.collect()
        # The core contract of into_partitions: exactly N output partitions.
        assert len(list(collected.iter_partitions())) == output_partitions
        assert sorted(collected.to_pydict()["x"]) == list(range(rows))


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoPartitions tests require the ray runner",
)
def test_into_partitions_mixed_empty_inputs(shuffle_ctx):
    """N output partitions when some pre-filter inputs are empty."""
    rows = 1000
    input_partitions = 8
    output_partitions = 5
    filter_threshold = 500  # roughly half the data survives
    with shuffle_ctx():
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
    """Flight rotation evenly distributes ragged per-push chunks across output buckets.

    Without rotation, [2, 2, 1] chunks would consistently short the last
    bucket: 30 pushes yield [60, 60, 30]. With rotation: ~[50, 50, 50].
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
    reason="distributed IntoPartitions tests require the ray runner",
)
def test_into_partitions_all_empty_inputs(shuffle_ctx):
    """N zero-row output partitions when all pre-filter inputs are empty."""
    rows = 1000
    input_partitions = 8
    output_partitions = 5
    with shuffle_ctx():
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
    reason="distributed shuffle tests require the ray runner",
)
def test_repartition_all_empty_inputs(shuffle_ctx):
    """Hash `.repartition(M, "x")` emits M zero-row output partitions when all inputs are empty."""
    rows = 1000
    input_partitions = 8
    output_partitions = 5
    with shuffle_ctx():
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
    reason="distributed gather tests require the ray runner",
)
def test_gather_all_empty_inputs(shuffle_ctx):
    """Gather over all-empty inputs emits a single empty result without hanging.

    Daemon thread + 30s timeout guards pytest teardown if the hang regresses.
    """
    rows = 1000
    input_partitions = 8
    with shuffle_ctx():
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
            raise TimeoutError("test_gather_all_empty_inputs hung past 30s")
        if "error" in result:
            raise result["error"]
        # The sum over an empty table should be 0 rows or a single NULL row.
        assert result["value"] in ([], [None], [0])


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed gather tests require the ray runner",
)
@pytest.mark.parametrize("input_partitions", [1, 8, 32])
def test_gather(flight, shuffle_ctx, input_partitions):
    """`top_n` and ungrouped agg correctness across both backends; single-partition inputs short-circuit Gather entirely."""
    rows = 1000
    with shuffle_ctx():
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
                if flight:
                    assert "FlightGather" in plan, f"expected FlightGather in plan:\n{plan}"
                else:
                    assert "Gather" in plan, f"expected Gather in plan:\n{plan}"
                    assert "FlightGather" not in plan, f"expected Ray gather, got Flight:\n{plan}"
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
def test_gather_mixed_empty_inputs(shuffle_ctx):
    """Gather handles a mix of empty and non-empty input partitions (all-empty covered by `test_gather_all_empty_inputs`)."""
    rows = 1000
    input_partitions = 8
    filter_threshold = 500  # roughly half the data survives
    with shuffle_ctx():
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
    reason="distributed IntoBatches tests require the ray runner",
)
@pytest.mark.parametrize(
    "rows, batch_size",
    # Divisible, ragged tail, small input + small batch, batch_size > rows (tail-only).
    # Scales kept small: each Flight rebatch task creates a shuffle-cache dir
    # on disk, and many tiny dirs make filesystem ops dominate the test time.
    [(100, 10), (100, 7), (10, 3), (50, 100)],
)
def test_into_batches(flight, shuffle_ctx, rows, batch_size):
    """IntoBatches under both backends: rebatch tasks must produce the right row set and partition shape."""
    with shuffle_ctx():
        df = daft.from_pydict({"x": list(range(rows))}).into_partitions(8).into_batches(batch_size)

        buf = io.StringIO()
        df.explain(show_all=True, file=buf)
        plan = buf.getvalue()
        expected, other = ("Flight", "Ray") if flight else ("Ray", "Flight")
        assert f"IntoBatches({expected})" in plan, f"expected IntoBatches({expected}) in plan:\n{plan}"
        assert f"IntoBatches({other})" not in plan, f"unexpected IntoBatches({other}) in plan:\n{plan}"

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
            # Sizes can exceed batch_size under Flight (task-boundary concat);
            # row-set correctness is covered above, just rule out empty batches.
            assert all(s > 0 for s in sizes), f"empty batch in sizes: {sizes}"


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoBatches tests require the ray runner",
)
def test_into_batches_all_empty_inputs(shuffle_ctx):
    """Ensures `into_batches` handles all-empty inputs (post-filter) without panicking or hanging."""
    with shuffle_ctx():
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
    reason="distributed IntoPartitions tests require the ray runner",
)
def test_into_partitions_coalesce_with_fused_downstream(shuffle_ctx):
    """Ensures the `into_partitions` Coalesce path re-reads its materialized partition refs into a fused downstream's morsel stream, so a fused Project sees real row values rather than the refs themselves.

    Setup: 8→2 forces Coalesce.
    """
    rows = 100
    with shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(8)
            .into_partitions(2)
            .with_column("y", daft.col("x") * 2)
            .collect()
        )
        out = df.to_pydict()
        assert sorted(out["x"]) == list(range(rows)), f"lost rows; got {len(out['x'])} expected {rows}"
        assert sorted(zip(out["x"], out["y"])) == [(i, i * 2) for i in range(rows)], (
            "fused Project after coalesce produced wrong column values"
        )


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoPartitions tests require the ray runner",
)
def test_into_partitions_equal_split_and_merge_with_fused_downstream(shuffle_ctx):
    """Ensures the `into_partitions` Equal+`enable_scan_task_split_and_merge=True` path re-reads its materialized partition refs into a fused downstream's morsel stream, mirroring the Coalesce variant.

    Setup: N→N with split-and-merge forces the Equal path.
    """
    rows = 100
    n = 4
    with shuffle_ctx(), daft.execution_config_ctx(enable_scan_task_split_and_merge=True):
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(n)
            .into_partitions(n)  # Equal path — exercises the partition-ref re-read.
            .with_column("y", daft.col("x") * 2)
            .collect()
        )
        out = df.to_pydict()
        assert sorted(out["x"]) == list(range(rows)), f"lost rows; got {len(out['x'])} expected {rows}"
        assert sorted(zip(out["x"], out["y"])) == [(i, i * 2) for i in range(rows)], (
            "fused Project after Equal+split-merge produced wrong column values"
        )


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoBatches tests require the ray runner",
)
def test_into_batches_followed_by_streaming_project(shuffle_ctx):
    """Ensures the `into_batches` rebatch step re-reads its materialized partition refs into a fused downstream's morsel stream, so a fused Project sees real row values rather than the refs themselves.

    `with_column` is used over `filter` because the optimizer can't push a
    derived column below IntoBatches.
    """
    rows = 1000
    with shuffle_ctx():
        df = (
            daft.from_pydict({"x": list(range(rows))})
            .into_partitions(8)
            .into_batches(100)
            .with_column("y", daft.col("x") * 2)
            .collect()
        )
        out = df.to_pydict()
        assert sorted(out["x"]) == list(range(rows)), f"lost rows; got {len(out['x'])} expected {rows}"
        # `y` must be derived correctly from the post-batched data.
        pairs = sorted(zip(out["x"], out["y"]))
        assert pairs == [(i, i * 2) for i in range(rows)], "project-after-into_batches produced wrong column values"


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="distributed IntoBatches tests require the ray runner",
)
def test_into_batches_emits_batch_size_partitions(shuffle_ctx):
    """Ensures `into_batches` emits partitions sized to the requested `batch_size`, not to the upstream partition — each upstream task must split its rows across multiple refs rather than consolidating them into one ref sized to the upstream partition."""
    rows = 1000
    batch_size = 100
    input_partitions = 2  # 500 rows per input partition — still > batch_size, exercises the "input partition larger than batch_size" path
    with shuffle_ctx():
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
        assert sum(sizes) == rows, f"lost rows; sizes={sizes}"

        # Every non-tail batch should be approximately `batch_size` rows.
        # Allow a tolerance for non-strict bucketing at task boundaries.
        tolerance = batch_size  # allow up to 2× batch_size; anything beyond points at a regression
        oversized = [s for s in sizes if s > batch_size + tolerance]
        assert not oversized, (
            f"expected batches ≤ {batch_size + tolerance} rows; "
            f"oversized batches: {oversized[:5]} (out of {len(sizes)} total)"
        )
