"""Tests for the UDF process pool functionality."""

from __future__ import annotations

import os
import time

import pytest

import daft
from daft import DataType, col
from daft.daft import get_process_pool_stats, reset_process_pool_total_created
from tests.conftest import get_tests_daft_runner_name


@pytest.fixture
def reset_process_pool_total_created_fixture():
    reset_process_pool_total_created()
    yield


def check_process_pool_stats(expected_active_workers: int = 0, expected_total_workers: int | None = None):
    """Check process pool stats only if running with native runner."""
    if get_tests_daft_runner_name() != "native":
        return

    active_workers, total_workers = get_process_pool_stats()
    assert (
        active_workers == expected_active_workers
    ), f"Should have {expected_active_workers} active workers, got {active_workers}"
    if expected_total_workers is not None:
        assert (
            total_workers == expected_total_workers
        ), f"Should have {expected_total_workers} total workers, got {total_workers}"


def test_process_pool_scales_to_num_cores(reset_process_pool_total_created_fixture):
    num_cores = os.cpu_count()

    @daft.func.batch(return_dtype=DataType.int64(), batch_size=1, use_process=True)
    def get_worker_pid(x: daft.Series) -> daft.Series:
        return [os.getpid()]

    df = daft.from_pydict({"a": list(range(num_cores * num_cores))})
    result = df.select(get_worker_pid(df["a"])).to_pydict()
    worker_pids = set(result["a"])

    assert len(worker_pids) == num_cores, f"Should have used {num_cores} worker processes, got {len(worker_pids)}"

    check_process_pool_stats(expected_total_workers=num_cores)


def test_process_udfs_share_the_same_pool(reset_process_pool_total_created_fixture):
    num_cores = os.cpu_count()

    @daft.func.batch(return_dtype=DataType.int64(), batch_size=1, use_process=True)
    def get_worker_pid(x: daft.Series) -> daft.Series:
        return [os.getpid()]

    df = daft.from_pydict({"a": list(range(num_cores * num_cores))})
    result = (
        df.with_column("worker_pid_1", get_worker_pid(df["a"]))
        .with_column("worker_pid_2", get_worker_pid(df["a"]))
        .to_pydict()
    )
    worker_pids_1 = set(result["worker_pid_1"])
    worker_pids_2 = set(result["worker_pid_2"])

    assert len(worker_pids_1) == num_cores, f"Should have used {num_cores} worker processes, got {len(worker_pids_1)}"
    assert len(worker_pids_2) == num_cores, f"Should have used {num_cores} worker processes, got {len(worker_pids_2)}"
    assert worker_pids_1 == worker_pids_2, "All UDFs should share the same worker process"

    check_process_pool_stats(expected_total_workers=num_cores)


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="Concurrent queries may start multiple different worker processes on ray runner",
)
def test_concurrent_queries_share_the_same_pool(
    reset_process_pool_total_created_fixture,
):
    num_cores = os.cpu_count()

    @daft.func.batch(return_dtype=DataType.int64(), batch_size=1, use_process=True)
    def get_worker_pid(x: daft.Series) -> daft.Series:
        return [os.getpid()]

    df1 = daft.from_pydict({"a": list(range(num_cores * num_cores))}).with_column(
        "worker_pid", get_worker_pid(col("a"))
    )
    df2 = daft.from_pydict({"a": list(range(num_cores * num_cores))}).with_column(
        "worker_pid", get_worker_pid(col("a"))
    )

    iters = [df1.__iter__(), df2.__iter__()]
    pids = set()
    while iters:
        iter = iters.pop(0)
        try:
            res = next(iter)
            pids.add(res["worker_pid"])
        except StopIteration:
            continue
        iters.append(iter)

    assert len(pids) == num_cores, f"Should use {num_cores} worker processes, got {len(pids)} distinct PIDs"

    check_process_pool_stats(expected_total_workers=num_cores)


@pytest.mark.parametrize("invocations", [1, os.cpu_count()])
def test_pool_scales_based_on_invocation_count(invocations, reset_process_pool_total_created_fixture):
    @daft.func.batch(return_dtype=DataType.int64(), batch_size=1, use_process=True)
    def get_pid(x: daft.Series) -> daft.Series:
        time.sleep(0.5)  # simulate work to ensure we can scale
        return [os.getpid()]

    df = daft.from_pydict({"a": list(range(invocations))})
    result = df.select(get_pid(df["a"])).to_pydict()
    pids = result["a"]
    distinct_pids = len(set(pids))
    assert distinct_pids == invocations, f"Should use {invocations} processes, got {distinct_pids} distinct PIDs"

    check_process_pool_stats(expected_total_workers=invocations)


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray", reason="Max concurrency uses distributed actors on ray runner"
)
def test_pool_scales_to_udf_with_max_concurrency(
    reset_process_pool_total_created_fixture,
):
    small_concurrency = 1
    large_concurrency = os.cpu_count() - 1

    @daft.cls(max_concurrency=small_concurrency)
    class GetPid1:
        def __init__(self):
            pass

        @daft.method.batch(return_dtype=DataType.int64(), batch_size=1)
        def __call__(self, x: daft.Series) -> daft.Series:
            return [os.getpid()]

    @daft.cls(max_concurrency=large_concurrency)
    class GetPid2:
        def __init__(self):
            pass

        @daft.method.batch(return_dtype=DataType.int64(), batch_size=1)
        def __call__(self, x: daft.Series) -> daft.Series:
            time.sleep(0.5)  # simulate work to ensure we can scale
            return [os.getpid()]

    get_pid1 = GetPid1()
    get_pid2 = GetPid2()
    num_rows = os.cpu_count() * os.cpu_count()
    df = daft.from_pydict({"a": list(range(num_rows)), "b": list(range(num_rows))})

    result = df.select(get_pid1(df["a"]), get_pid2(df["b"])).to_pydict()
    pids1 = result["a"]
    pids2 = result["b"]
    max_distinct_pids = max(len(set(pids1)), len(set(pids2)))

    assert (
        max_distinct_pids == large_concurrency
    ), f"Should use at most highest max_concurrency ({large_concurrency}) processes, got {max_distinct_pids} distinct PIDs"

    check_process_pool_stats(expected_total_workers=large_concurrency)


def test_pool_cleanup_on_failure(reset_process_pool_total_created_fixture):
    """Test that processes are cleaned up after query completion or failure."""

    @daft.func(use_process=True)
    def udf_func(x: int) -> int:
        raise ValueError(f"Intentional failure for value {x}")

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    with pytest.raises(Exception):
        df.select(udf_func(df["a"])).collect()

    check_process_pool_stats(expected_total_workers=1)


def test_segfault_handling(reset_process_pool_total_created_fixture):
    """Test that segfaults are handled gracefully."""
    import ctypes

    @daft.func(use_process=True)
    def segfault_udf(x: int) -> int:
        if x == 3:
            # Trigger segfault
            ctypes.string_at(0)
        return x * 2

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})

    with pytest.raises(Exception) as exc_info:
        df.select(segfault_udf(df["a"])).collect()

    # Should mention segfault in error message
    error_msg = str(exc_info.value)
    assert "SIGSEGV" in error_msg or "segmentation fault" in error_msg.lower()

    # Worker should be cleaned up
    check_process_pool_stats()


def test_sigkill_handling(reset_process_pool_total_created_fixture):
    """Test that SIGKILL is handled gracefully."""
    import signal

    @daft.func(use_process=True)
    def kill_self_udf(x: int) -> int:
        if x == 3:
            os.kill(os.getpid(), signal.SIGKILL)
        return x * 2

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})

    with pytest.raises(Exception) as exc_info:
        df.select(kill_self_udf(df["a"])).collect()

    # Should mention signal in error message
    error_msg = str(exc_info.value)
    assert "SIGKILL" in error_msg or "killed" in error_msg.lower()

    # Worker should be cleaned up
    check_process_pool_stats()


def test_worker_not_recycled_after_death(reset_process_pool_total_created_fixture):
    """Test that dead workers are not returned to the pool."""

    @daft.func(use_process=True)
    def failing_udf(x: int) -> int:
        os._exit(1)

    @daft.func(use_process=True)
    def working_udf(x: int) -> int:
        return x * 2

    df = daft.from_pydict({"a": [1]})

    # First call should fail
    with pytest.raises(Exception):
        df.select(failing_udf(df["a"])).collect()

    # Dead worker should be cleaned up
    check_process_pool_stats()

    # Second call with different UDF should spawn a new worker and succeed
    result = df.select(working_udf(df["a"])).collect()
    assert result.to_pydict()["a"] == [2]


def test_multiple_failures_dont_exhaust_pool(reset_process_pool_total_created_fixture):
    """Test that multiple process deaths don't permanently damage the pool."""

    @daft.func(use_process=True)
    def always_exit_udf(_x: int) -> int:
        os._exit(1)

    df = daft.from_pydict({"a": [1]})

    # Each attempt should fail and clean up
    for _ in range(3):
        with pytest.raises(Exception):
            df.select(always_exit_udf(df["a"])).collect()

        check_process_pool_stats()


def test_concurrent_queries_after_worker_death(reset_process_pool_total_created_fixture):
    """Test that the pool recovers when one query kills its worker."""
    should_exit = [True]

    @daft.func.batch(return_dtype=DataType.int64(), batch_size=1, use_process=True)
    def conditional_exit_udf(x: daft.Series) -> daft.Series:
        if should_exit[0]:
            os._exit(1)
        time.sleep(0.1)  # Simulate work
        return [x[0] * 2]

    df = daft.from_pydict({"a": list(range(10))})

    # First query kills its worker
    with pytest.raises(Exception):
        df.select(conditional_exit_udf(df["a"])).collect()

    check_process_pool_stats()

    # Reset flag for subsequent queries
    should_exit[0] = False

    # Second query should work with new workers
    result = df.select(conditional_exit_udf(df["a"])).collect()
    assert len(result) == 10

    check_process_pool_stats()
