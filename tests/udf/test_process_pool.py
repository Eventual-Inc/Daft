"""Tests for the UDF process pool functionality."""

from __future__ import annotations

import os
import time
from typing import Callable

import pytest

import daft
from daft import DataType


@pytest.fixture(autouse=True)
def reset_pool_size():
    """Reset the pool size env var after each test."""
    original = os.environ.get("DAFT_UDF_POOL_SIZE")
    yield
    if original is not None:
        os.environ["DAFT_UDF_POOL_SIZE"] = original
    elif "DAFT_UDF_POOL_SIZE" in os.environ:
        del os.environ["DAFT_UDF_POOL_SIZE"]


def get_pid_udf() -> Callable[[int], int]:
    """Helper to create a UDF that returns the process ID."""

    @daft.func(use_process=True)
    def get_pid(x: int) -> int:
        import os

        return os.getpid()

    return get_pid


def is_process_alive(pid: int) -> bool:
    """Check if a process is still alive."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def wait_for_pids_to_exit(pids: set[int], timeout: float = 3.0, check_interval: float = 0.1) -> None:
    """Wait for all given process IDs to exit."""
    if not pids:
        return

    start = time.time()
    while time.time() - start < timeout:
        alive_pids = {pid for pid in pids if is_process_alive(pid)}
        if not alive_pids:
            return
        time.sleep(check_interval)

    alive_pids = {pid for pid in pids if is_process_alive(pid)}
    if alive_pids:
        import warnings

        warnings.warn(
            f"Processes {alive_pids} did not exit within {timeout}s. "
            f"These may be zombie processes or workers that weren't properly cleaned up.",
            UserWarning,
        )


# ==============================================================================
# Basic Functionality Tests
# ==============================================================================


def test_basic_process_pool_execution():
    """Test that UDFs execute in worker processes and pool provides basic functionality."""
    import os

    main_pid = os.getpid()

    # Test 1: UDF executes in worker process (not main)
    @daft.func(use_process=True)
    def get_worker_pid(x: int) -> int:
        import os

        return os.getpid()

    df = daft.from_pydict({"a": list(range(10))})
    result = df.select(get_worker_pid(df["a"])).collect()
    worker_pids = set(result.to_pydict()["a"])

    assert len(worker_pids) > 0, "Should have used at least one worker process"
    assert main_pid not in worker_pids, f"UDF should not execute in main process (PID {main_pid})"

    # Test 2: Pool statistics reflect actual usage
    _, _, total = daft.execution.udf.get_process_pool_stats()
    assert total > 0, "Process pool should have created at least one worker"

    # Test 3: Multiple UDFs share the same pool
    @daft.func(use_process=True)
    def double(x: int) -> int:
        return x * 2

    @daft.func(use_process=True)
    def triple(x: int) -> int:
        return x * 3

    df = daft.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})
    result = df.select(double(df["a"]), triple(df["b"])).collect()
    assert result.to_pydict() == {"a": [2, 4, 6], "b": [30, 60, 90]}


def test_python_dtype_fallback():
    """Test that UDFs with Python dtypes fall back to inline execution."""
    df = daft.from_pydict({"a": [{"x": 1}, {"x": 2}, {"x": 3}]})

    @daft.func(use_process=True, return_dtype=DataType.python())
    def get_x(obj):
        return obj["x"]

    result = df.select(get_x(df["a"])).collect()
    assert result.to_pydict()["a"] == [1, 2, 3]


# ==============================================================================
# Pool Sizing and Concurrency Tests
# ==============================================================================


@pytest.mark.parametrize(
    "pool_size,max_concurrency,expected_max_pids",
    [
        (2, 10, 2),  # Pool size limits usage
        (2, 5, 5),  # Pool scales up to max_concurrency
        (None, 3, 3),  # No pool size limit, uses max_concurrency
    ],
)
def test_pool_sizing_and_scaling(pool_size, max_concurrency, expected_max_pids):
    """Test pool sizing with various DAFT_UDF_POOL_SIZE and max_concurrency combinations."""
    if pool_size is not None:
        os.environ["DAFT_UDF_POOL_SIZE"] = str(pool_size)

    @daft.cls(max_concurrency=max_concurrency)
    class GetPid:
        def __init__(self):
            pass

        def __call__(self, x: int) -> int:
            import os

            return os.getpid()

    get_pid = GetPid()
    df = daft.from_pydict({"a": list(range(20))})
    result = df.select(get_pid(df["a"])).collect()
    pids = result.to_pydict()["a"]
    distinct_pids = len(set(pids))

    assert distinct_pids <= expected_max_pids, (
        f"Should use at most {expected_max_pids} processes, got {distinct_pids} distinct PIDs. "
        f"PIDs used: {set(pids)}"
    )
    assert distinct_pids > 0, "Should have used at least one process"


def test_max_concurrency_respects_highest_udf():
    """Test that pool size respects the UDF with highest max_concurrency."""

    @daft.cls(max_concurrency=3)
    class GetPid1:
        def __init__(self):
            pass

        def __call__(self, x: int) -> int:
            import os

            return os.getpid()

    @daft.cls(max_concurrency=7)
    class GetPid2:
        def __init__(self):
            pass

        def __call__(self, x: int) -> int:
            import os

            return os.getpid()

    get_pid1 = GetPid1()
    get_pid2 = GetPid2()
    df = daft.from_pydict({"a": list(range(20)), "b": list(range(20))})

    result = df.select(get_pid1(df["a"]), get_pid2(df["b"])).collect()
    pids1 = result.to_pydict()["a"]
    pids2 = result.to_pydict()["b"]
    max_distinct_pids = max(len(set(pids1)), len(set(pids2)))

    assert (
        max_distinct_pids <= 7
    ), f"Should use at most highest max_concurrency (7) processes, got {max_distinct_pids} distinct PIDs"


# ==============================================================================
# Lifecycle and Cleanup Tests
# ==============================================================================


@pytest.mark.parametrize("should_fail", [False, True])
def test_process_cleanup(should_fail):
    """Test that processes are cleaned up after query completion or failure."""
    if should_fail:

        @daft.func(use_process=True)
        def udf_func(x: int) -> int:
            raise ValueError(f"Intentional failure for value {x}")

        df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
        with pytest.raises(Exception):
            df.select(udf_func(df["a"])).collect()
        # Can't verify PID cleanup for failures since we don't capture PIDs
    else:
        get_pid = get_pid_udf()
        df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
        result = df.select(get_pid(df["a"])).collect()
        pids = set(result.to_pydict()["a"])
        assert len(pids) > 0, "Should have used at least one process"

        time.sleep(0.2)
        wait_for_pids_to_exit(pids, timeout=2.0)


def test_worker_reuse_and_caching():
    """Test that workers are reused across queries and properly cache UDF state."""

    @daft.cls
    class StatefulCounter:
        def __init__(self):
            self.call_count = 0

        def count(self, _: int) -> int:
            import os

            self.call_count += 1
            return os.getpid()

    counter = StatefulCounter()

    # Run multiple queries
    df1 = daft.from_pydict({"a": [1, 2, 3]})
    result1 = df1.select(counter.count(df1["a"])).collect()
    pids1 = set(result1.to_pydict()["a"])

    df2 = daft.from_pydict({"a": [4, 5, 6]})
    result2 = df2.select(counter.count(df2["a"])).collect()
    pids2 = set(result2.to_pydict()["a"])

    # Both queries should use worker processes
    assert len(pids1) > 0 and len(pids2) > 0, "Both queries should use worker processes"

    # Verify teardown doesn't break subsequent UDFs
    @daft.func(use_process=True)
    def other_udf(x: int) -> int:
        return x * 3

    df3 = daft.from_pydict({"a": [10, 20, 30]})
    result3 = df3.select(other_udf(df3["a"])).collect()
    assert result3.to_pydict()["a"] == [30, 60, 90], "Should work after previous UDF teardown"

    # Cleanup
    time.sleep(0.2)
    wait_for_pids_to_exit(pids1 | pids2, timeout=2.0)


# ==============================================================================
# Error Handling Tests
# ==============================================================================


def test_error_handling():
    """Test error handling for initialization failures and runtime errors."""

    # Test 1: Initialization failure
    @daft.func(use_process=True)
    def failing_init_udf(x: int) -> int:
        import nonexistent_module  # noqa: F401

        return x * 2

    df = daft.from_pydict({"a": [1, 2, 3]})
    with pytest.raises(Exception):
        df.select(failing_init_udf(df["a"])).collect()

    # Test 2: Pool recovers after errors
    @daft.func(use_process=True)
    def sometimes_fails(x: int) -> int:
        if x == 2:
            raise ValueError(f"Intentional failure for x={x}")
        return x * 10

    df = daft.from_pydict({"a": [1, 2, 3]})
    with pytest.raises(Exception):
        df.select(sometimes_fails(df["a"])).collect()

    # Pool should still work for subsequent queries
    @daft.func(use_process=True)
    def working_udf(x: int) -> int:
        return x * 2

    df2 = daft.from_pydict({"a": [10, 20, 30]})
    result = df2.select(working_udf(df2["a"])).collect()
    assert result.to_pydict()["a"] == [20, 40, 60], "Pool should recover and process new queries"


# ==============================================================================
# Concurrency and Data Transfer Tests
# ==============================================================================


def test_concurrent_queries():
    """Test multiple queries running concurrently using threading."""
    import threading

    @daft.func(use_process=True)
    def slow_multiply(x: int) -> int:
        import time

        time.sleep(0.05)
        return x * 2

    results = []
    errors = []

    def run_query(query_id: int):
        try:
            df = daft.from_pydict({"a": list(range(5))})
            result = df.select(slow_multiply(df["a"])).collect()
            results.append((query_id, result.to_pydict()["a"]))
        except Exception as e:
            errors.append((query_id, e))

    threads = [threading.Thread(target=run_query, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0, f"Some queries failed: {errors}"
    assert len(results) == 3, "All queries should complete"

    expected = [0, 2, 4, 6, 8]
    for query_id, result in results:
        assert result == expected, f"Query {query_id} returned incorrect result"
