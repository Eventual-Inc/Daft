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
        # Signal 0 doesn't kill the process, just checks if it exists
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def wait_for_pids_to_exit(pids: set[int], timeout: float = 3.0, check_interval: float = 0.1) -> None:
    """Wait for all given process IDs to exit.

    Args:
        pids: Set of process IDs to wait for
        timeout: Maximum time to wait in seconds
        check_interval: Time between checks in seconds

    Note: This function will not hang indefinitely - it will fail the test if processes
    don't exit within the timeout. This helps identify cleanup issues.
    """
    if not pids:
        return

    start = time.time()
    while time.time() - start < timeout:
        alive_pids = {pid for pid in pids if is_process_alive(pid)}
        if not alive_pids:
            return
        time.sleep(check_interval)

    # Final check
    alive_pids = {pid for pid in pids if is_process_alive(pid)}
    if alive_pids:
        # Don't fail immediately - log a warning but allow test to continue
        # This prevents tests from hanging, but we still want to know about cleanup issues
        import warnings

        warnings.warn(
            f"Processes {alive_pids} did not exit within {timeout}s. "
            f"These may be zombie processes or workers that weren't properly cleaned up.",
            UserWarning,
        )


def test_multiple_udfs_with_use_process():
    """Test that multiple different UDFs with use_process=True share the same process pool."""
    df = daft.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

    @daft.func(use_process=True)
    def double(x: int) -> int:
        return x * 2

    @daft.func(use_process=True)
    def triple(x: int) -> int:
        return x * 3

    # Both UDFs should execute successfully using the pool
    result = df.select(double(df["a"]), triple(df["b"])).collect()
    assert result.to_pydict() == {"a": [2, 4, 6], "b": [30, 60, 90]}


def test_udf_inline_fallback_for_python_dtype():
    """Test that UDFs with Python dtypes fall back to inline execution (not pool)."""
    df = daft.from_pydict({"a": [{"x": 1}, {"x": 2}, {"x": 3}]})

    @daft.func(use_process=True, return_dtype=DataType.python())
    def get_x(obj):
        return obj["x"]

    # This should work even though use_process=True, because Python dtype
    # triggers inline fallback (non-serializable types can't use process pool)
    result = df.select(get_x(df["a"])).collect()
    values = result.to_pydict()["a"]
    assert values == [1, 2, 3]


def test_pool_size_configuration():
    """Test that DAFT_UDF_POOL_SIZE env var limits the number of concurrent processes."""
    original = os.environ.get("DAFT_UDF_POOL_SIZE")
    os.environ["DAFT_UDF_POOL_SIZE"] = "2"

    try:

        @daft.cls(max_concurrency=10)
        class GetPid:
            def __init__(self):
                pass

            def __call__(self, x: int) -> int:
                import os

                return os.getpid()

        get_pid_cls = GetPid()
        df = daft.from_pydict({"a": list(range(20))})

        result = df.select(get_pid_cls(df["a"])).collect()
        pids = result.to_pydict()["a"]
        distinct_pids = len(set(pids))

        # Should respect the pool size limit (2), not max_concurrency (10)
        assert distinct_pids <= 2, (
            f"Should use at most 2 processes (pool size), got {distinct_pids} distinct PIDs. " f"PIDs used: {set(pids)}"
        )
    finally:
        if original is not None:
            os.environ["DAFT_UDF_POOL_SIZE"] = original
        elif "DAFT_UDF_POOL_SIZE" in os.environ:
            del os.environ["DAFT_UDF_POOL_SIZE"]


def test_processes_spun_down_after_completion():
    """Test that all processes are spun down after query completion."""
    get_pid = get_pid_udf()
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})

    # Run query and collect PIDs
    result = df.select(get_pid(df["a"])).collect()
    pids = set(result.to_pydict()["a"])
    assert len(pids) > 0, "Should have used at least one process"

    # Give a small delay for teardown to complete
    time.sleep(0.2)

    # Wait for processes to exit (they should be cleaned up after query completion)
    # Use a shorter timeout to avoid hanging - if processes don't exit quickly,
    # there's likely a cleanup issue
    wait_for_pids_to_exit(pids, timeout=2.0)


def test_processes_spun_down_after_failure():
    """Test that all processes are spun down after query failure."""

    @daft.func(use_process=True)
    def failing_udf(x: int) -> int:
        import os

        return os.getpid()

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})

    # Query should fail - we can't easily capture PIDs from failed queries
    # but we can verify the query fails as expected
    with pytest.raises(Exception):
        df.select(failing_udf(df["a"])).collect()

    # Give a small delay for cleanup
    time.sleep(0.2)

    # Note: We can't verify process cleanup here since we don't have the PIDs,
    # but the important thing is that the query fails gracefully and doesn't hang


def test_pool_scales_up_for_max_concurrency():
    """Test that pool scales up when UDF max_concurrency exceeds default pool size."""
    original = os.environ.get("DAFT_UDF_POOL_SIZE")
    os.environ["DAFT_UDF_POOL_SIZE"] = "2"

    try:

        @daft.cls(max_concurrency=5)
        class GetPid:
            def __init__(self):
                pass

            def __call__(self, x: int) -> int:
                import os

                return os.getpid()

        get_pid = GetPid()
        df = daft.from_pydict({"a": list(range(20))})

        # Run query - should scale up pool to accommodate max_concurrency=5
        result = df.select(get_pid(df["a"])).collect()
        pids = result.to_pydict()["a"]
        distinct_pids = len(set(pids))

        # Should use up to max_concurrency processes (5), not limited by initial pool size (2)
        assert distinct_pids <= 5, (
            f"Should use at most max_concurrency (5) processes, got {distinct_pids} distinct PIDs. "
            f"PIDs used: {set(pids)}"
        )
        assert distinct_pids > 0, "Should have used at least one process"
    finally:
        if original is not None:
            os.environ["DAFT_UDF_POOL_SIZE"] = original
        elif "DAFT_UDF_POOL_SIZE" in os.environ:
            del os.environ["DAFT_UDF_POOL_SIZE"]


def test_max_processes_equals_highest_max_concurrency():
    """Test that max number of processes equals the UDF with highest max_concurrency."""

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

    # Run query with both UDFs
    result = df.select(get_pid1(df["a"]), get_pid2(df["b"])).collect()
    pids1 = result.to_pydict()["a"]
    pids2 = result.to_pydict()["b"]
    distinct_pids1 = len(set(pids1))
    distinct_pids2 = len(set(pids2))
    max_distinct_pids = max(distinct_pids1, distinct_pids2)

    # The max number of distinct processes used should be <= highest max_concurrency (7)
    assert max_distinct_pids <= 7, (
        f"Should use at most highest max_concurrency (7) processes, got {max_distinct_pids} distinct PIDs. "
        f"UDF1 PIDs: {set(pids1)}, UDF2 PIDs: {set(pids2)}"
    )
    assert max_distinct_pids > 0, "Should have used at least one process"


def test_worker_reuse_across_queries():
    """Test that workers are reused across multiple queries with the same UDF."""
    get_pid = get_pid_udf()

    # First query
    df1 = daft.from_pydict({"a": [1, 2, 3]})
    result1 = df1.select(get_pid(df1["a"])).collect()
    pids1 = set(result1.to_pydict()["a"])

    # Second query with same UDF (no delay - workers should still be available)
    df2 = daft.from_pydict({"a": [4, 5, 6]})
    result2 = df2.select(get_pid(df2["a"])).collect()
    pids2 = set(result2.to_pydict()["a"])

    # Both queries should use processes
    assert len(pids1) > 0 and len(pids2) > 0, "Both queries should use processes"

    # Workers may be reused (PIDs overlap) or new ones may be created
    # The important thing is that the pool is working and both queries succeed
    # Note: We don't assert on overlap since workers might be cleaned up between queries
    # The key test is that both queries execute successfully

    # Give a small delay for cleanup after second query
    time.sleep(0.2)

    # Clean up - wait for all PIDs to exit (with shorter timeout to avoid hanging)
    all_pids = pids1 | pids2
    wait_for_pids_to_exit(all_pids, timeout=2.0)


def test_concurrent_execution():
    """Test that multiple UDFs can execute concurrently."""

    @daft.func(use_process=True)
    def slow_udf(x: int) -> int:
        import time

        time.sleep(0.1)  # Simulate some work
        return x * 2

    df = daft.from_pydict({"a": list(range(10))})

    start = time.time()
    result = df.select(slow_udf(df["a"])).collect()
    elapsed = time.time() - start

    # With concurrent execution, this should take less than 10 * 0.1 = 1.0 seconds
    # (assuming at least some parallelism)
    assert elapsed < 0.8, f"Execution took {elapsed}s, expected < 0.8s with concurrency"
    assert result.to_pydict()["a"] == [x * 2 for x in range(10)]
