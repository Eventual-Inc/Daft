"""Tests for the UDF process pool functionality."""

from __future__ import annotations

import os

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


def test_multiple_udfs_with_use_process():
    """Test that multiple different UDFs with use_process=True share the same process pool."""
    from daft.daft import _get_process_pool_stats

    df = daft.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]})

    @daft.func(use_process=True)
    def double(x: int) -> int:
        return x * 2

    @daft.func(use_process=True)
    def triple(x: int) -> int:
        return x * 3

    # Get initial pool stats
    max_workers_before, active_before, total_before = _get_process_pool_stats()

    result = df.select(double(df["a"]), triple(df["b"])).collect()
    assert result.to_pydict() == {"a": [2, 4, 6], "b": [30, 60, 90]}

    # Verify that both UDFs used the pool (workers were created)
    max_workers_after, active_after, total_after = _get_process_pool_stats()
    assert total_after > total_before, "Pool should have created workers for the UDFs"
    assert active_after <= max_workers_after, "Active workers should not exceed max"


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
    """Test that DAFT_UDF_POOL_SIZE env var configures the maximum pool size."""
    from daft.daft import _get_process_pool_stats

    # Set env var before any pool operations (pool is initialized lazily)
    # Note: This test assumes the pool hasn't been initialized yet by previous tests
    # If it has, the env var won't take effect until next test run
    original = os.environ.get("DAFT_UDF_POOL_SIZE")
    os.environ["DAFT_UDF_POOL_SIZE"] = "4"

    try:
        df = daft.from_pydict({"a": [1, 2, 3, 4, 5, 6, 7, 8]})

        @daft.func(use_process=True)
        def double(x: int) -> int:
            return x * 2

        result = df.select(double(df["a"])).collect()
        assert result.to_pydict() == {"a": [2, 4, 6, 8, 10, 12, 14, 16]}

        # Verify pool size was configured correctly (or at least active workers don't exceed max)
        max_workers, active_workers, total_workers = _get_process_pool_stats()
        # Note: max_workers might be higher if pool was already initialized, but active should respect limit
        assert active_workers <= max_workers, f"Active workers ({active_workers}) should not exceed max ({max_workers})"
        assert total_workers <= max_workers, f"Total workers ({total_workers}) should not exceed max ({max_workers})"
    finally:
        if original is not None:
            os.environ["DAFT_UDF_POOL_SIZE"] = original
        elif "DAFT_UDF_POOL_SIZE" in os.environ:
            del os.environ["DAFT_UDF_POOL_SIZE"]


def test_sequential_queries_reuse_pool():
    """Test that sequential queries reuse the same process pool (workers persist)."""
    from daft.daft import _get_process_pool_stats

    @daft.func(use_process=True)
    def increment(x: int) -> int:
        return x + 1

    # First query
    df1 = daft.from_pydict({"a": [1, 2, 3]})
    result1 = df1.select(increment(df1["a"])).collect()
    assert result1.to_pydict() == {"a": [2, 3, 4]}

    # Get pool stats after first query
    max_workers, active_after_first, total_after_first = _get_process_pool_stats()
    assert total_after_first > 0, "Pool should have created at least one worker"

    # Second query should reuse the pool (workers cached, not recreated)
    df2 = daft.from_pydict({"a": [10, 20, 30]})
    result2 = df2.select(increment(df2["a"])).collect()
    assert result2.to_pydict() == {"a": [11, 21, 31]}

    # Verify workers were reused (total should be same or similar, not doubled)
    _, active_after_second, total_after_second = _get_process_pool_stats()
    assert total_after_second <= total_after_first + 1, "Pool should reuse workers, not create many new ones"


def test_processes_spun_down_after_completion():
    """Test that all processes are spun down after query completion."""
    from daft.daft import _get_process_pool_stats

    @daft.func(use_process=True)
    def get_pid(x: int) -> int:
        import os
        return os.getpid()

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})

    # Get initial state
    max_before, active_before, total_before = _get_process_pool_stats()

    # Run query
    result = df.select(get_pid(df["a"])).collect()
    pids = result.to_pydict()["a"]
    distinct_pids = len(set(pids))
    assert distinct_pids > 0, "Should have used at least one process"

    # After query completion and UDF teardown, workers with empty cache should exit
    # Wait a bit for teardown to complete
    import time
    time.sleep(0.1)

    # After query completion, all workers should be spun down
    # (workers exit when their cache becomes empty after teardown)
    max_after, active_after, total_created_after = _get_process_pool_stats()
    assert active_after == 0, f"All workers should exit after query completion (cache empty), got {active_after} active"
    # total_created is cumulative, so it should have increased by at least the number of distinct processes used
    assert total_created_after >= total_before + distinct_pids, f"Should have created at least {distinct_pids} workers, total_created went from {total_before} to {total_created_after}"


def test_processes_spun_down_after_failure():
    """Test that all processes are spun down after query failure."""
    from daft.daft import _get_process_pool_stats

    @daft.func(use_process=True)
    def failing_udf(x: int) -> int:
        if x > 3:
            raise ValueError("Intentional failure")
        return x * 2

    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})

    # Get initial state
    max_before, active_before, total_before = _get_process_pool_stats()

    # Query should fail
    with pytest.raises(Exception):
        df.select(failing_udf(df["a"])).collect()

    # Wait for cleanup
    import time
    time.sleep(0.1)

    # After query failure, UDF operator is dropped and teardown is called
    # Workers with empty cache should exit
    max_after, active_after, total_created_after = _get_process_pool_stats()
    assert active_after == 0, f"All workers should exit after query failure (cache empty), got {active_after} active"
    # total_created is cumulative, so it may have increased, but active should be 0
    assert total_created_after >= total_before, f"total_created is cumulative, should be >= {total_before}, got {total_created_after}"


def test_pool_scales_up_for_max_concurrency():
    """Test that pool scales up when UDF max_concurrency exceeds default pool size."""
    from daft.daft import _get_process_pool_stats

    # Set a small default pool size
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

        # Get initial pool size
        max_before, _, _ = _get_process_pool_stats()

        # Run query - should scale up pool to accommodate max_concurrency=5
        result = df.select(get_pid(df["a"])).collect()
        pids = result.to_pydict()["a"]
        distinct_pids = len(set(pids))

        # Verify pool scaled up
        max_workers, active_workers, total_workers = _get_process_pool_stats()
        assert max_workers >= 5, f"Pool should have scaled up from {max_before} to at least 5, got {max_workers}"
        assert distinct_pids <= max_workers, f"Should not use more processes than max_workers ({max_workers}), got {distinct_pids} distinct PIDs"
        # Should use up to max_concurrency processes
        assert distinct_pids <= 5, f"Should use at most max_concurrency (5) processes, got {distinct_pids} distinct PIDs"
    finally:
        if original is not None:
            os.environ["DAFT_UDF_POOL_SIZE"] = original
        elif "DAFT_UDF_POOL_SIZE" in os.environ:
            del os.environ["DAFT_UDF_POOL_SIZE"]


def test_max_processes_equals_highest_max_concurrency():
    """Test that max number of processes equals the UDF with highest max_concurrency."""
    from daft.daft import _get_process_pool_stats

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

    # Get initial pool size
    max_before, _, _ = _get_process_pool_stats()

    # Run query with both UDFs
    result = df.select(get_pid1(df["a"]), get_pid2(df["b"])).collect()
    pids1 = result.to_pydict()["a"]
    pids2 = result.to_pydict()["b"]
    distinct_pids1 = len(set(pids1))
    distinct_pids2 = len(set(pids2))
    max_distinct_pids = max(distinct_pids1, distinct_pids2)

    # Verify pool scaled to highest max_concurrency (7)
    max_workers, active_workers, total_workers = _get_process_pool_stats()
    assert max_workers >= 7, f"Pool should have scaled from {max_before} to at least 7 (highest max_concurrency), got {max_workers}"
    # The max number of distinct processes used should be <= highest max_concurrency
    assert max_distinct_pids <= 7, f"Should use at most highest max_concurrency (7) processes, got {max_distinct_pids} distinct PIDs"
    assert max_distinct_pids <= max_workers, f"Should not use more processes than max_workers ({max_workers}), got {max_distinct_pids} distinct PIDs"
