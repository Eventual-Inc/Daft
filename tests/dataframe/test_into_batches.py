from __future__ import annotations

import pytest

import daft


def test_large_partition_into_batches(make_df):
    """Test splitting a large partition (size 64) into batches of size 8."""
    # Create a dataframe with 64 rows in a single partition
    df = make_df({"id": list(range(64))}, repartition=1)

    # Split into batches of size 8
    df = df.into_batches(8)

    # Create a UDF that asserts each batch size is exactly 8
    @daft.func.batch(return_dtype=daft.DataType.int64())
    def check_batch_size(data):
        batch_size = len(data.to_pylist())
        assert batch_size == 8, f"Expected batch size 8, got {batch_size}"
        return data.to_pylist()

    # Apply the UDF to verify batch sizes
    result = df.with_column("checked_id", check_batch_size(df["id"])).collect()

    # Verify the data is unchanged (order might be different due to partitioning)
    result_ids = sorted(result.select("checked_id").to_pydict()["checked_id"])
    assert result_ids == list(range(64))


def test_many_small_partitions_into_batches(make_df):
    """Test coalescing many small partitions (64 partitions of size 1) into batches of size 8."""
    # Create a dataframe with 64 rows split into 64 partitions (1 row each)
    df = make_df({"id": list(range(64))}, repartition=64)

    # Split into batches of size 8
    df = df.into_batches(8)

    batch_lengths = [len(batch) for batch in df.to_arrow_iter()]

    assert sum(batch_lengths) == 64, f"Expected 64 rows, got {sum(batch_lengths)}"

    # Verify coalescing happened: significantly fewer output batches than input partitions.
    # With batch_size=8 and 64 rows, we expect roughly 8-13 batches, not 64.
    assert len(batch_lengths) <= 20, (
        f"Expected coalescing to reduce batch count, got {len(batch_lengths)} batches: {batch_lengths}"
    )


def test_into_batches_with_remainder():
    """Test into_batches when the total size doesn't evenly divide by batch_size."""
    # Create a dataframe with 70 rows (not evenly divisible by 8)
    df = daft.from_pydict({"id": list(range(70))})

    # Split into batches of size 8
    df = df.into_batches(8)

    # Create a UDF that checks batch sizes
    @daft.func.batch(return_dtype=daft.DataType.int64())
    def check_batch_size(data):
        batch_size = len(data.to_pylist())
        assert batch_size == 8 or batch_size == 6, f"Expected batch size 8 or 6, got {batch_size}"
        return data.to_pylist()

    # Apply the UDF
    result = df.with_column("checked_id", check_batch_size(df["id"])).collect()

    # Verify we still have all 70 rows
    result_ids = sorted(result.select("checked_id").to_pydict()["checked_id"])
    assert result_ids == list(range(70))


def test_into_batches_single_row():
    """Test into_batches with a single row."""
    df = daft.from_pydict({"id": [42]})
    df = df.into_batches(8)

    @daft.func.batch(return_dtype=daft.DataType.int64())
    def check_batch_size(data):
        batch_size = len(data.to_pylist())
        assert batch_size == 1, f"Expected batch size 1, got {batch_size}"
        return data.to_pylist()

    result = df.with_column("checked_id", check_batch_size(df["id"])).collect()
    assert len(result) == 1
    assert result.to_pydict()["checked_id"] == [42]


def test_into_batches_empty_dataframe():
    """Test into_batches with an empty dataframe."""
    df = daft.from_pydict({"id": []})
    df = df.into_batches(8)

    result = df.collect()
    assert len(result) == 0


# ---------------------------------------------------------------------------
# Regression tests for GitHub issue #7087
#
# The NativeRunner plan cache is keyed by `plan_fingerprint`. Python never
# populates this field, so every execution fell back to fingerprint=0.
#
# The collision fires when two generators are alive at the same time: the
# first generator starts a ScanTasks pipeline that occupies slot 0; when
# the second generator (an InMemory pipeline) calls next() before the first
# is exhausted, it reuses the stale slot and hits the InputSender type
# mismatch at input_sender.rs:32.
#
# Serial executions do NOT trigger it — try_finish clears the plan from the
# cache once a generator is fully consumed.
# ---------------------------------------------------------------------------


def test_interleaved_iter_partitions_no_panic(tmp_path):
    """Interleaved iteration across two DataFrames with different source types must not panic.

    Regression for #7087: advancing a ScanTasks generator (read_parquet)
    and then calling next() on a concurrent InMemory generator (from_pydict)
    before the first is exhausted triggered an unreachable! panic in
    InputSender::send at input_sender.rs:32.
    """
    daft.from_pydict({"id": [1, 2, 3, 4]}).write_parquet(str(tmp_path / "data"))

    df1 = daft.read_parquet(str(tmp_path / "data"))   # ScanTasks source
    df2 = daft.from_pydict({"id": [5, 6, 7, 8]})      # InMemory source

    gen1 = df1.into_batches(2).iter_partitions()
    gen2 = df2.into_batches(2).iter_partitions()

    # Advance gen1 first — occupies the cache slot with a ScanTasks pipeline.
    # Then advance gen2 before gen1 is exhausted — without the fix this
    # collides on fingerprint=0 and panics.
    batch1 = next(gen1)
    batch2 = next(gen2)

    result1 = batch1.to_arrow()["id"].to_pylist()
    for mp in gen1:
        result1.extend(mp.to_arrow()["id"].to_pylist())

    result2 = batch2.to_arrow()["id"].to_pylist()
    for mp in gen2:
        result2.extend(mp.to_arrow()["id"].to_pylist())

    assert sorted(result1) == [1, 2, 3, 4]
    assert sorted(result2) == [5, 6, 7, 8]


def test_interleaved_to_arrow_iter_no_panic(tmp_path):
    """to_arrow_iter() variant of the interleaved-iteration regression (#7087)."""
    daft.from_pydict({"id": [10, 20, 30, 40]}).write_parquet(str(tmp_path / "data"))

    df1 = daft.read_parquet(str(tmp_path / "data"))   # ScanTasks source
    df2 = daft.from_pydict({"id": [50, 60, 70, 80]})  # InMemory source

    gen1 = df1.into_batches(2).to_arrow_iter()
    gen2 = df2.into_batches(2).to_arrow_iter()

    batch1 = next(gen1)
    batch2 = next(gen2)

    result1 = batch1["id"].to_pylist()
    for b in gen1:
        result1.extend(b["id"].to_pylist())

    result2 = batch2["id"].to_pylist()
    for b in gen2:
        result2.extend(b["id"].to_pylist())

    assert sorted(result1) == [10, 20, 30, 40]
    assert sorted(result2) == [50, 60, 70, 80]
