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
# NativeRunner's plan cache keyed every plan on `plan_fingerprint`, which
# Python never populates, so all plans fell back to fingerprint=0.  A prior
# operation (e.g. write_parquet) would occupy slot 0 with a ScanTasks
# pipeline; the next iter_partitions() / to_arrow_iter() call then hit the
# same slot with an InMemory plan, and the type mismatch triggered an
# `unreachable!` panic in InputSender::send at input_sender.rs.
#
# The fix generates a unique fingerprint per call when none is provided by
# the caller, so the cache never collides across independent executions.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("batch_size", [2, 3])
def test_iter_partitions_after_prior_daft_op_no_panic(tmp_path, batch_size):
    """iter_partitions must not panic when called after a prior daft operation.

    Regression for #7087: the NativeRunner plan cache collided on
    fingerprint=0, causing a type mismatch between the stale ScanTasks
    sender and the new InMemory plan.
    """
    # Any prior operation that goes through the execution engine is enough
    # to occupy the cache slot that used to be hardcoded to 0.
    df1 = daft.from_pydict({"id": [1, 2, 3]})
    df1.write_parquet(str(tmp_path / "prior_op"))

    df2 = daft.from_pydict({"id": [4, 5, 6]})
    result = []
    for mp in df2.into_batches(batch_size).iter_partitions():
        result.extend(mp.to_arrow()["id"].to_pylist())

    assert sorted(result) == [4, 5, 6]


def test_to_arrow_iter_after_prior_daft_op_no_panic(tmp_path):
    """to_arrow_iter() must not panic when called after a prior daft operation.

    Regression for #7087 — same root cause as iter_partitions variant above.
    """
    df1 = daft.from_pydict({"id": [10, 20, 30]})
    df1.write_parquet(str(tmp_path / "prior_op"))

    df2 = daft.from_pydict({"id": [40, 50, 60]})
    result = []
    for batch in df2.into_batches(2).to_arrow_iter():
        result.extend(batch["id"].to_pylist())

    assert sorted(result) == [40, 50, 60]


def test_multiple_sequential_iter_partitions_no_panic(tmp_path):
    """Multiple sequential iter_partitions calls must each return correct results.

    Regression for #7087: each call must get an independent cache slot so
    they don't interfere with each other.
    """
    df1 = daft.from_pydict({"id": [1, 2, 3]})
    df1.write_parquet(str(tmp_path / "prior_op"))

    for i in range(3):
        rows = list(range(i * 10, i * 10 + 5))
        df = daft.from_pydict({"id": rows})
        result = []
        for mp in df.into_batches(2).iter_partitions():
            result.extend(mp.to_arrow()["id"].to_pylist())
        assert sorted(result) == rows, f"Iteration {i}: expected {rows}, got {sorted(result)}"
