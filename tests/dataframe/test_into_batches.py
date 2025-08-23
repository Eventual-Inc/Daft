from __future__ import annotations

import pytest

import daft
from daft.context import get_context
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray"
    and get_context().daft_execution_config.use_experimental_distributed_engine is False,
    reason="requires Native Runner or Flotilla to be in use",
)


def test_large_partition_into_batches(make_df):
    """Test splitting a large partition (size 64) into batches of size 8."""
    # Create a dataframe with 64 rows in a single partition
    df = make_df({"id": list(range(64))}, repartition=1)
    assert df.num_partitions() == 1

    # Split into batches of size 8
    df = df.into_batches(8)

    # Create a UDF that asserts each batch size is exactly 8
    @daft.udf(return_dtype=daft.DataType.int64())
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
    assert df.num_partitions() == 64

    # Split into batches of size 8
    df = df.into_batches(8)

    batch_lengths = [len(batch) for batch in df.to_arrow_iter()]

    # Expected all batches to be >= the batch threshold (8 * 0.8 = 6)
    for i in range(len(batch_lengths) - 1):
        assert batch_lengths[i] >= int(8 * 0.8), f"Expected batch to be >= 6, got {batch_lengths[i]}"

    assert sum(batch_lengths) == 64, f"Expected 64 rows, got {sum(batch_lengths)}"


def test_into_batches_with_remainder():
    """Test into_batches when the total size doesn't evenly divide by batch_size."""
    # Create a dataframe with 70 rows (not evenly divisible by 8)
    df = daft.from_pydict({"id": list(range(70))})

    # Split into batches of size 8
    df = df.into_batches(8)

    # Create a UDF that checks batch sizes
    @daft.udf(return_dtype=daft.DataType.int64())
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

    @daft.udf(return_dtype=daft.DataType.int64())
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
