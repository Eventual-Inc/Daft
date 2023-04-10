from __future__ import annotations

import random
from uuid import uuid4

import numpy as np
import pytest

from daft import DataFrame

NUM_SAMPLES = 1_000_000


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_join_baseline(benchmark, num_samples) -> None:
    """Test simple join performance.

    Keys are unique integers; no data payload; one-to-one matches.
    """

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    left_table = DataFrame.from_pydict({"mycol": left_arr}).collect()
    right_table = DataFrame.from_pydict({"mycol": right_arr}).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mycol"]).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("mycol").to_pandas()["mycol"].to_numpy() == np.arange(num_samples)).all()


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("num_partitions", [2])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_join_multipart(benchmark, num_samples, num_partitions) -> None:
    """Test the impact of an additional partition."""

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    left_table = DataFrame.from_pydict({"mycol": left_arr}).into_partitions(num_partitions).collect()
    right_table = DataFrame.from_pydict({"mycol": right_arr}).into_partitions(num_partitions).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mycol"]).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("mycol").to_pandas()["mycol"].to_numpy() == np.arange(num_samples)).all()


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_join_largekey(benchmark, num_samples) -> None:
    """Test the impact of string keys vs integer keys."""

    keys = [str(uuid4()) for _ in range(num_samples)]

    left_keys = keys.copy()
    random.shuffle(left_keys)
    right_keys = keys.copy()
    random.shuffle(right_keys)

    left_table = DataFrame.from_pydict({"mycol": left_keys}).collect()
    right_table = DataFrame.from_pydict({"mycol": right_keys}).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mycol"]).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    result_keys = result.to_pydict()["mycol"]
    result_keys.sort()
    keys.sort()
    assert result_keys == keys


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_join_withdata(benchmark, num_samples) -> None:
    """Test the impact of data payloads."""

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    long_A = "A" * 1024
    long_B = "B" * 1024

    left_table = DataFrame.from_pydict(
        {
            "mykey": left_arr,
            "left_data": [long_A for _ in range(num_samples)],
        }
    ).collect()
    right_table = DataFrame.from_pydict(
        {
            "mykey": right_arr,
            "right_data": [long_B for _ in range(num_samples)],
        }
    ).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mykey"]).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("mykey").to_pandas()["mykey"].to_numpy() == np.arange(num_samples)).all()
    assert result.groupby("left_data", "right_data").agg([("mykey", "count")]).to_pydict() == {
        "left_data": [long_A],
        "right_data": [long_B],
        "mykey": [num_samples],
    }


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("left_low_card", [True, False])
@pytest.mark.parametrize("cardinality", [100])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_join_one_to_many(benchmark, num_samples, cardinality, left_low_card) -> None:
    """Test the performance of joining a low-cardinality table to a high-cardinality table."""

    low_card = np.arange(num_samples) % cardinality
    np.random.shuffle(low_card)

    high_card = np.arange(num_samples)
    np.random.shuffle(high_card)

    low_card_df = DataFrame.from_pydict(
        {
            "keys": low_card,
        }
    ).collect()
    high_card_df = DataFrame.from_pydict(
        {
            "keys": high_card,
        }
    ).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        if left_low_card:
            return low_card_df.join(high_card_df, on=["keys"]).collect()
        else:
            return high_card_df.join(low_card_df, on=["keys"]).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    data = result.sort("keys").to_pydict()["keys"]
    assert len(data) == num_samples
    groupsize = num_samples // cardinality
    assert data[0] == 0
    assert data[groupsize - 1] == 0
    assert data[-groupsize] == cardinality - 1
    assert data[-1] == cardinality - 1


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("cardinality", [100])
@pytest.mark.parametrize("num_samples", [10_000])
def test_join_many_to_many(benchmark, num_samples, cardinality) -> None:
    """Test the performance of joining a low-cardinality table to a low-cardinality table."""

    left = np.arange(num_samples) % cardinality
    np.random.shuffle(left)

    right = np.arange(num_samples) % cardinality
    np.random.shuffle(right)

    left_df = DataFrame.from_pydict({"keys": left}).collect()
    right_df = DataFrame.from_pydict({"keys": right}).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_df.join(right_df, on=["keys"]).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    data = result.sort("keys").to_pydict()["keys"]
    assert len(data) == num_samples**2 // cardinality
    groupsize = (num_samples // cardinality) ** 2
    assert data[0] == 0
    assert data[groupsize - 1] == 0
    assert data[-groupsize] == cardinality - 1
    assert data[-1] == cardinality - 1


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize("num_columns", [1, 4])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_multicolumn_joins(benchmark, num_columns, num_samples) -> None:
    """Evaluate the performance impact of using additional columns in the join.

    The join cardinality is the same for all cases;
    redundant columns are used for the multicolumn joins.
    """

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    left_table = DataFrame.from_pydict(
        {
            "nums_a": left_arr * 7 % 9,
            "nums_b": left_arr * 7 % 10,
            "nums_c": left_arr * 7 % 11,
            "nums": left_arr,
        }
    ).collect()
    right_table = DataFrame.from_pydict(
        {
            "nums_a": right_arr * 7 % 9,
            "nums_b": right_arr * 7 % 10,
            "nums_c": right_arr * 7 % 11,
            "nums": right_arr,
        }
    ).collect()

    # Run the benchmark.
    def bench_join() -> DataFrame:
        # Use the unique column "nums" plus some redundant columns.
        join_on = ["nums_a", "nums_b", "nums_c", "nums"][-num_columns:]
        return left_table.join(right_table, on=join_on).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("nums").to_pandas()["nums"].to_numpy() == np.arange(num_samples)).all()
