from __future__ import annotations

import random
from uuid import uuid4

import numpy as np
import pytest

import daft
from daft import DataFrame, col

NUM_SAMPLES = 1_000_000

JOIN_TYPES = ["inner", "left", "right", "outer"]


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize(
    "num_samples, num_partitions",
    [(10_000, 1), (10_000, 100)],
    ids=["10_000/1", "10_000/100"],
)
@pytest.mark.parametrize("join_type", JOIN_TYPES)
def test_join_simple(benchmark, num_samples, num_partitions, join_type) -> None:
    """Test simple join performance.

    Keys are consecutive integers; no data payload; one-to-one matches.
    """

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    left_table = (
        daft.from_pydict(
            {
                "mycol": left_arr,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )
    right_table = (
        daft.from_pydict(
            {
                "mycol": right_arr,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )

    # Run the benchmark.
    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mycol"], how=join_type).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("mycol").to_pandas()["mycol"].to_numpy() == np.arange(num_samples)).all()


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize(
    "num_samples, num_partitions",
    [(10_000, 1), (10_000, 100)],
    ids=["10_000/1", "10_000/100"],
)
@pytest.mark.parametrize("join_type", JOIN_TYPES)
def test_join_largekey(benchmark, num_samples, num_partitions, join_type) -> None:
    """Test the impact of string keys vs integer keys."""

    keys = [str(uuid4()) for _ in range(num_samples)]

    left_keys = keys.copy()
    random.shuffle(left_keys)
    right_keys = keys.copy()
    random.shuffle(right_keys)

    left_table = (
        daft.from_pydict(
            {
                "mycol": left_keys,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )
    right_table = (
        daft.from_pydict(
            {
                "mycol": right_keys,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )

    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mycol"], how=join_type).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    result_keys = result.to_pydict()["mycol"]
    result_keys.sort()
    keys.sort()
    assert result_keys == keys


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize(
    "num_samples, num_partitions",
    [(10_000, 1), (10_000, 100)],
    ids=["10_000/1", "10_000/100"],
)
@pytest.mark.parametrize("join_type", JOIN_TYPES)
def test_join_withdata(benchmark, num_samples, num_partitions, join_type) -> None:
    """Test the impact of data payloads."""

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    long_A = "A" * 1024
    long_B = "B" * 1024

    left_table = (
        daft.from_pydict(
            {
                "mykey": left_arr,
                "left_data": [long_A for _ in range(num_samples)],
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )
    right_table = (
        daft.from_pydict(
            {
                "mykey": right_arr,
                "right_data": [long_B for _ in range(num_samples)],
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )

    # Run the benchmark.
    # Run the benchmark.
    def bench_join() -> DataFrame:
        return left_table.join(right_table, on=["mykey"], how=join_type).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("mykey").to_pandas()["mykey"].to_numpy() == np.arange(num_samples)).all()
    assert result.groupby("left_data", "right_data").agg(col("mykey").count()).to_pydict() == {
        "left_data": [long_A],
        "right_data": [long_B],
        "mykey": [num_samples],
    }


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize(
    "left_bigger",
    [True, False],
    ids=["left_bigger", "right_bigger"],
)
@pytest.mark.parametrize("num_partitions", [1, 10], ids=["1part", "10part"])
@pytest.mark.parametrize("join_type", JOIN_TYPES)
def test_broadcast_join(benchmark, left_bigger, num_partitions, join_type) -> None:
    """Test the performance of joining a smaller table to a bigger table.

    The cardinality is one-to-many.
    """

    small_length = 1_000
    big_factor = 10

    small_arr = np.arange(small_length)
    np.random.shuffle(small_arr)

    big_arr = np.concatenate([np.arange(small_length) for _ in range(big_factor)])
    np.random.shuffle(big_arr)

    small_table = daft.from_pydict(
        {
            "keys": small_arr,
            "data": [str(x) for x in small_arr],
        }
    ).collect()
    big_table = (
        daft.from_pydict(
            {
                "keys": big_arr,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )

    # Run the benchmark.
    def bench_join() -> DataFrame:
        if left_bigger:
            return big_table.join(small_table, on=["keys"], how=join_type).collect()
        else:
            return small_table.join(big_table, on=["keys"], how=join_type).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    data = result.sort("keys").to_pydict()["data"]
    assert data[:big_factor] == ["0"] * big_factor
    assert data[-big_factor:] == [str(small_length - 1)] * big_factor


@pytest.mark.benchmark(group="joins")
@pytest.mark.parametrize(
    "num_samples, num_partitions",
    [(10_000, 1), (10_000, 100)],
    ids=["10_000/1", "10_000/100"],
)
@pytest.mark.parametrize("num_columns", [1, 4])
@pytest.mark.parametrize("join_type", JOIN_TYPES)
def test_multicolumn_joins(benchmark, num_columns, num_samples, num_partitions, join_type) -> None:
    """Evaluate the performance impact of using additional columns in the join.

    The join cardinality is the same for all cases;
    redundant columns are used for the multicolumn joins.
    """

    left_arr = np.arange(num_samples)
    np.random.shuffle(left_arr)
    right_arr = np.arange(num_samples)
    np.random.shuffle(right_arr)

    left_table = (
        daft.from_pydict(
            {
                "nums_a": left_arr * 17 % 9,
                "nums_b": left_arr * 17 % 10,
                "nums_c": left_arr * 17 % 11,
                "nums": left_arr,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )
    right_table = (
        daft.from_pydict(
            {
                "nums_a": right_arr * 17 % 9,
                "nums_b": right_arr * 17 % 10,
                "nums_c": right_arr * 17 % 11,
                "nums": right_arr,
            }
        )
        .into_partitions(num_partitions)
        .collect()
    )

    # Run the benchmark.
    # Run the benchmark.
    def bench_join() -> DataFrame:
        # Use the unique column "nums" plus some redundant columns.
        join_on = ["nums_a", "nums_b", "nums_c", "nums"][-num_columns:]
        return left_table.join(right_table, on=join_on, how=join_type).collect()

    result = benchmark(bench_join)

    # Make sure the result is correct.
    assert (result.sort("nums").to_pandas()["nums"].to_numpy() == np.arange(num_samples)).all()
