from __future__ import annotations

import random
from uuid import uuid4

import numpy as np
import pytest

from daft import DataFrame

NUM_SAMPLES = 1_000_000


@pytest.mark.benchmark(group="sorts")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_sort_simple(benchmark, num_samples) -> None:
    """Test simple sort performance.

    Keys are unique integers; no data payload.
    """

    arr = np.arange(num_samples)
    np.random.shuffle(arr)

    df = DataFrame.from_pydict({"mykey": arr}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.sort("mykey").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert (result.to_pandas()["mykey"].to_numpy() == np.arange(num_samples)).all()


@pytest.mark.benchmark(group="sorts")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_sort_skew(benchmark, num_samples) -> None:
    """Test sort performance with skewed key distribution."""

    arr = np.arange(num_samples)
    arr[: num_samples // 2] = 0
    np.random.shuffle(arr)

    df = DataFrame.from_pydict({"mykey": arr}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.sort("mykey").collect()

    benchmark(bench)


@pytest.mark.benchmark(group="sorts")
@pytest.mark.parametrize("num_partitions", [10])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_sort_multipart(benchmark, num_samples, num_partitions) -> None:
    """Test the performance impact of multiple partitions."""

    arr = np.arange(num_samples)
    np.random.shuffle(arr)

    df = DataFrame.from_pydict({"mykey": arr}).into_partitions(num_partitions).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.sort("mykey").collect()

    benchmark(bench)


@pytest.mark.benchmark(group="sorts")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_sort_strings(benchmark, num_samples) -> None:
    """Test the impact of string keys vs integer keys."""

    keys = [str(uuid4()) for _ in range(num_samples)]
    random.shuffle(keys)

    df = DataFrame.from_pydict({"mykey": keys}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.sort("mykey").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    keys.sort()
    assert result.to_pydict()["mykey"] == keys


@pytest.mark.benchmark(group="sorts")
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_sort_withdata(benchmark, num_samples) -> None:
    """Test the impact of data payloads."""

    arr = np.arange(num_samples)
    np.random.shuffle(arr)

    long_A = "A" * 1024

    df = DataFrame.from_pydict(
        {
            "mykey": arr,
            "data": [long_A for _ in range(num_samples)],
        }
    ).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.sort("mykey").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert (result.to_pandas()["mykey"].to_numpy() == np.arange(num_samples)).all()
    assert result.groupby("data").agg([("mykey", "count")]).to_pydict() == {
        "data": [long_A],
        "mykey": [num_samples],
    }


@pytest.mark.benchmark(group="sorts")
@pytest.mark.parametrize("num_columns", [1, 4])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_multicolumn_sort(benchmark, num_columns, num_samples) -> None:
    """Evaluate the performance impact of sorting multiple columns.

    Each additional sort column increases the sort key cardinality by approximately the same factor of ~10
    (i.e. each additional sort column should be doing around the same additional amount of work).

    Using the last column produces a unique sort key.
    """

    arr = np.arange(num_samples)
    np.random.shuffle(arr)

    df = DataFrame.from_pydict(
        {
            # all coprime
            "nums_9": arr * 7 % 9,
            "nums_10": arr * 7 % 10,
            "nums_11": arr * 7 % 11,
            "nums": arr,
        }
    ).collect()

    # Run the benchmark.
    sort_on = ["nums_9", "nums_10", "nums_11", "nums"][-num_columns:]

    def bench() -> DataFrame:
        return df.sort(sort_on).collect()

    benchmark(bench)
