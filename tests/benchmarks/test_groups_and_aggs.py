from __future__ import annotations

import random
from uuid import uuid4

import numpy as np
import pytest

import daft
from daft import DataFrame

NUM_SAMPLES = [1_000_000, 10_000_000]


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
def test_agg_baseline(benchmark, num_samples) -> None:
    """Test baseline aggregation performance.

    No groups, simplest aggregation (count).
    """
    df = daft.from_pydict({"mycol": np.arange(num_samples)}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.count("mycol").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert result.to_pydict()["mycol"][0] == num_samples


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_partitions", [2])
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
def test_agg_multipart(benchmark, num_samples, num_partitions) -> None:
    """Evaluate the impact of multiple partitions."""
    df = daft.from_pydict({"mycol": np.arange(num_samples)}).into_partitions(num_partitions).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.count("mycol").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert result.to_pydict()["mycol"][0] == num_samples


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
def test_comparable_agg(benchmark, num_samples) -> None:
    """Test aggregation performance for comparisons against string types."""

    data = [str(uuid4()) for _ in range(num_samples)] + ["ffffffff-ffff-ffff-ffff-ffffffffffff"]
    random.shuffle(data)

    df = daft.from_pydict({"mycol": data}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.max("mycol").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert result.to_pydict()["mycol"][0] == "ffffffff-ffff-ffff-ffff-ffffffffffff"


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
def test_numeric_agg(benchmark, num_samples) -> None:
    """Test aggregation performance for numeric aggregation ops."""

    df = daft.from_pydict({"mycol": np.arange(num_samples)}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.mean("mycol").collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert result.to_pydict()["mycol"][0] == (num_samples - 1) / 2.0


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_groups", [1, 10, 1000, None])
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
def test_groupby(benchmark, num_samples, num_groups) -> None:
    """Test performance of grouping to one group vs many."""

    keys = np.arange(num_samples)
    if num_groups is not None:
        keys = keys % num_groups

    np.random.shuffle(keys)

    df = daft.from_pydict(
        {
            "keys": keys,
            "data": np.arange(num_samples),
        }
    ).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.groupby("keys").agg([("data", "count")]).collect()

    result = benchmark(bench)

    # Make sure the result is correct.

    expected_len = num_groups if num_groups is not None else num_samples

    assert len(result) == expected_len

    assert (result.to_pandas()["data"].to_numpy() == (np.ones(expected_len) * (num_samples / expected_len))).all()


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_groups", [1, 10, 1_000, None])
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
def test_groupby_string(benchmark, num_samples, num_groups) -> None:
    """Test performance of grouping to one group vs many."""

    keys = np.arange(num_samples)
    if num_groups is not None:
        keys = keys % num_groups
    np.random.shuffle(keys)

    keys = [f"{i:09}" for i in keys]

    df = daft.from_pydict(
        {
            "keys": keys,
            "data": np.arange(num_samples),
        }
    ).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.groupby("keys").agg([("data", "count")]).collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    expected_len = num_groups if num_groups is not None else num_samples

    assert len(result) == expected_len
    assert (result.to_pandas()["data"].to_numpy() == (np.ones(expected_len) * (num_samples / expected_len))).all()


@pytest.mark.benchmark(group="aggregations")
@pytest.mark.parametrize("num_samples", NUM_SAMPLES)
@pytest.mark.parametrize("num_columns", [1, 2])
def test_multicolumn_groupby(benchmark, num_columns, num_samples) -> None:
    """Evaluates the impact of an additional column in the groupby.

    The group cardinality is the same in both cases;
    a redundant column is used for the multicolumn group.
    """

    num_groups = 100

    keys = np.arange(num_samples) % num_groups
    np.random.shuffle(keys)

    df = daft.from_pydict(
        {
            "keys_a": keys * 7 % 10,
            "keys": keys,
            "data": np.arange(num_samples),
        }
    ).collect()

    # Run the benchmark.
    group_cols = ["keys_a", "keys"][-num_columns:]

    def bench() -> DataFrame:
        return df.groupby(*group_cols).agg([("data", "count")]).collect()

    result = benchmark(bench)

    # Make sure the result is correct.
    assert len(result) == num_groups
    assert (result.to_pandas()["data"].to_numpy() == (np.ones(num_groups) * (num_samples / num_groups))).all()
