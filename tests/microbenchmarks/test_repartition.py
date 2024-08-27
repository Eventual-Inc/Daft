from __future__ import annotations

import numpy as np
import pytest

import daft
from daft import DataFrame

NUM_SAMPLES = 10_000_000
NUM_PARTITIONS = 100


@pytest.mark.benchmark(group="partitioning")
@pytest.mark.parametrize("end_partitions", [NUM_PARTITIONS])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_split(benchmark, num_samples, end_partitions) -> None:
    """Test performance of splitting into multiple partitions."""
    df = daft.from_pydict({"mycol": np.arange(num_samples)}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.into_partitions(end_partitions).collect()

    benchmark(bench)


@pytest.mark.benchmark(group="partitioning")
@pytest.mark.parametrize("start_partitions", [NUM_PARTITIONS])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_coalesce(benchmark, num_samples, start_partitions) -> None:
    """Test performance of coalescing partitions."""
    df = daft.from_pydict({"mycol": np.arange(num_samples)}).into_partitions(start_partitions).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.into_partitions(1).collect()

    benchmark(bench)


@pytest.mark.benchmark(group="partitioning")
@pytest.mark.parametrize("end_partitions", [NUM_PARTITIONS])
@pytest.mark.parametrize("start_partitions", [NUM_PARTITIONS])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_repartition_random(benchmark, num_samples, start_partitions, end_partitions) -> None:
    """Test performance of random repartitioning."""
    df = daft.from_pydict({"mycol": np.arange(num_samples)}).into_partitions(start_partitions).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.repartition(end_partitions).collect()

    benchmark(bench)


@pytest.mark.benchmark(group="partitioning")
@pytest.mark.parametrize("distribution", ["uniform", "skewed"])
@pytest.mark.parametrize("end_partitions", [NUM_PARTITIONS])
@pytest.mark.parametrize("start_partitions", [1, NUM_PARTITIONS])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_repartition_hash(benchmark, num_samples, start_partitions, end_partitions, distribution) -> None:
    """Test performance of hash repartitioning."""
    data = np.arange(num_samples)
    if distribution == "skewed":
        data[: num_samples // 2] = 0

    np.random.shuffle(data)

    df = daft.from_pydict({"mycol": data}).into_partitions(start_partitions).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.repartition(end_partitions, "mycol").collect()

    benchmark(bench)
