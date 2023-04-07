from __future__ import annotations

import numpy as np
import pytest

from daft import DataFrame

NUM_SAMPLES = 10_000_000
NUM_PARTITIONS = 100


@pytest.mark.benchmark(group="partitioning")
@pytest.mark.parametrize("end_partitions", [NUM_PARTITIONS])
@pytest.mark.parametrize("num_samples", [NUM_SAMPLES])
def test_split(benchmark, num_samples, end_partitions) -> None:
    """Test performance of splitting into multiple partitions."""
    df = DataFrame.from_pydict({"mycol": np.arange(num_samples)}).collect()

    # Run the benchmark.
    def bench() -> DataFrame:
        return df.into_partitions(end_partitions).collect()

    benchmark(bench)
