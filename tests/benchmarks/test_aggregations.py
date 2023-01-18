from __future__ import annotations

import numpy as np
import pytest

from daft import DataFrame


@pytest.mark.aggregations
@pytest.fixture(scope="module")
def gen_aranged_df(num_samples=1_000_000_000) -> DataFrame:
    return DataFrame.from_pydict({"x": np.arange(num_samples, dtype=np.int32)}).collect()


@pytest.mark.benchmark(group="aggregations")
def test_single_int32_column_sum(gen_aranged_df, benchmark) -> None:
    def bench_sum() -> DataFrame:
        return gen_aranged_df.sum("x").collect()

    result = benchmark(bench_sum)
    total_count = 1_000_000_000 - 1
    total_sum = total_count * (total_count + 1) / 2
    assert (result.to_pandas()["x"] == total_sum).all()


@pytest.mark.benchmark(group="aggregations")
def test_single_int32_column_mean(gen_aranged_df, benchmark) -> None:
    def bench_mean() -> DataFrame:
        return gen_aranged_df.mean("x").collect()

    result = benchmark(bench_mean)
    total_count = 1_000_000_000 - 1
    total_mean = total_count / 2.0
    assert (result.to_pandas()["x"] == total_mean).all()
