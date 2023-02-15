from __future__ import annotations

import numpy as np
import pytest

from daft import DataFrame


@pytest.fixture(scope="module", params=[(1, 64), (8, 8), (64, 1)], ids=["1x64mib", "8x8mib", "64x1mib"])
def gen_aranged_df(request) -> DataFrame:
    num_partitions, mibs_per_partition = request.param

    total_mibs = num_partitions * mibs_per_partition
    num_samples = int((total_mibs * 1024 * 1024) / 4)

    return DataFrame.from_pydict({"x": np.arange(num_samples, dtype=np.int32)}).repartition(num_partitions).collect()


@pytest.mark.benchmark(group="aggregations")
def test_single_int32_column_sum(gen_aranged_df, benchmark) -> None:
    def bench_sum() -> DataFrame:
        return gen_aranged_df.sum("x").collect()

    result = benchmark(bench_sum)
    total_count = len(gen_aranged_df)
    total_sum = total_count * (total_count - 1) / 2
    assert (result.to_pandas()["x"] == total_sum).all()


@pytest.mark.benchmark(group="aggregations")
def test_single_int32_column_mean(gen_aranged_df, benchmark) -> None:
    def bench_mean() -> DataFrame:
        return gen_aranged_df.mean("x").collect()

    result = benchmark(bench_mean)
    total_count = len(gen_aranged_df)
    total_mean = (total_count - 1) / 2.0
    assert (result.to_pandas()["x"] == total_mean).all()
