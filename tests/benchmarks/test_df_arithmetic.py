from __future__ import annotations

import numpy as np
import pytest

from daft import DataFrame


@pytest.mark.aggregations
@pytest.fixture(scope="module")
def gen_aranged_df(num_samples=1_000_000) -> DataFrame:
    return DataFrame.from_pydict(
        {
            "i": ((np.arange(num_samples, dtype=np.int64) * 9582398353) % 100).tolist(),
            "j": ((np.arange(num_samples, dtype=np.int64) * 847892347987) % 100).tolist(),
        }
    ).collect()


@pytest.mark.benchmark(group="aggregations")
def test_integer_multiplications(gen_aranged_df, benchmark) -> None:
    def bench_sum() -> DataFrame:
        return (
            gen_aranged_df.with_column(
                "k",
                (gen_aranged_df["i"] * gen_aranged_df["j"])
                + (gen_aranged_df["i"] * gen_aranged_df["j"])
                + (gen_aranged_df["i"] * gen_aranged_df["j"])
                + (gen_aranged_df["i"] * gen_aranged_df["j"]),
            )
            .min("k")
            .collect()
        )

    result = benchmark(bench_sum)
    assert result.to_pydict()["k"] == [0]
