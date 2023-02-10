from __future__ import annotations

import numpy as np
import pytest

from daft import DataFrame


@pytest.mark.skip(reason="Pending fix: https://github.com/Eventual-Inc/Daft/pull/565")
@pytest.mark.benchmark(group="join")
def test_join_groupby_agg_sort_limit(benchmark) -> None:
    np.random.seed(42)
    num_samples = 1_000_000
    word_df = DataFrame.from_pydict(
        {
            "doc_id": np.random.choice(np.arange(400, dtype=np.int64), size=(num_samples,)).tolist(),
            "word": np.random.choice(np.arange(15000, dtype=np.int64), size=(num_samples,)).tolist(),
        }
    ).collect()

    def bench_join() -> DataFrame:
        return (
            word_df.join(
                word_df,
                on="word",
            )
            .groupby("doc_id")
            .agg([(word_df["doc_id"].alias("doc_count"), "count")])
            .sort(
                "doc_count",
                desc=True,
            )
            .limit(10)
            .collect()
        )

    result = benchmark(bench_join)
    assert result.to_pydict() == {}


def test_join_rhs_high_cardinality(benchmark) -> None:
    lhs = DataFrame.from_pydict({"v1": list(range(1000)), "v2": list(range(1000))})
    rhs = DataFrame.from_pydict({"v1": list(range(10_000_000)), "v2": list(range(10_000_000))})

    def bench_join() -> DataFrame:
        return lhs.join(rhs, on="v1").groupby("v2", "right.v2").count().sort("v2").limit(5).collect()

    result = benchmark(bench_join)
    assert result.to_pydict() == {"v1": [1, 1, 1, 1, 1], "v2": [0, 1, 2, 3, 4], "right.v2": [0, 1, 2, 3, 4]}


@pytest.mark.skip(reason="Pending fix: https://github.com/Eventual-Inc/Daft/pull/565")
def test_join_lhs_arithmetic(benchmark) -> None:
    lhs = DataFrame.from_pydict({"v1": list(range(10_000)), "v2": list(range(10_000))})
    rhs = DataFrame.from_pydict({"v1": list(range(10_000_000)), "v2": list(range(10_000_000))})

    def bench_join() -> DataFrame:
        return (
            lhs.join(rhs, on="v1")
            .select((lhs["v1"] > 50).if_else(lhs["v1"] + lhs["v2"], lhs["v1"] * lhs["v2"]))
            .collect()
        )

    result = benchmark(bench_join)
    assert result.to_pydict() == {}
