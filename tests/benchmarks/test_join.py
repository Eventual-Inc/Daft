from __future__ import annotations

import numpy as np
import pytest

from daft import DataFrame


@pytest.mark.skip(reason="[RUST-INT] This currently hangs and needs to be investigated")
@pytest.mark.benchmark(group="join")
def test_join_groupby_agg_sort_limit(benchmark) -> None:
    """Hash Join where RHS has no projection

    Adapted from: https://github.com/duckdb/duckdb/blob/master/benchmark/micro/join/hashjoin_benno_norhsfetch.benchmark
    """
    num_samples = 1_000_000
    word_df = DataFrame.from_pydict(
        {
            "a": np.arange(num_samples, dtype=np.int64) % 5000,
            "b": np.arange(num_samples, dtype=np.int64) % 15000,
        }
    ).collect()

    def bench_join() -> DataFrame:
        return (
            word_df.join(
                word_df,
                on="b",
            )
            .groupby("a")
            .agg([(word_df["a"].alias("a_sum"), "sum")])
            .sort(
                "a_sum",
                desc=True,
            )
            .limit(10)
            .collect()
        )

    result = benchmark(bench_join)
    assert result.to_pydict() == {
        "a": [4999, 4998, 4997, 4996, 4995, 4994, 4993, 4992, 4991, 4990],
        "a_sum": [
            66656666,
            66643332,
            66629998,
            66616664,
            66603330,
            66589996,
            66576662,
            66563328,
            66549994,
            66536660,
        ],
    }


@pytest.mark.skip(reason="[RUST-INT] This currently hangs and needs to be investigated")
@pytest.mark.benchmark(group="join")
def test_join_rhs_high_cardinality(benchmark) -> None:
    """Hash Join where RHS has high cardinality

    Adapted from: https://github.com/duckdb/duckdb/blob/master/benchmark/micro/join/hashjoin_highcardinality.benchmark
    """
    lhs = DataFrame.from_pydict({"v1": np.arange(1000), "v2": np.arange(1000)})
    rhs = DataFrame.from_pydict({"v1": np.arange(10_000_000), "v2": np.arange(10_000_000)})

    def bench_join() -> DataFrame:
        return lhs.join(rhs, on="v1").groupby("v2", "right.v2").count().sort("v2").limit(5).collect()

    result = benchmark(bench_join)
    assert result.to_pydict() == {"v1": [1, 1, 1, 1, 1], "v2": [0, 1, 2, 3, 4], "right.v2": [0, 1, 2, 3, 4]}


@pytest.mark.skip(reason="[RUST-INT] This currently hangs and needs to be investigated")
@pytest.mark.benchmark(group="join")
def test_join_lhs_arithmetic(benchmark) -> None:
    """Hash Join where LHS performs if_else operation

    Adapted from: https://github.com/duckdb/duckdb/blob/master/benchmark/micro/join/hashjoin_lhsarithmetic.benchmark
    """
    lhs = DataFrame.from_pydict({"v1": np.arange(10_000), "v2": np.arange(10_000)})
    rhs = DataFrame.from_pydict({"v1": np.arange(10_000_000), "v2": np.arange(10_000_000)})

    def bench_join() -> DataFrame:
        return (
            lhs.join(rhs, on="v1")
            .select((lhs["v1"] > 50).if_else(lhs["v1"] + lhs["v2"], lhs["v1"] * lhs["v2"]))
            .collect()
        )

    result = benchmark(bench_join)
    assert result.to_pydict() == {
        "v1": [i**2 for i in range(51)] + [i * 2 for i in range(51, 10000)],
    }


@pytest.mark.skip(reason="[RUST-INT] This currently hangs and needs to be investigated")
@pytest.mark.benchmark(group="join")
def test_many_inner_joins(benchmark) -> None:
    """Tests many inner joins

    Adapted from: https://github.com/duckdb/duckdb/blob/master/benchmark/micro/join/many_inner_joins.benchmark
    """
    n_rows = 1_000_000
    main_df = DataFrame.from_pydict({"id": np.arange(n_rows), **{f"value{i}_id": np.arange(n_rows) for i in range(20)}})
    value_df = DataFrame.from_pydict({"id": np.arange(n_rows), "value": np.arange(n_rows)})

    def bench_join() -> DataFrame:
        df = main_df
        for i in range(20):
            df = df.join(value_df, left_on=f"value{i}_id", right_on="id")
        return df.select("id").collect()

    result = benchmark(bench_join)
    assert result.to_pydict() == {"id": list(range(n_rows))}
