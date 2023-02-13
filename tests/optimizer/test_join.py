from __future__ import annotations

import pytest

from daft.dataframe import DataFrame
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import PruneColumns


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner([RuleBatch("push_into_join", Once, [PruneColumns()])])


def test_self_join_groupby_aggregate(optimizer) -> None:
    df = DataFrame.from_pydict({"a": [1, 2, 3], "b": [1, 2, 3]})
    unoptimized_df = (
        df.join(
            df,
            on="a",
        )
        .groupby("b")
        .agg([(df["b"].alias("b_count"), "count")])
    )

    optimized = optimizer(unoptimized_df.plan())

    optimized_df = (
        df.join(
            df.select("a"),
            on="a",
        )
        .select("b")
        .groupby("b")
        .agg([(df["b"].alias("b_count"), "count")])
    )
    assert optimized.is_eq(optimized_df.plan())


def test_join_same_names(optimizer) -> None:
    lhs = DataFrame.from_pydict({"v1": list(range(10)), "v2": list(range(10))})
    rhs = DataFrame.from_pydict({"v1": list(range(100)), "v2": list(range(100))})
    unoptimized_join_df = (
        lhs.join(rhs, on="v1").select((lhs["v1"] > 50).if_else(lhs["v1"] + lhs["v2"], lhs["v1"] * lhs["v2"])).collect()
    )

    optimized = optimizer(unoptimized_join_df.plan())
    optimized_join_df = (
        lhs.join(rhs.select("v1"), on="v1")
        .select((lhs["v1"] > 50).if_else(lhs["v1"] + lhs["v2"], lhs["v1"] * lhs["v2"]))
        .collect()
    )

    assert optimized.is_eq(optimized_join_df.plan())
