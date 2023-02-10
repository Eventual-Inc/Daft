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
    df = (
        df.join(
            df,
            on="a",
        )
        .groupby("b")
        .agg([(df["b"].alias("b_count"), "count")])
    )

    # No optimization should be possible
    optimized = optimizer(df.plan())
    assert optimized.is_eq(df.plan())


def test_join_same_names(optimizer) -> None:
    lhs = DataFrame.from_pydict({"v1": list(range(10)), "v2": list(range(10))})
    rhs = DataFrame.from_pydict({"v1": list(range(100)), "v2": list(range(100))})
    joined = (
        lhs.join(rhs, on="v1").select((lhs["v1"] > 50).if_else(lhs["v1"] + lhs["v2"], lhs["v1"] * lhs["v2"])).collect()
    )

    # TODO: No optimization made, but in the future could do a pushdown into the LHS dataframe
    optimized = optimizer(joined.plan())
    assert optimized.is_eq(joined.plan())
