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
