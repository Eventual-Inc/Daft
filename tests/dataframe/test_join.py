from __future__ import annotations

import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import Filter, Join, LogicalPlan
from daft.logical.optimizer import PushDownPredicates


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner([RuleBatch("push_into_join", Once, [PushDownPredicates()])])


def test_filter_join_pushdown(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = DataFrame.from_pylist(valid_data)
    df2 = DataFrame.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("sepal_length") > 4.8)
    filtered = filtered.where(col("right.sepal_width") > 4.8)

    optimized = optimizer(filtered.plan())

    expected = df1.where(col("sepal_length") > 4.8).join(df2.where(col("sepal_width") > 4.8), on="variety")
    assert isinstance(optimized, Join)
    assert isinstance(expected.plan(), Join)

    assert optimized.is_eq(expected.plan())


def test_filter_join_pushdown_nonvalid(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = DataFrame.from_pylist(valid_data)
    df2 = DataFrame.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("right.sepal_width") > col("sepal_length"))

    optimized = optimizer(filtered.plan())

    assert isinstance(optimized, Filter)

    assert optimized.is_eq(filtered.plan())
