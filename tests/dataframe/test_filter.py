from __future__ import annotations

import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import PushDownPredicates


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "pred_pushdown",
                Once,
                [
                    PushDownPredicates(),
                ],
            )
        ]
    )


def test_filter_pushdown_select(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


def test_filter_pushdown_with_column(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.with_column("foo", col("sepal_length") + 1).where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).with_column("foo", col("sepal_length") + 1)
    assert unoptimized.column_names == [*df.column_names, "foo"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


@pytest.mark.skip(reason="Currently fails until we implement breaking up & expressions into expression lists")
def test_filter_merge(valid_data: list[dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.where(col("sepal_length") > 4.8).where(col("sepal_width") > 2.4)
    optimized = df.where((col("sepal_width") > 2.4) & (col("sepal_length") > 4.8))
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


def test_filter_missing_column(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_length", "sepal_width").where(col("petal_length") > 4.8)


def test_filter_pushdown_sort(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.sort("sepal_length").select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).sort("sepal_length").select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())
