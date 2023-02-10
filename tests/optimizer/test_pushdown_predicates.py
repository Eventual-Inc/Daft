from __future__ import annotations

import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import Filter, Join, LogicalPlan
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


def test_no_pushdown_on_modified_column(optimizer) -> None:
    df = DataFrame.from_pydict({"ints": [i for i in range(3)], "ints_dup": [i for i in range(3)]})
    df = df.with_column(
        "modified",
        col("ints_dup") + 1,
    ).where(col("ints") == col("modified").alias("ints_dup"))

    # Optimizer cannot push down the filter because it uses a column that was projected
    assert optimizer(df.plan()).is_eq(df.plan())


def test_filter_pushdown_select(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


def test_filter_pushdown_select_alias(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.select("sepal_length", "sepal_width").where(col("sepal_length").alias("foo") > 4.8)
    optimized = df.where(col("sepal_length").alias("foo") > 4.8).select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


def test_filter_pushdown_with_column(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.with_column("foo", col("sepal_length") + 1).where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).with_column("foo", col("sepal_length") + 1)
    assert unoptimized.column_names == [*df.column_names, "foo"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


def test_filter_pushdown_with_column_alias(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.with_column("foo", col("sepal_length").alias("foo") + 1).where(
        col("sepal_length").alias("foo") > 4.8
    )
    optimized = df.where(col("sepal_length").alias("foo") > 4.8).with_column(
        "foo", col("sepal_length").alias("foo") + 1
    )
    assert unoptimized.column_names == [*df.column_names, "foo"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


def test_filter_merge(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.where(col("sepal_length") > 4.8).where(col("sepal_width") > 2.4)
    optimized = df.where((col("sepal_width") > 2.4) & (col("sepal_length") > 4.8))
    assert optimizer(unoptimized.plan()).is_eq(
        optimized.plan()
    ), f"Expected:\n{optimized.plan()}\n\n--------\n\nReceived:\n{optimizer(unoptimized.plan())}"


def test_filter_pushdown_sort(valid_data: list[dict[str, float]], optimizer) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.sort("sepal_length").select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).sort("sepal_length").select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert optimizer(unoptimized.plan()).is_eq(optimized.plan())


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
