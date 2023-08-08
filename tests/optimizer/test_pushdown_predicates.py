from __future__ import annotations

import pytest

import daft
from daft.expressions import ExpressionsProjection, col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import Concat, Filter, Join, LogicalPlan
from daft.logical.optimizer import PushDownPredicates
from tests.optimizer.conftest import assert_plan_eq


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
    df = daft.from_pydict({"ints": [i for i in range(3)], "ints_dup": [i for i in range(3)]})
    df = df.with_column(
        "modified",
        col("ints_dup") + 1,
    ).where(col("ints") == col("modified").alias("ints_dup"))

    # Optimizer cannot push down the filter because it uses a column that was projected
    assert_plan_eq(optimizer(df._get_current_builder()._plan), df._get_current_builder()._plan)


def test_filter_pushdown_select(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_pushdown_select_alias(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.select("sepal_length", "sepal_width").where(col("sepal_length").alias("foo") > 4.8)
    optimized = df.where(col("sepal_length").alias("foo") > 4.8).select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_pushdown_with_column(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.with_column("foo", col("sepal_length") + 1).where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).with_column("foo", col("sepal_length") + 1)
    assert unoptimized.column_names == [*df.column_names, "foo"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_pushdown_with_column_partial_predicate_pushdown(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = (
        df.with_column("foo", col("sepal_length") + 1).where(col("sepal_length") > 4.8).where(col("foo") > 4.8)
    )
    optimized = df.where(col("sepal_length") > 4.8).with_column("foo", col("sepal_length") + 1).where(col("foo") > 4.8)
    assert unoptimized.column_names == [*df.column_names, "foo"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_pushdown_with_column_alias(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.with_column("foo", col("sepal_length").alias("foo") + 1).where(
        col("sepal_length").alias("foo") > 4.8
    )
    optimized = df.where(col("sepal_length").alias("foo") > 4.8).with_column(
        "foo", col("sepal_length").alias("foo") + 1
    )
    assert unoptimized.column_names == [*df.column_names, "foo"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_merge(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.where((col("sepal_length") > 4.8).alias("foo")).where((col("sepal_width") > 2.4).alias("foo"))

    # HACK: We manually modify the plan here because currently CombineFilters works by combining predicates as an ExpressionsProjection rather than taking the & of the two predicates
    DUMMY = col("sepal_width") > 100
    EXPECTED = ExpressionsProjection(
        [(col("sepal_width") > 2.4).alias("foo"), (col("sepal_length") > 4.8).alias("foo").alias("copy.foo")]
    )
    optimized = df.where(DUMMY)
    optimized._get_current_builder()._plan._predicate = EXPECTED

    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_pushdown_sort(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.sort("sepal_length").select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).sort("sepal_length").select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_pushdown_repartition(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized = df.repartition(2).select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).repartition(2).select("sepal_length", "sepal_width")
    assert unoptimized.column_names == ["sepal_length", "sepal_width"]
    assert_plan_eq(optimizer(unoptimized._get_current_builder()._plan), optimized._get_current_builder()._plan)


def test_filter_join_pushdown(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("sepal_length") > 4.8)
    filtered = filtered.where(col("right.sepal_width") > 4.8)

    optimized = optimizer(filtered._get_current_builder()._plan)

    expected = df1.where(col("sepal_length") > 4.8).join(df2.where(col("sepal_width") > 4.8), on="variety")
    assert isinstance(optimized, Join)
    assert isinstance(expected._get_current_builder()._plan, Join)
    assert_plan_eq(optimized, expected._get_current_builder()._plan)


def test_filter_join_pushdown_aliases(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("sepal_length").alias("foo") > 4.8)
    filtered = filtered.where(col("right.sepal_width").alias("foo") > 4.8)

    optimized = optimizer(filtered._get_current_builder()._plan)

    expected = df1.where(
        # Filter merging creates a `copy.*` column when merging predicates with the same name
        (col("sepal_length").alias("foo") > 4.8).alias("copy.foo")
    ).join(df2.where(col("sepal_width").alias("foo") > 4.8), on="variety")
    assert isinstance(optimized, Join)
    assert isinstance(expected._get_current_builder()._plan, Join)
    assert_plan_eq(optimized, expected._get_current_builder()._plan)


def test_filter_join_pushdown_nonvalid(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("right.sepal_width") > col("sepal_length"))

    optimized = optimizer(filtered._get_current_builder()._plan)

    assert isinstance(optimized, Filter)
    assert_plan_eq(optimized, filtered._get_current_builder()._plan)


def test_filter_join_pushdown_nonvalid_aliases(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("right.sepal_width").alias("sepal_width") > col("sepal_length"))

    optimized = optimizer(filtered._get_current_builder()._plan)

    assert isinstance(optimized, Filter)
    assert_plan_eq(optimized, filtered._get_current_builder()._plan)


def test_filter_join_partial_predicate_pushdown(valid_data: list[dict[str, float]], optimizer) -> None:
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)

    joined = df1.join(df2, on="variety")

    filtered = joined.where(col("sepal_length") > 4.8)
    filtered = filtered.where(col("right.sepal_width") > 4.8)
    filtered = filtered.where(((col("sepal_length") > 4.8) | (col("right.sepal_length") > 4.8)).alias("foo"))

    optimized = optimizer(filtered._get_current_builder()._plan)

    expected = (
        df1.where(col("sepal_length") > 4.8)
        .join(df2.where(col("sepal_width") > 4.8), on="variety")
        .where(((col("sepal_length") > 4.8) | (col("right.sepal_length") > 4.8)).alias("foo"))
    )
    assert isinstance(optimized, Filter)
    assert isinstance(expected._get_current_builder()._plan, Filter)
    assert_plan_eq(optimized, expected._get_current_builder()._plan)


def test_filter_concat_predicate_pushdown(valid_data, optimizer) -> None:
    df1 = daft.from_pylist(valid_data)
    df2 = daft.from_pylist(valid_data)
    concatted = df1.concat(df2)
    filtered = concatted.where(col("sepal_length") > 4.8)
    optimized = optimizer(filtered._get_current_builder()._plan)

    expected = df1.where(col("sepal_length") > 4.8).concat(df2.where(col("sepal_length") > 4.8))
    assert isinstance(optimized, Concat)
    assert isinstance(expected._get_current_builder()._plan, Concat)
    assert_plan_eq(optimized, expected._get_current_builder()._plan)
