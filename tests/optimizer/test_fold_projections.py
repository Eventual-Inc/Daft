from __future__ import annotations

import pytest

import daft
from daft import col
from daft.daft import ResourceRequest
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import FoldProjections
from tests.optimizer.conftest import assert_plan_eq


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "fold_projections",
                Once,
                [FoldProjections()],
            )
        ]
    )


def test_fold_projections(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.select("sepal_length", "sepal_width").select("sepal_length")
    df_optimized = df.select("sepal_length")
    assert df_unoptimized.column_names == ["sepal_length"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_fold_projections_aliases(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.select(col("sepal_length").alias("foo"), "sepal_width").select(col("foo").alias("sepal_width"))
    df_optimized = df.select(col("sepal_length").alias("foo").alias("sepal_width"))

    assert df_unoptimized.column_names == ["sepal_width"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)


def test_cannot_fold_projections(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.select(col("sepal_length") + 1, "sepal_width").select("sepal_length")
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_unoptimized._get_current_builder()._plan)


def test_fold_projections_resource_requests(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df_unoptimized = df.with_column(
        "bar", col("sepal_length"), resource_request=ResourceRequest(num_cpus=1)
    ).with_column("foo", col("sepal_length"), resource_request=ResourceRequest(num_gpus=1))
    df_optimized = df.select(*df.column_names, col("sepal_length").alias("bar"), col("sepal_length").alias("foo"))
    df_optimized._get_current_builder()._plan._resource_request = ResourceRequest(num_cpus=1, num_gpus=1)

    assert df_unoptimized.column_names == [*df.column_names, "bar", "foo"]
    assert_plan_eq(optimizer(df_unoptimized._get_current_builder()._plan), df_optimized._get_current_builder()._plan)
