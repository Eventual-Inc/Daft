from __future__ import annotations

import pytest

import daft
from daft import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import DropRepartition
from tests.optimizer.conftest import assert_plan_eq


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "drop_repartitions",
                Once,
                [DropRepartition()],
            )
        ]
    )


def test_drop_unneeded_repartition(valid_data: list[dict[str, float]], tmpdir, optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df = df.repartition(2)
    df = df.groupby("variety").agg([("sepal_length", "mean")])
    repartitioned_df = df.repartition(2, df["variety"])
    assert_plan_eq(optimizer(repartitioned_df._get_current_builder()._plan), df._get_current_builder()._plan)


def test_drop_single_repartitions(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized_df = df.repartition(1, "variety")
    assert_plan_eq(optimizer(unoptimized_df._get_current_builder()._plan), df._get_current_builder()._plan)


def test_drop_double_repartition(valid_data: list[dict[str, float]], tmpdir, optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized_df = df.repartition(2).repartition(3)
    optimized_df = df.repartition(3)
    assert_plan_eq(optimizer(unoptimized_df._get_current_builder()._plan), optimized_df._get_current_builder()._plan)


@pytest.mark.skip(reason="Broken on issue: https://github.com/Eventual-Inc/Daft/issues/596")
def test_repartition_alias(valid_data: list[dict[str, float]], tmpdir, optimizer) -> None:
    df = daft.from_pylist(valid_data)
    df = df.repartition(2, "variety").select(col("sepal_length").alias("variety")).repartition(2, "variety")
    assert_plan_eq(optimizer(df._get_current_builder()._plan), df._get_current_builder()._plan)
