from __future__ import annotations

import pytest

import daft
from daft.internal.rule_runner import FixedPointPolicy, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import PushDownLimit
from tests.optimizer.conftest import assert_plan_eq


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "push_down_limit",
                FixedPointPolicy(3),
                [PushDownLimit()],
            )
        ]
    )


def test_limit_pushdown_repartition(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized_df = df.repartition(3).limit(1)
    optimized_df = df.limit(1).repartition(3)
    assert_plan_eq(optimizer(unoptimized_df._get_current_builder()._plan), optimized_df._get_current_builder()._plan)


def test_limit_pushdown_projection(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    unoptimized_df = df.select("variety").limit(1)
    optimized_df = df.limit(1).select("variety")
    assert_plan_eq(optimizer(unoptimized_df._get_current_builder()._plan), optimized_df._get_current_builder()._plan)
