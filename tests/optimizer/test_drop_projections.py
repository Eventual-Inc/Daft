from __future__ import annotations

import pytest

import daft
from daft import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import DropProjections
from tests.optimizer.conftest import assert_plan_eq


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [
            RuleBatch(
                "drop_projections",
                Once,
                [DropProjections()],
            )
        ]
    )


def test_drop_projections(valid_data: list[dict[str, float]], optimizer) -> None:
    df = daft.from_pylist(valid_data)
    projection_df = df.select("petal_length", "petal_width", "sepal_length", "sepal_width", "variety")
    assert_plan_eq(optimizer(projection_df._get_current_builder()._plan), df._get_current_builder()._plan)


@pytest.mark.parametrize(
    "selection",
    [
        # Projection changes the columns on the schema
        ["variety"],
        # Projection runs operation on the schema
        ["variety", "petal_width", "petal_length", "sepal_width", col("sepal_length") + 1],
        # Projection changes ordering of columns on the schema
        ["variety", "petal_length", "petal_width", "sepal_length", "sepal_width"],
        # Projection changes names
        ["petal_length", "petal_width", "sepal_length", "sepal_width", col("variety").alias("foo")],
    ],
)
def test_cannot_drop_projections(valid_data: list[dict[str, float]], selection, optimizer) -> None:
    df = daft.from_pylist(valid_data)
    projection_df = df.select(*selection)
    assert_plan_eq(optimizer(projection_df._get_current_builder()._plan), projection_df._get_current_builder()._plan)
