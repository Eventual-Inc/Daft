from typing import Dict, List

import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical import logical_plan, optimizer
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import (
    FoldProjections,
    PushDownClausesIntoScan,
    PushDownPredicates,
)
from daft.logical.schema import ExpressionList


@pytest.fixture(scope="function")
def optimizer() -> RuleRunner[LogicalPlan]:
    return RuleRunner(
        [RuleBatch("push_into_scan", Once, [PushDownPredicates(), FoldProjections(), PushDownClausesIntoScan()])]
    )


def test_filter_scan_pushdown(valid_data: List[Dict[str, float]], optimizer) -> None:
    predicate_expr = col("sepal_length") > 4.8
    df = DataFrame.from_pylist(valid_data)

    original_schema = df.schema()
    df = df.where(predicate_expr)

    optimized = optimizer.optimize(df.plan())
    expected = logical_plan.Scan(original_schema, predicate=ExpressionList([predicate_expr]), columns=None)
    assert isinstance(optimized, logical_plan.Scan)
    assert optimized.is_eq(expected)


def test_projection_scan_pushdown(valid_data: List[Dict[str, float]], optimizer) -> None:
    selected_columns = ["sepal_length", "sepal_width"]
    df = DataFrame.from_pylist(valid_data)
    original_schema = df.schema()
    df = df.select(*selected_columns)
    assert df.column_names() == selected_columns

    optimized = optimizer.optimize(df.plan())
    expected = logical_plan.Scan(original_schema, predicate=None, columns=selected_columns)
    assert optimized.is_eq(expected)
