from typing import Dict, List

import pytest

from daft.dataframe import DataFrame
from daft.datasources import InMemorySourceInfo
from daft.expressions import col
from daft.internal.rule_runner import Once, RuleBatch, RuleRunner
from daft.logical import logical_plan
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

    optimized = optimizer(df.plan())
    expected = logical_plan.Scan(
        schema=original_schema,
        predicate=ExpressionList([predicate_expr]),
        columns=None,
        source_info=InMemorySourceInfo(data={header: [row[header] for row in valid_data] for header in valid_data[0]}),
    )
    assert isinstance(optimized, logical_plan.Scan)
    assert optimized.is_eq(expected)


def test_projection_scan_pushdown(valid_data: List[Dict[str, float]], optimizer) -> None:
    selected_columns = ["sepal_length", "sepal_width"]
    df = DataFrame.from_pylist(valid_data)
    original_schema = df.schema()
    df = df.select(*selected_columns)
    assert df.column_names() == selected_columns

    optimized = optimizer(df.plan())
    expected = logical_plan.Scan(
        schema=original_schema,
        predicate=None,
        columns=selected_columns,
        source_info=InMemorySourceInfo(data={header: [row[header] for row in valid_data] for header in valid_data[0]}),
    )
    assert optimized.is_eq(expected)
