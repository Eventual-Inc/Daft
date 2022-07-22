from typing import Dict, List

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.logical import logical_plan, optimizer
from daft.logical.schema import ExpressionList

from .utils import optimize_plan


def test_filter_scan_pushdown(valid_data: List[Dict[str, float]]) -> None:
    predicate_expr = col("sepal_length") > 4.8
    df = DataFrame.from_pylist(valid_data)
    df = df.where(predicate_expr)

    optimized = optimize_plan(df.explain(), [optimizer.PushDownClausesIntoScan()])
    expected = logical_plan.Scan(df.schema(), predicate=ExpressionList([predicate_expr]), columns=None)
    assert isinstance(optimized, logical_plan.Scan)
    assert optimized == expected


def test_projection_scan_pushdown(valid_data: List[Dict[str, float]]) -> None:
    selected_columns = ["sepal_length", "sepal_width"]
    df = DataFrame.from_pylist(valid_data)
    df = df.select(*selected_columns)
    assert [c.name() for c in df.schema()] == selected_columns

    optimized = optimize_plan(df.explain(), [optimizer.PushDownClausesIntoScan()])
    expected = logical_plan.Scan(df.schema(), predicate=None, columns=selected_columns)
    assert optimized == expected
