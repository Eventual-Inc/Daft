from typing import Dict, List

import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.logical import optimizer
from tests.dataframe.utils import optimize_plan


def test_filter_pushdown(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.select("sepal_length", "sepal_width").where(col("sepal_length") > 4.8)
    optimized = df.where(col("sepal_length") > 4.8).select("sepal_length", "sepal_width")
    assert [c.name() for c in unoptimized.schema()] == ["sepal_length", "sepal_width"]
    assert optimize_plan(unoptimized.explain(), [optimizer.PushDownPredicates()]) == optimized.explain()


def test_filter_merge(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    unoptimized = df.where(col("sepal_length") > 4.8).where(col("sepal_width") > 2.4)
    optimized = df.where(col("sepal_length") > 4.8 & col("sepal_width") > 2.4)
    assert optimize_plan(unoptimized.explain(), [optimizer.PushDownPredicates()]) == optimized.explain()


def test_filter_missing_column(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_length", "sepal_width").where(col("petal_length") > 4.8)
