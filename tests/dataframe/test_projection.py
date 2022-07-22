from typing import Dict, List

import pytest

from daft.dataframe import DataFrame
from daft.logical import optimizer
from tests.dataframe.utils import optimize_plan

# TODO(jay): Add tests for:
# 1. df.select().select() -> Projection(projection=projection_1 & projection_2)


def test_select_dataframe(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    df = df.select("sepal_length", "sepal_width")
    assert [c.name() for c in df.schema()] == ["sepal_length", "sepal_width"]


def test_select_dataframe_missing_col(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df = df.select("foo", "sepal_length")


def test_fold_projections(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    df_unoptimized = df.select("sepal_length", "sepal_width").select("sepal_width")
    df_optimized = df.select("sepal_width")

    assert [c.name() for c in df_unoptimized.schema()] == ["sepal_width"]
    assert optimize_plan(df_unoptimized.explain(), [optimizer.FoldProjections()]) == df_optimized.explain()
