from typing import Dict, List

import pytest

from daft.dataframe import DataFrame
from daft.expressions import col
from daft.logical import optimizer
from tests.dataframe.utils import optimize_plan


def test_select_dataframe(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    df = df.select("sepal_length", "sepal_width")
    assert df.column_names() == ["sepal_length", "sepal_width"]


def test_select_dataframe_missing_col(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df = df.select("foo", "sepal_length")


def test_fold_projections(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    df_unoptimized = df.select("sepal_length", "sepal_width").select("sepal_width")
    df_optimized = df.select("sepal_width")

    assert df_unoptimized.column_names() == ["sepal_width"]
    assert optimize_plan(df_unoptimized.plan(), [optimizer.FoldProjections()]).is_eq(df_optimized.plan())


def test_with_column(valid_data: List[Dict[str, float]]) -> None:
    expr = col("sepal_length") + col("sepal_width")
    df = DataFrame.from_pylist(valid_data)
    expanded_df = df.with_column("foo", expr)
    # TODO(jay): Test that the expression with name "foo" is equal to the expected expression, except for the IDs of the columns
    assert expanded_df.column_names() == [c.name() for c in df.schema()] + ["foo"]
