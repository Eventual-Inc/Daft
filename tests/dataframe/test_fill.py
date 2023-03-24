from __future__ import annotations

from typing import Any

from daft import DataFrame


def test_fill_nan_data_frame(missing_value_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(missing_value_data)
    filled_df = df[["petal_length", "sepal_width"]].fillnan(3.1)
    pd_df = filled_df.to_pandas()
    assert pd_df.loc[1, "sepal_width"] == 3.1


def test_fill_nan_expression(missing_value_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(missing_value_data)
    pd_df = df.with_column("sepal_width", df["sepal_width"].fillnan(3.1)).to_pandas()
    assert pd_df.loc[1, "sepal_width"] == 3.1


def test_fill_null_data_frame(missing_value_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(missing_value_data)
    filled_df = df[["petal_length", "sepal_length"]].fillnull(3.1)
    pd_df = filled_df.to_pandas()
    assert pd_df.loc[0, "sepal_length"] == 3.1


def test_fill_null_expression(missing_value_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(missing_value_data)
    pd_df = df.with_column("sepal_length", df["sepal_length"].fillnull(3.1)).to_pandas()
    assert pd_df.loc[0, "sepal_length"] == 3.1
