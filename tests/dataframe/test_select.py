from __future__ import annotations

import pytest


def test_select_dataframe_missing_col(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)

    with pytest.raises(ValueError):
        df = df.select("foo", "sepal_length")


def test_select_dataframe(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.select("sepal_length", "sepal_width")
    assert df.column_names == ["sepal_length", "sepal_width"]


def test_multiple_select_same_col(make_df, valid_data: list[dict[str, float]]):
    df = make_df(valid_data)
    df = df.select(df["sepal_length"], df["sepal_length"].alias("sepal_length_2"))
    pdf = df.to_pandas()
    assert len(pdf.columns) == 2
    assert pdf.columns.to_list() == ["sepal_length", "sepal_length_2"]


def test_select_ordering(make_df, valid_data: list[dict[str, float]]):
    df = make_df(valid_data)
    df = df.select(
        df["variety"], df["petal_length"].alias("foo"), df["sepal_length"], df["sepal_width"], df["petal_width"]
    )
    df = df.collect()
    assert df.column_names == ["variety", "foo", "sepal_length", "sepal_width", "petal_width"]
