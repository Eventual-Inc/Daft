from __future__ import annotations


def test_with_columns_renamed(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.with_columns_renamed({"sepal_width": "sepal_width_2", "petal_length": "petal_length_2"})
    assert df.column_names == ["sepal_length", "sepal_width_2", "petal_length_2", "petal_width", "variety"]


def test_with_columns_renamed_same_name(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.with_columns_renamed({"sepal_width": "sepal_width"})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_with_columns_renamed_empty(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.with_columns_renamed({})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_with_columns_renamed_nonexistent_column(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.with_columns_renamed({"sepal_length_2": "sepal_length"})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]
