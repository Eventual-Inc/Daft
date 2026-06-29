from __future__ import annotations

import pytest


def test_rename(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename({"sepal_width": "sepal_width_2", "petal_length": "petal_length_2"})
    assert df.column_names == ["sepal_length", "sepal_width_2", "petal_length_2", "petal_width", "variety"]


def test_rename_same_name(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename({"sepal_width": "sepal_width"})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_rename_empty(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename({})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_rename_nonexistent_column(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename({"sepal_length_2": "sepal_length"})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_rename_kwargs(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename(sepal_width="sepal_width_2", petal_length="petal_length_2")
    assert df.column_names == ["sepal_length", "sepal_width_2", "petal_length_2", "petal_width", "variety"]


def test_rename_cols_map_as_keyword(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename(cols_map={"sepal_width": "sepal_width_2", "petal_length": "petal_length_2"})
    assert df.column_names == ["sepal_length", "sepal_width_2", "petal_length_2", "petal_width", "variety"]


def test_rename_cols_map_as_name(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    df = df.rename(variety="cols_map")
    df = df.rename(cols_map={"cols_map": "random"})
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "random"]


def test_rename_dict_and_kwargs(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    with pytest.raises(ValueError) as excinfo:
        _ = df.rename({"sepal_width": "sepal_width_2", "petal_length": "petal_length_2"}, sepal_width="sepal_width_2")
    assert "Can not pass in columns to rename as both a dict and kwargs" in str(excinfo.value)


def test_rename_empty_dict_and_kwargs(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    with pytest.raises(ValueError) as excinfo:
        _ = df.rename({}, sepal_width="sepal_width_2")
    assert "Can not pass in columns to rename as both a dict and kwargs" in str(excinfo.value)


def test_rename_no_args(make_df, valid_data: list[dict[str, float]]) -> None:
    df = make_df(valid_data)
    with pytest.raises(ValueError) as excinfo:
        _ = df.rename()

    assert "rename requires at least 1 argument" in str(excinfo.value)
