from __future__ import annotations

from typing import Any

import pytest

import daft
from daft import DataFrame


def test_filter_missing_column(make_df, valid_data: list[dict[str, Any]]) -> None:
    df = make_df(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_width").where(df["petal_length"] > 4.8)


def test_drop_na(make_df, missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = make_df(missing_value_data)
    df_len_no_col = len(df.drop_nan().collect())
    assert df_len_no_col == 2

    df: DataFrame = make_df(missing_value_data)
    df_len_col = len(df.drop_nan("sepal_width").collect())
    assert df_len_col == 2


def test_drop_null(make_df, missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = make_df(missing_value_data)
    df_len_no_col = len(df.drop_null().collect())
    assert df_len_no_col == 2

    df: DataFrame = make_df(missing_value_data)
    df_len_col = len(df.drop_null("sepal_length").collect())
    assert df_len_col == 2


def test_filter_sql() -> None:
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 9, 9]})

    df = df.where("z = 9 AND y > 5").collect().to_pydict()
    expected = {"x": [3], "y": [6], "z": [9]}

    assert df == expected


def test_filter_alias_for_where() -> None:
    df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6], "z": [7, 9, 9]})

    expected = df.where("z = 9 AND y > 5").collect().to_pydict()
    actual = df.filter("z = 9 AND y > 5").collect().to_pydict()

    assert actual == expected
