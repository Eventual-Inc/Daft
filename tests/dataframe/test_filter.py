from __future__ import annotations

from typing import Any

import pytest

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
