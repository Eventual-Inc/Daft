from __future__ import annotations

from typing import Any

import pytest

from daft import DataFrame


def test_filter_missing_column(valid_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_width").where(df["petal_length"] > 4.8)


def test_drop_na(missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = DataFrame.from_pylist(missing_value_data)
    df_len_no_col = len(df.drop_na().collect())
    assert df_len_no_col == 2

    df: DataFrame = DataFrame.from_pylist(missing_value_data)
    df_len_col = len(df.drop_na("sepal_width").collect())
    assert df_len_col == 2


def test_drop_null(missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = DataFrame.from_pylist(missing_value_data)
    df_len_no_col = len(df.drop_null().collect())
    print(df_len_no_col)
    assert df_len_no_col == 1

    df: DataFrame = DataFrame.from_pylist(missing_value_data)
    df_len_col = len(df.drop_null("sepal_length").collect())
    print(df_len_col)
    assert df_len_col == 2
