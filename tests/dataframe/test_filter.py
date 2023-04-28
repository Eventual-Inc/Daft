from __future__ import annotations

from typing import Any

import pytest

import daft
from daft import DataFrame


def test_filter_missing_column(valid_data: list[dict[str, Any]]) -> None:
    df = daft.from_pylist(valid_data)
    with pytest.raises(ValueError):
        df.select("sepal_width").where(df["petal_length"] > 4.8)


@pytest.mark.skip(reason="Requires Expression.float.is_nan()")
def test_drop_na(missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = daft.from_pylist(missing_value_data)
    df_len_no_col = len(df.drop_nan().collect())
    assert df_len_no_col == 2

    df: DataFrame = daft.from_pylist(missing_value_data)
    df_len_col = len(df.drop_nan("sepal_width").collect())
    assert df_len_col == 2


def test_drop_null(missing_value_data: list[dict[str, Any]]) -> None:
    df: DataFrame = daft.from_pylist(missing_value_data)
    df_len_no_col = len(df.drop_null().collect())
    assert df_len_no_col == 2

    df: DataFrame = daft.from_pylist(missing_value_data)
    df_len_col = len(df.drop_null("sepal_length").collect())
    assert df_len_col == 2
