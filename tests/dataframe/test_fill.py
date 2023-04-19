from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from daft import DataFrame


def test_fill_nan_data_frame(missing_value_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(missing_value_data)
    df = df.with_column("sepal_width", df["sepal_width"].fillnan(3.1))
    pd_df = df.to_pandas()
    assert pd_df.loc[1, "sepal_width"] == 3.1


def test_fill_nan_data_frame_with_none(missing_value_data: list[dict[str, Any]]) -> None:
    pytest.skip(
        "This tests is skiped until None will be resolved to nan in float columns in rust with the next release"
    )
    df = DataFrame.from_pylist(missing_value_data)
    df = df.with_column("sepal_width_none", df["sepal_width"].fillnan(None))
    pd_df = df.to_pandas()
    assert pd.isna(pd_df.loc[1, "sepal_width"])


def test_fill_null_data_frame(missing_value_data: list[dict[str, Any]]) -> None:
    df = DataFrame.from_pylist(missing_value_data)
    df = df.with_column("sepal_length", df["sepal_length"].fillnull(3.1))
    pd_df = df.to_pandas()
    assert pd_df.loc[0, "sepal_length"] == 3.1
