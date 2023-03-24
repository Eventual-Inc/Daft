from __future__ import annotations

from typing import Any

import pandas as pd

from daft import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals


def test_is_nan_arrow():
    data = {"id": [str(i) for i in range(20)], "foo": [float("nan") if i % 2 == 1 else float(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("is_null", col("foo").is_nan())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["is_null"] = pd_df["foo"].isna()
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_is_null_arrow():
    data = {"id": [str(i) for i in range(20)], "foo": [None if i % 2 == 1 else str(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("is_null", col("foo").is_null())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["is_null"] = pd_df["foo"].isnull()
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


class MyObj:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MyObj):
            return False
        return self.x == other.x


def test_is_null_pylist():
    data = {"id": [str(i) for i in range(20)], "foo": [None if i % 2 == 1 else MyObj(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("is_null", col("foo").is_null())
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["is_null"] = pd_df["foo"].isnull()
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")
