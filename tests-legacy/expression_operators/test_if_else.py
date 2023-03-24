from __future__ import annotations

from typing import Any

import pandas as pd

from daft import DataFrame
from daft.expressions import col
from tests.conftest import assert_df_equals


def test_if_else_arrow():
    data = {"id": [i for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("one_or_zero", (col("id") > 15).if_else(1, 0))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["one_or_zero"] = (pd_df["id"] > 15).astype(int)
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_if_else_arrow_col():
    data = {"id": [i for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("one_or_zero", (col("id") > 15).if_else(col("id"), 0))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["one_or_zero"] = pd_df["id"].where(pd_df["id"] > 15, 0)
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


class MyObj:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MyObj):
            return False
        return self.x == other.x

    def __repr__(self):
        return f"<MyObj x={self.x}>"


def test_if_else_pylist():
    data = {"id": [i for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("one_or_zero", (col("id") > 15).if_else(MyObj(1), MyObj(0)))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["one_or_zero"] = [MyObj(i) for i in (pd_df["id"] > 15).astype(int)]
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")


def test_if_else_pylist_col():
    data = {"id": [i for i in range(20)], "obj": [MyObj(i) for i in range(20)]}
    df = DataFrame.from_pydict(data)
    df = df.with_column("one_or_zero", (col("id") > 15).if_else(col("obj"), MyObj(0)))
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["one_or_zero"] = pd.Series([MyObj(i) for i in range(20)]).where(pd_df["id"] > 15, MyObj(0))
    assert_df_equals(df.to_pandas(), pd_df, sort_key="id")
