from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa

from daft import DataFrame, col
from daft.execution.operators import ExpressionType
from tests.conftest import assert_df_equals


class MyObj:
    def __init__(self, x):
        self._x = x

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, MyObj) and other._x == self._x


def test_explode_single_col_pylist():
    def add_one(o: MyObj):
        if o is None or o is np.nan:
            return None
        return o._x + 1

    data = {"explode": [[MyObj(1), MyObj(2), MyObj(3)], [MyObj(4), MyObj(5)], [], None], "repeat": ["a", "b", "c", "d"]}
    df = DataFrame.from_pydict(data)
    df = df.explode(col("explode"))
    df = df.with_column("explode_plus1", col("explode").apply(add_one))

    assert df.schema()["explode"].daft_type == ExpressionType.python_object()

    daft_pd_df = df.to_pandas()
    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode("explode")
    pd_df["explode_plus1"] = pd_df["explode"].apply(add_one)
    assert_df_equals(daft_pd_df, pd_df, sort_key="explode_plus1")


def test_explode_single_col_arrow():
    data = {"explode": pa.array([[1, 2, 3], [4, 5], [], None]), "repeat": ["a", "b", "c", "d"]}
    df = DataFrame.from_pydict(data)
    df = df.explode(col("explode"))
    df = df.with_column("explode_plus1", col("explode") + 1)

    assert df.schema()["explode"].daft_type == ExpressionType.python_object()

    daft_pd_df = df.to_pandas()
    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode("explode")
    pd_df["explode_plus1"] = pd_df["explode"] + 1

    pd_df["explode"] = pd_df["explode"].astype(float)
    pd_df["explode_plus1"] = pd_df["explode_plus1"].astype(float)

    assert_df_equals(daft_pd_df, pd_df, sort_key="explode")


def test_explode_multi_col():
    data = {
        "explode1": [[1, 2, 3], [4, 5], [], None],
        "explode2": [["a", "a", "a"], ["b", "b"], [], None],
        "repeat": ["a", "b", "c", "d"],
    }
    df = DataFrame.from_pydict(data)
    df = df.explode(col("explode1"), col("explode2"))
    df = df.with_column("explode1_plus1", col("explode1") + 1)
    df = df.with_column("explode2_startswitha", col("explode2").str.startswith("a"))

    assert df.schema()["explode1"].daft_type == ExpressionType.python_object()
    assert df.schema()["explode2"].daft_type == ExpressionType.python_object()

    daft_pd_df = df.to_pandas()
    pd_df = pd.DataFrame(data)
    pd_df = pd_df.explode(["explode1", "explode2"])
    pd_df["explode1_plus1"] = pd_df["explode1"] + 1
    pd_df["explode2_startswitha"] = pd_df["explode2"].str.startswith("a")

    pd_df["explode1"] = pd_df["explode1"].astype(float)
    pd_df["explode1_plus1"] = pd_df["explode1_plus1"].astype(float)

    assert_df_equals(daft_pd_df, pd_df, sort_key="explode1")
