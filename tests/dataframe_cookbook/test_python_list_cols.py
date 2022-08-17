from __future__ import annotations

from typing import Any

import pandas as pd

from daft.dataframe import DataFrame
from daft.execution.operators import ExpressionType
from daft.expressions import col
from tests.conftest import assert_df_equals


class MyObj:
    def __init__(self, x: int):
        self._x = x

    def __add__(self, other: MyObj) -> MyObj:
        return MyObj(self._x + other._x)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MyObj):
            return False
        return self._x == other._x


def test_load_pydict_with_obj():
    data = {
        "foo": [1, 2, 3],
        "bar": [1.0, None, 3.0],
        "baz": ["a", "b", "c"],
        "obj": [MyObj(i) for i in range(3)],
    }
    daft_df = DataFrame.from_pydict(data)
    assert [field.daft_type for field in daft_df.schema()] == [
        ExpressionType.from_py_type(int),
        ExpressionType.from_py_type(float),
        ExpressionType.from_py_type(str),
        ExpressionType.from_py_type(MyObj),
    ]
    pd_df = pd.DataFrame.from_dict(data)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="foo")


def test_pyobj_addition():
    data = {
        "foo": [1, 2, 3],
        "bar": [1.0, None, 3.0],
        "baz": ["a", "b", "c"],
        "obj": [MyObj(i) for i in range(3)],
    }
    daft_df = DataFrame.from_pydict(data).with_column("obj_doubled", col("obj") + col("obj"))
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["obj_doubled"] = pd_df["obj"] + pd_df["obj"]
    assert_df_equals(daft_pd_df, pd_df, sort_key="foo")
