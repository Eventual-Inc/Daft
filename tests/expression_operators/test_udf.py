from __future__ import annotations

import sys
from typing import List

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from daft import DataFrame, udf


class MyObj:
    def __init__(self, x: int):
        self._x = x


@udf(return_type=int)
def my_udf(
    # Test different arg containers
    arg_untyped,
    arg_list: list,
    arg_typing_list: List,
    arg_typing_list_int: List[int],
    arg_numpy_array: np.ndarray,
    arg_polars_series: pl.Series,
    arg_pandas_series: pd.Series,
    arg_pyarrow_array: pa.Array,
    # Test arg non-containers
    arg_int: int,
    arg_str: str,
    arg_myobj: MyObj,
    # Test custom kwargs non-containers
    kwarg_untyped=1,
    kwarg_int: int = 3,
    kwarg_myobj: np.ndarray = MyObj(1),
    return_container: str = "numpy",
):
    # Test that containers are passed in as the correct container type according to type hints
    assert isinstance(arg_untyped, list)
    assert isinstance(arg_list, list)
    assert isinstance(arg_typing_list, list)
    assert isinstance(arg_typing_list_int, list)
    assert isinstance(arg_numpy_array, np.ndarray)
    assert isinstance(arg_polars_series, pl.Series)
    assert isinstance(arg_pandas_series, pd.Series)
    assert isinstance(arg_pyarrow_array, pa.Array)

    # Test that user's values are passed in correctly as scalar values
    assert isinstance(arg_int, int)
    assert isinstance(arg_str, str)
    assert isinstance(arg_myobj, MyObj)
    assert isinstance(kwarg_untyped, int)
    assert isinstance(kwarg_int, int)
    assert isinstance(kwarg_myobj, MyObj)

    # Test different combinations of return container types
    if return_container == "numpy":
        return arg_numpy_array
    elif return_container == "list":
        return arg_numpy_array.tolist()
    elif return_container == "arrow":
        return pa.array(arg_numpy_array)
    elif return_container == "arrow_chunked":
        return pa.chunked_array([pa.array(arg_numpy_array)])
    elif return_container == "pandas":
        return pd.Series(arg_numpy_array)
    elif return_container == "polars":
        return pl.Series(arg_numpy_array)
    raise NotImplementedError(return_container)


@pytest.mark.parametrize("return_container", ["numpy", "list", "arrow", "arrow_chunked", "pandas", "polars"])
def test_udf_typing(return_container):
    df = DataFrame.from_pydict({"a": [1, 2, 3]})
    df = df.with_column(
        "newcol",
        my_udf(
            # args
            df["a"],
            df["a"],
            df["a"],
            df["a"],
            df["a"],
            df["a"],
            df["a"],
            df["a"],
            # arg non-containers
            3,
            "foo",
            MyObj(2),
            # kwargs non-containers
            kwarg_untyped=2,
            kwarg_int=3,
            kwarg_myobj=MyObj(2),
            # Try different return containers
            return_container=return_container,
        ),
    )
    data = df.to_pydict()
    assert data["newcol"] == [1, 2, 3]


def test_udf_typing_kwargs():
    df = DataFrame.from_pydict({"a": [1, 2, 3]})
    df = df.with_column(
        "newcol",
        my_udf(
            # args
            arg_untyped=df["a"],
            arg_list=df["a"],
            arg_typing_list=df["a"],
            arg_typing_list_int=df["a"],
            arg_numpy_array=df["a"],
            arg_polars_series=df["a"],
            arg_pandas_series=df["a"],
            arg_pyarrow_array=df["a"],
            # arg non-containers
            arg_int=3,
            arg_str="foo",
            arg_myobj=MyObj(2),
        ),
    )
    data = df.to_pydict()
    assert data["newcol"] == [1, 2, 3]


###
# Tests for type_hints=... kwarg
###


@udf(return_type=int, type_hints={"a": np.ndarray})
def my_udf_type_hints(a: list):
    assert isinstance(a, np.ndarray)
    return a


def test_udf_type_hints_override():
    df = DataFrame.from_pydict({"a": [1, 2, 3]})
    df = df.with_column("newcol", my_udf_type_hints(df["a"]))
    data = df.to_pydict()
    assert data["newcol"] == [1, 2, 3]


@pytest.mark.skipif(
    sys.version_info > (3, 8), reason="Requires python3.8 or lower which does not support advanced type annotations"
)
def test_udf_new_typing_annotations():
    """Test behavior on unsupported advanced type annotations, using `type_hints` as the workaround"""

    def my_udf_new_typing_annotations(
        arg_list_int: list[int],
        arg_numpy_array_int: np.ndarray[int],
    ):
        assert isinstance(arg_list_int, list)
        assert isinstance(arg_numpy_array_int, np.ndarray)
        return arg_list_int

    with pytest.raises(TypeError):
        udf(my_udf_new_typing_annotations, return_type=int)

    my_udf_new_typing_annotations_udf = udf(
        my_udf_new_typing_annotations,
        return_type=int,
        type_hints={"arg_list_int": List[int], "arg_numpy_array_int": np.ndarray},
    )
    df = DataFrame.from_pydict({"a": [1, 2, 3]})
    df = df.with_column(
        "newcol",
        my_udf_new_typing_annotations_udf(
            arg_list_int=df["a"],
            arg_numpy_array_int=df["a"],
        ),
    )
    data = df.to_pydict()
    assert data["newcol"] == [1, 2, 3]
