from __future__ import annotations

import time
from typing import List

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from daft import DataFrame, StatefulUDF, udf
from daft.resource_request import ResourceRequest


class MyObj:
    def __init__(self, x: int):
        self._x = x


@udf(
    return_dtype=int,
    input_columns={
        "arg_untyped": list,
        "arg_list": list,
        "arg_typing_list": List,
        "arg_typing_list_int": List[int],
        "arg_numpy_array": np.ndarray,
        "arg_polars_series": pl.Series,
        "arg_pandas_series": pd.Series,
        "arg_pyarrow_array": pa.Array,
    },
)
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
# Tests for class UDFs
###


@udf(return_dtype=int, input_columns={"b": np.ndarray})
class MyUDF:
    def __init__(self):
        self._a = 1

    def __call__(self, b: np.ndarray):
        return b + self._a


def test_class_udf():
    df = DataFrame.from_pydict({"a": [1, 2, 3]})
    df = df.with_column("newcol", MyUDF(df["a"]))
    data = df.to_pydict()
    assert data["newcol"] == [2, 3, 4]


###
# Test bad UDF invocations
###


@udf(return_dtype=int, input_columns={"is_a_col": list})
def udf_one_col(is_a_col: list, not_a_col: int):
    assert isinstance(is_a_col, list)
    assert isinstance(not_a_col, int)
    return is_a_col


def test_bad_udf_invocations():
    df = DataFrame.from_pydict({"a": [1, 2, 3]})

    # Cannot call UDF on non-expression when parameter is specified in input_columns
    with pytest.raises(ValueError):
        df = df.with_column("b", udf_one_col(1, 1))

    # Cannot call UDF on expression column when parameter is not specified in input_columns
    with pytest.raises(ValueError):
        df = df.with_column("b", udf_one_col(df["a"], df["a"]))

    # Calling UDF on df["a"] expression will transform it into a list at runtime for
    # `is_a_col`, but not transform the expression for `not_a_col`
    df = df.with_column("b", udf_one_col(df["a"], 1))
    df.collect()


###
# Test UDF mutex initializations
###


@udf(return_dtype=int, input_columns={"x": list})
class MyMutexUDF(StatefulUDF):
    def __init__(self):

        with self.mutex():
            time.sleep(0.5)

    def __call__(self, x):
        return x


def test_mutex_udf_initialization():

    df = DataFrame.from_pydict({"a": list(range(10))}).repartition(2)
    df = df.with_column("b", MyMutexUDF(df["a"]), resource_request=ResourceRequest(num_cpus=0.5))

    start = time.time()
    df.collect()
    elapsed = time.time() - start

    # If mutex works, the initializations for MyMutexUDF should happen serially and elapsed time should be about 1 second
    assert elapsed == 1
