from __future__ import annotations

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from daft import DataFrame, udf


@udf(return_type=int)
def my_udf(
    # Test different arg containers
    arg_untyped,
    arg_list: list,
    arg_list_int: list[int],
    arg_typing_list: list,
    arg_typing_list_int: list[int],
    arg_numpy_array: np.ndarray,
    arg_numpy_array_int: np.ndarray[int],
    arg_polars_series: pl.Series,
    arg_pandas_series: pd.Series,
    arg_pyarrow_array: pa.Array,
    # Test arg non-containers
    arg_int: int,
    arg_str: str,
    arg_2_by_2_np_arr: np.ndarray,
    # Test custom kwargs non-containers
    kwarg_untyped=1,
    kwarg_int: int = 3,
    kwarg_2_by_2_np_arr: np.ndarray = np.ones((2, 2)),
    return_container: str = "numpy",
):
    # Test that containers are passed in as the correct container type according to type hints
    assert isinstance(arg_untyped, list)
    assert isinstance(arg_list, list)
    assert isinstance(arg_list_int, list)
    assert isinstance(arg_typing_list, list)
    assert isinstance(arg_typing_list_int, list)
    assert isinstance(arg_numpy_array, np.ndarray)
    assert isinstance(arg_numpy_array_int, np.ndarray)
    assert isinstance(arg_polars_series, pl.Series)
    assert isinstance(arg_pandas_series, pd.Series)
    assert isinstance(arg_pyarrow_array, pa.Array)

    # Test that user's values are passed in correctly as scalar values
    assert isinstance(arg_int, int)
    assert isinstance(arg_str, str)
    assert isinstance(arg_2_by_2_np_arr, np.ndarray)
    assert isinstance(kwarg_untyped, int)
    assert isinstance(kwarg_int, int)
    assert isinstance(kwarg_2_by_2_np_arr, np.ndarray) and kwarg_2_by_2_np_arr.shape == (2, 2)

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
            df["a"],
            df["a"],
            # arg non-containers
            3,
            "foo",
            np.ones((2, 2)),
            # kwargs non-containers
            kwarg_untyped=2,
            kwarg_int=3,
            kwarg_2_by_2_np_arr=np.ones((2, 2)),
            # Try different return containers
            return_container=return_container,
        ),
    )
    data = df.to_pydict()
    assert data["a"] == [1, 2, 3]
