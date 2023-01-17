from __future__ import annotations

from typing import Callable

import numpy as np
import pandas as pd
import polars as pl
import pytest

from daft.dataframe import DataFrame
from daft.expressions import col, lit
from daft.types import ExpressionType
from daft.udf import polars_udf, udf
from tests.conftest import assert_df_equals


def parametrize_udfs(name: str, funcs: Callable | list[Callable], return_type: type):
    """Parametrizes a test with the different variants of UDFs (numpy and polars) when given
    a function to wrap.

    >>> def f(): ...
    >>>
    >>> @parametrize_udfs(name, f, return_type=int)
    >>> def test_foo(): ...

    Is equivalent to:

    >>> @polars_udf(return_type=int)
    >>> def f_pl(): ...
    >>>
    >>> @udf(return_type=int)
    >>> def f_np(): ...
    >>>
    >>> @pytest.mark.parametrize(name, [f_np, f_pl], return_type=int)
    >>> def test_foo(name):
    >>>     ...
    """
    funclist = funcs
    if not isinstance(funclist, list):
        funclist = [funclist]

    def _wrapper(test_func):
        udfs = []
        for func in funclist:
            udfs.extend(
                [
                    udf(func, return_type=return_type),
                    polars_udf(func, return_type=return_type),
                ]
            )
        return pytest.mark.parametrize(name, udfs)(test_func)

    return _wrapper


###
# Using Python dicts as an object
###


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_python_dict(repartition_nparts):
    data = {"id": [i for i in range(10)], "dicts": [{"foo": i} for i in range(10)]}
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts)
    assert daft_df.schema()["id"].daft_type == ExpressionType.from_py_type(int)
    assert daft_df.schema()["dicts"].daft_type == ExpressionType.from_py_type(dict)
    pd_df = pd.DataFrame.from_dict(data)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


def rename_key_list(dicts):
    new_dicts = []
    for d in dicts:
        new_dicts.append({"bar": d["foo"]})
    return new_dicts


def rename_key_np(dicts):
    new_dicts = []
    for d in dicts:
        new_dicts.append({"bar": d["foo"]})
    return np.array(new_dicts)


def rename_key_pd(dicts):
    new_dicts = []
    for d in dicts:
        new_dicts.append({"bar": d["foo"]})
    return pd.Series(new_dicts)


def rename_key_pl(dicts):
    new_dicts = []
    for d in dicts:
        new_dicts.append({"bar": d["foo"]})
    return pl.Series(new_dicts)


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
@parametrize_udfs("rename_key", [rename_key_list, rename_key_np, rename_key_pd, rename_key_pl], return_type=dict)
def test_python_dict_udf(repartition_nparts, rename_key):
    data = {"id": [i for i in range(10)], "dicts": [{"foo": i} for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data)
        .repartition(repartition_nparts)
        .with_column("dicts_renamed", rename_key(col("dicts")))
    )
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["dicts_renamed"] = pd.Series([{"bar": d["foo"]} for d in pd_df["dicts"]])
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


def merge_dicts(dicts, to_merge):
    new_dicts = []
    for d in dicts:
        new_dicts.append({**d, **to_merge})
    return new_dicts


merge_dicts_np = udf(merge_dicts, return_type=dict)
merge_dicts_pl = polars_udf(merge_dicts, return_type=dict)


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
@parametrize_udfs("merge_dicts", merge_dicts, return_type=dict)
def test_python_dict_udf_merge_dicts(repartition_nparts, merge_dicts):
    to_merge = {"new_field": 1}
    data = {"id": [i for i in range(10)], "dicts": [{"foo": i} for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data)
        .repartition(repartition_nparts)
        .with_column("dicts_renamed", merge_dicts(col("dicts"), to_merge))
    )
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["dicts_renamed"] = pd.Series([{**d, **to_merge} for d in pd_df["dicts"]])
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_python_chained_expression_calls(repartition_nparts):
    data = {"id": [i for i in range(10)], "dicts": [{"foo": str(i)} for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data)
        .repartition(repartition_nparts)
        .with_column("foo_starts_with_1", col("dicts").apply(lambda d: d["foo"], return_type=str).str.startswith("1"))
    )
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["foo_starts_with_1"] = pd.Series([d["foo"] for d in pd_df["dicts"]]).str.startswith("1")
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


###
# Using np.ndarray as an object
###


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_load_pydict_with_obj(repartition_nparts):
    data = {"id": [i for i in range(10)], "features": [np.ones(i) for i in range(10)]}
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts)
    assert daft_df.schema()["id"].daft_type == ExpressionType.from_py_type(int)
    assert daft_df.schema()["features"].daft_type == ExpressionType.from_py_type(np.ndarray)
    pd_df = pd.DataFrame.from_dict(data)
    daft_pd_df = daft_df.to_pandas()
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_pyobj_add_2_cols(repartition_nparts):
    data = {"id": [i for i in range(10)], "features": [np.ones(i) for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data)
        .repartition(repartition_nparts)
        .with_column("features_doubled", col("features") + col("features"))
    )
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["features_doubled"] = pd_df["features"] + pd_df["features"]
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize(
    "op",
    [
        1 + col("features"),
        col("features") + 1,
        col("features") + lit(np.int64(1)),
        lit(np.int64(1)) + col("features"),
        # col("features") + lit(np.array([1])),
        # lit(np.array([1])) + col("features"),
        col("ones") + col("features"),
        col("features") + col("ones"),
    ],
)
@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_pyobj_add(repartition_nparts, op):
    data = {
        "id": [i for i in range(10)],
        "features": [np.ones(i) for i in range(10)],
        "ones": [1 for i in range(10)],
    }
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts).with_column("features_plus_one", op)
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["features_plus_one"] = pd_df["features"] + 1
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


def get_length(features):
    return [len(feature) for feature in features]


def zeroes(features):
    return [np.zeros(feature.shape) for feature in features]


def make_features(lengths):
    return [np.ones(length) for length in lengths]


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
@parametrize_udfs("get_length", get_length, return_type=int)
def test_pyobj_obj_to_primitive_udf(repartition_nparts, get_length):
    data = {"id": [i for i in range(10)], "features": [np.ndarray(i) for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data).repartition(repartition_nparts).with_column("length", get_length(col("features")))
    )
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["length"] = pd.Series([len(feature) for feature in pd_df["features"]])
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
@parametrize_udfs("zeroes", zeroes, return_type=dict)
def test_pyobj_obj_to_obj_udf(repartition_nparts, zeroes):
    data = {"id": [i for i in range(10)], "features": [np.ones(i) for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data).repartition(repartition_nparts).with_column("zero_objs", zeroes(col("features")))
    )
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["zero_objs"] = pd.Series([np.zeros(len(feature)) for feature in pd_df["features"]])
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
@parametrize_udfs("make_features", make_features, return_type=dict)
def test_pyobj_primitive_to_obj_udf(repartition_nparts, make_features):
    data = {"lengths": [i for i in range(10)]}
    daft_df = (
        DataFrame.from_pydict(data)
        .repartition(repartition_nparts)
        .with_column("features", make_features(col("lengths")))
    )
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["features"] = pd.Series([np.ones(length) for length in pd_df["lengths"]])
    assert_df_equals(daft_pd_df, pd_df, sort_key="lengths")


def test_pyobj_filter_error_on_pyobj():
    data = {"id": [i for i in range(10)], "features": [np.ndarray(i) if i % 2 == 0 else None for i in range(10)]}
    with pytest.raises(ValueError):
        daft_df = DataFrame.from_pydict(data).where(col("features") != None)


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
@parametrize_udfs("get_length", get_length, return_type=int)
def test_pyobj_filter_udf(repartition_nparts, get_length):
    data = {"id": [i for i in range(10)], "features": [np.ndarray(i) for i in range(10)]}
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts).where(get_length(col("features")) > 5)
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["length"] = pd.Series([len(feature) for feature in pd_df["features"]])
    pd_df = pd_df[pd_df["length"] > 5].drop(columns=["length"])
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


###
# Using .as_py() to call python methods on your objects easily
###


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_pyobj_aspy_method_call(repartition_nparts):
    data = {"id": [i for i in range(10)], "features": [np.arange(i) for i in range(1, 11)]}
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts)
    daft_df = daft_df.with_column("max", col("features").as_py(np.ndarray).max())
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["max"] = pd.Series([feature.max() for feature in pd_df["features"]])
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_pyobj_aspy_method_call_args(repartition_nparts):
    data = {"id": [i for i in range(10)], "features": [np.arange(i) for i in range(1, 11)]}
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts)
    daft_df = daft_df.with_column("clipped", col("features").as_py(np.ndarray).clip(0, 1))
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["clipped"] = pd.Series([feature.clip(0, 1) for feature in pd_df["features"]])
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")


@pytest.mark.parametrize("repartition_nparts", [1, 5, 6, 10, 11])
def test_pyobj_dict_indexing(repartition_nparts):
    data = {"id": [i for i in range(10)], "dicts": [{"foo": i} for i in range(10)]}
    daft_df = DataFrame.from_pydict(data).repartition(repartition_nparts)
    daft_df = daft_df.with_column("foo", col("dicts").as_py(dict)["foo"])
    daft_pd_df = daft_df.to_pandas()
    pd_df = pd.DataFrame.from_dict(data)
    pd_df["foo"] = pd.Series([d["foo"] for d in pd_df["dicts"]])
    assert_df_equals(daft_pd_df, pd_df, sort_key="id")
