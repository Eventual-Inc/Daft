from __future__ import annotations

import functools
import operator

import pytest

from daft import DataType, col


def test_global_product_basic(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 3]})
    result = df.product("a").collect()
    row = result.to_pydict()
    assert row["a"] == [6]


def test_global_product_single_value(make_df, with_morsel_size):
    df = make_df({"a": [42]})
    result = df.product("a").collect()
    row = result.to_pydict()
    assert row["a"] == [42]


def test_global_product_with_nulls(make_df, with_morsel_size):
    df = make_df({"a": [2, None, 3, None, 5]})
    result = df.product("a").collect()
    row = result.to_pydict()
    assert row["a"] == [30]


def test_global_product_all_nulls(make_df, with_morsel_size):
    df = make_df({"a": [None, None, None]}).with_column("a", col("a").cast(DataType.int64()))
    result = df.product("a").collect()
    assert result.to_pydict()["a"] == [None]


def test_global_product_with_zero(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 0, 4]})
    result = df.product("a").collect()
    row = result.to_pydict()
    assert row["a"] == [0]


def test_global_product_negative_values(make_df, with_morsel_size):
    df = make_df({"a": [-1, 2, -3]})
    result = df.product("a").collect()
    row = result.to_pydict()
    assert row["a"] == [6]


def test_global_product_floats(make_df, with_morsel_size):
    df = make_df({"a": [1.5, 2.0, 4.0]})
    result = df.product("a").collect()
    row = result.to_pydict()
    assert abs(row["a"][0] - 12.0) < 1e-10


def test_global_product_multiple_cols(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 3], "b": [4, 5, 6]})
    result = df.product("a", "b").collect()
    row = result.to_pydict()
    assert row["a"] == [6]
    assert row["b"] == [120]


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_global_product_multi_partition(make_df, repartition_nparts, with_morsel_size):
    data = [1, 2, 3, 4, 5]
    expected = functools.reduce(operator.mul, data, 1)
    df = make_df({"a": data}, repartition=repartition_nparts)
    result = df.product("a").collect()
    row = result.to_pydict()
    assert row["a"] == [expected]


def test_grouped_product_basic(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a", "b", "b"], "vals": [1, 2, 3, 4, 5]})
    result = df.groupby("keys").product("vals").sort("keys").collect()
    row = result.to_pydict()
    assert row["keys"] == ["a", "b"]
    assert row["vals"] == [6, 20]


def test_grouped_product_with_nulls(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a", "b", "b"], "vals": [2, None, 3, None, 5]})
    result = df.groupby("keys").product("vals").sort("keys").collect()
    row = result.to_pydict()
    assert row["keys"] == ["a", "b"]
    assert row["vals"] == [6, 5]


def test_grouped_product_all_columns(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "b", "b"], "x": [2, 3, 4, 5], "y": [10, 10, 2, 2]})
    result = df.groupby("keys").product().sort("keys").collect()
    row = result.to_pydict()
    assert row["x"] == [6, 20]
    assert row["y"] == [100, 4]
