from __future__ import annotations

import math

import pytest

from daft import DataType, col


def test_global_skew_symmetric(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 3, 4, 5]})
    result = df.skew("a").collect()
    row = result.to_pydict()
    assert row["a"][0] == 0.0


def test_global_skew_right_skewed(make_df, with_morsel_size):
    df = make_df({"a": [1, 1, 1, 2, 10]})
    result = df.skew("a").collect()
    row = result.to_pydict()
    assert row["a"][0] > 0


def test_global_skew_left_skewed(make_df, with_morsel_size):
    df = make_df({"a": [1, 9, 10, 10, 10]})
    result = df.skew("a").collect()
    row = result.to_pydict()
    assert row["a"][0] < 0


def test_global_skew_multiple_cols(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 3, 4, 5], "b": [1, 1, 1, 2, 10]})
    result = df.skew("a", "b").collect()
    row = result.to_pydict()
    assert row["a"][0] == 0.0
    assert row["b"][0] > 0


def test_global_skew_with_nulls(make_df, with_morsel_size):
    df = make_df({"a": [None, 1, 2, None, 3]})
    result = df.skew("a").collect()
    row = result.to_pydict()
    assert row["a"][0] is not None
    assert math.isclose(row["a"][0], 0.0, abs_tol=1e-10)


def test_global_skew_single_value(make_df, with_morsel_size):
    df = make_df({"a": [5.0]})
    result = df.skew("a").collect()
    val = result.to_pydict()["a"][0]
    assert isinstance(val, float) and math.isnan(val)


def test_global_skew_all_nulls(make_df, with_morsel_size):
    df = make_df({"a": [None, None, None]}).with_column("a", col("a").cast(DataType.float64()))
    result = df.skew("a").collect()
    assert result.to_pydict()["a"][0] is None


def test_global_skew_two_values(make_df, with_morsel_size):
    df = make_df({"a": [1.0, 2.0]})
    result = df.skew("a").collect()
    val = result.to_pydict()["a"][0]
    assert val == 0.0


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_global_skew_multi_partition(make_df, repartition_nparts, with_morsel_size):
    df = make_df({"a": [1, 2, 3, 4, 5]}, repartition=repartition_nparts)
    result = df.skew("a").collect()
    row = result.to_pydict()
    assert row["a"][0] == 0.0


def test_global_skew_known_value(make_df, with_morsel_size):
    df = make_df({"a": [1, 1, 2]})
    result = df.skew("a").collect()
    row = result.to_pydict()
    assert math.isclose(row["a"][0], 0.70710678, rel_tol=1e-5)


def test_grouped_skew(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a", "b", "b", "b"], "vals": [1, 2, 3, 1, 1, 2]})
    result = df.groupby("keys").skew("vals").collect()
    result_dict = result.sort("keys").to_pydict()
    assert result_dict["vals"][0] == 0.0
    assert math.isclose(result_dict["vals"][1], 0.70710678, rel_tol=1e-5)


def test_grouped_skew_all_columns(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a"], "x": [1, 2, 3], "y": [1, 1, 2]})
    result = df.groupby("keys").skew().collect()
    row = result.to_pydict()
    assert row["x"][0] == 0.0
    assert math.isclose(row["y"][0], 0.70710678, rel_tol=1e-5)
