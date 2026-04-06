from __future__ import annotations

import pytest


def test_global_variance_single_col(make_df, with_morsel_size):
    df = make_df({"a": [0, 1, 2]})
    result = df.var("a").collect()
    row = result.to_pydict()
    assert abs(row["a"][0] - 1.0) < 1e-10


def test_global_variance_multiple_cols(make_df, with_morsel_size):
    df = make_df({"a": [0, 1, 2], "b": [10, 20, 30]})
    result = df.var("a", "b").collect()
    row = result.to_pydict()
    assert abs(row["a"][0] - 1.0) < 1e-10
    assert abs(row["b"][0] - 100.0) < 1e-10


def test_global_variance_ddof_0(make_df, with_morsel_size):
    df = make_df({"a": [0, 1, 2]})
    result = df.var("a", ddof=0).collect()
    row = result.to_pydict()
    assert abs(row["a"][0] - 2.0 / 3.0) < 1e-10


def test_global_variance_with_nulls(make_df, with_morsel_size):
    df = make_df({"a": [None, 1, 2, None, 3]})
    result = df.var("a").collect()
    row = result.to_pydict()
    assert row["a"][0] is not None
    assert abs(row["a"][0] - 1.0) < 1e-10


def test_global_variance_all_nulls(make_df, with_morsel_size):
    df = make_df({"a": [None, None, None]})
    result = df.var("a").collect()
    row = result.to_pydict()
    assert row["a"][0] is None


def test_global_variance_single_value(make_df, with_morsel_size):
    df = make_df({"a": [5.0]})
    result_ddof1 = df.var("a", ddof=1).collect()
    assert result_ddof1.to_pydict()["a"][0] is None
    result_ddof0 = df.var("a", ddof=0).collect()
    assert result_ddof0.to_pydict()["a"][0] == 0.0


def test_global_variance_matches_stddev_squared(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 3, 4, 5]})
    var_result = df.var("a").collect().to_pydict()["a"][0]
    std_result = df.stddev("a").collect().to_pydict()["a"][0]
    assert abs(var_result - std_result**2) < 1e-10


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_global_variance_multi_partition(make_df, repartition_nparts, with_morsel_size):
    df = make_df({"a": [1, 2, 3, 4, 5]}, repartition=repartition_nparts)
    result = df.var("a").collect()
    row = result.to_pydict()
    assert abs(row["a"][0] - 2.5) < 1e-10


def test_grouped_variance(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a", "b", "b"], "vals": [0, 1, 2, 10, 20]})
    result = df.groupby("keys").var("vals").collect()
    result_dict = result.sort("keys").to_pydict()
    assert abs(result_dict["vals"][0] - 1.0) < 1e-10
    assert abs(result_dict["vals"][1] - 50.0) < 1e-10


def test_grouped_variance_ddof_0(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a"], "vals": [0, 1, 2]})
    result = df.groupby("keys").var("vals", ddof=0).collect()
    row = result.to_pydict()
    assert abs(row["vals"][0] - 2.0 / 3.0) < 1e-10


def test_grouped_variance_single_item_group(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "b"], "vals": [1, 2, 100]})
    result = df.groupby("keys").var("vals").collect()
    result_dict = result.sort("keys").to_pydict()
    assert result_dict["vals"][0] is not None
    assert result_dict["vals"][1] is None


def test_grouped_variance_all_columns(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a"], "x": [0, 1, 2], "y": [10, 20, 30]})
    result = df.groupby("keys").var().collect()
    row = result.to_pydict()
    assert abs(row["x"][0] - 1.0) < 1e-10
    assert abs(row["y"][0] - 100.0) < 1e-10
