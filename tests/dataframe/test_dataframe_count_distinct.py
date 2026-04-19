from __future__ import annotations

import pytest

from daft import DataType, col


def test_global_count_distinct_basic(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 2, 3, 3, 3]})
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [3]


def test_global_count_distinct_all_same(make_df, with_morsel_size):
    df = make_df({"a": [5, 5, 5]})
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [1]


def test_global_count_distinct_all_unique(make_df, with_morsel_size):
    df = make_df({"a": [1, 2, 3, 4, 5]})
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [5]


def test_global_count_distinct_with_nulls(make_df, with_morsel_size):
    df = make_df({"a": [1, None, 2, None, 2]})
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [2]


def test_global_count_distinct_all_nulls(make_df, with_morsel_size):
    df = make_df({"a": [None, None, None]}).with_column("a", col("a").cast(DataType.int64()))
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [0]


def test_global_count_distinct_strings(make_df, with_morsel_size):
    df = make_df({"a": ["foo", "bar", "foo", "baz", "bar"]})
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [3]


def test_global_count_distinct_multiple_cols(make_df, with_morsel_size):
    df = make_df({"a": [1, 1, 2, 2], "b": ["x", "y", "x", "x"]})
    result = df.count_distinct("a", "b").collect()
    row = result.to_pydict()
    assert row["a"] == [2]
    assert row["b"] == [2]


@pytest.mark.parametrize("repartition_nparts", [1, 2, 4])
def test_global_count_distinct_multi_partition(make_df, repartition_nparts, with_morsel_size):
    df = make_df({"a": [1, 2, 2, 3, 3, 3]}, repartition=repartition_nparts)
    result = df.count_distinct("a").collect()
    assert result.to_pydict()["a"] == [3]


def test_grouped_count_distinct_basic(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a", "b", "b"], "vals": [1, 1, 2, 3, 3]})
    result = df.groupby("keys").count_distinct("vals").sort("keys").collect()
    row = result.to_pydict()
    assert row["keys"] == ["a", "b"]
    assert row["vals"] == [2, 1]


def test_grouped_count_distinct_with_nulls(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "a", "b", "b"], "vals": [1, None, 1, None, None]})
    result = df.groupby("keys").count_distinct("vals").sort("keys").collect()
    row = result.to_pydict()
    assert row["keys"] == ["a", "b"]
    assert row["vals"] == [1, 0]


def test_grouped_count_distinct_all_columns(make_df, with_morsel_size):
    df = make_df({"keys": ["a", "a", "b", "b"], "x": [1, 1, 2, 3], "y": ["p", "q", "p", "p"]})
    result = df.groupby("keys").count_distinct().sort("keys").collect()
    row = result.to_pydict()
    assert row["x"] == [1, 2]
    assert row["y"] == [2, 1]
