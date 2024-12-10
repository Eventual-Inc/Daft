from __future__ import annotations

import daft
from daft import col


def test_simple_intersect(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"bar": [2, 3, 4]})
    result = df1.intersect(df2)
    assert result.to_pydict() == {"foo": [2, 3]}
    df1 = make_df({"foo": [1, 2, 2]})
    df2 = make_df({"bar": [2, 2, 4]})
    result = df1.intersect_all(df2)
    assert result.to_pydict() == {"foo": [2, 2]}


def test_simple_except(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"bar": [2, 3, 4]})
    result = df1.except_distinct(df2)
    assert result.to_pydict() == {"foo": [1]}
    df1 = make_df({"foo": [1, 1, 1, 2, 4, 4]})
    df2 = make_df({"bar": [1, 2, 2, 4]})
    result = df1.except_all(df2).sort(by="foo")
    assert result.to_pydict() == {"foo": [1, 1, 4]}


def test_intersect_with_duplicate(make_df):
    df1 = make_df({"foo": [1, 2, 2, 3]})
    df2 = make_df({"bar": [2, 3, 3]})
    result = df1.intersect(df2)
    assert result.to_pydict() == {"foo": [2, 3]}
    result = df1.intersect_all(df2)
    assert result.to_pydict() == {"foo": [2, 3]}


def test_except_with_duplicate(make_df):
    df1 = make_df({"foo": [1, 2, 2, 3]})
    df2 = make_df({"bar": [2, 3, 3]})
    result = df1.except_distinct(df2)
    assert result.to_pydict() == {"foo": [1]}
    result = df1.except_all(df2).sort(by="foo")
    assert result.to_pydict() == {"foo": [1, 2]}


def test_self_intersect(make_df):
    df = make_df({"foo": [1, 2, 3]})
    result = df.intersect(df).sort(by="foo")
    assert result.to_pydict() == {"foo": [1, 2, 3]}
    result = df.intersect_all(df).sort(by="foo")
    assert result.to_pydict() == {"foo": [1, 2, 3]}


def test_self_except(make_df):
    df = make_df({"foo": [1, 2, 3]})
    result = df.except_distinct(df).sort(by="foo")
    assert result.to_pydict() == {"foo": []}
    result = df.except_all(df).sort(by="foo")
    assert result.to_pydict() == {"foo": []}


def test_intersect_empty(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"bar": []}).select(col("bar").cast(daft.DataType.int64()))
    result = df1.intersect(df2)
    assert result.to_pydict() == {"foo": []}
    result = df1.intersect_all(df2)
    assert result.to_pydict() == {"foo": []}


def test_except_empty(make_df):
    df1 = make_df({"foo": [1, 2, 3]})
    df2 = make_df({"bar": []}).select(col("bar").cast(daft.DataType.int64()))
    result = df1.except_distinct(df2).sort(by="foo")
    assert result.to_pydict() == {"foo": [1, 2, 3]}
    result = df1.except_all(df2).sort(by="foo")
    assert result.to_pydict() == {"foo": [1, 2, 3]}


def test_intersect_with_nulls(make_df):
    df1 = make_df({"foo": [1, 2, None]})
    df1_without_mull = make_df({"foo": [1, 2]})
    df2 = make_df({"bar": [2, 3, None]})
    df2_without_null = make_df({"bar": [2, 3]})

    result = df1.intersect(df2)
    assert result.to_pydict() == {"foo": [2, None]}
    result = df1.intersect_all(df2)
    assert result.to_pydict() == {"foo": [2, None]}

    result = df1_without_mull.intersect(df2)
    assert result.to_pydict() == {"foo": [2]}
    result = df1_without_mull.intersect_all(df2)
    assert result.to_pydict() == {"foo": [2]}

    result = df1.intersect(df2_without_null)
    assert result.to_pydict() == {"foo": [2]}
    result = df1.intersect_all(df2_without_null)
    assert result.to_pydict() == {"foo": [2]}


def test_except_with_nulls(make_df):
    df1 = make_df({"foo": [1, 2, None]})
    df1_without_mull = make_df({"foo": [1, 2]})
    df2 = make_df({"bar": [2, 3, None]})
    df2_without_null = make_df({"bar": [2, 3]})

    result = df1.except_distinct(df2)
    assert result.to_pydict() == {"foo": [1]}
    result = df1.except_all(df2)
    assert result.to_pydict() == {"foo": [1]}

    result = df1_without_mull.except_distinct(df2)
    assert result.to_pydict() == {"foo": [1]}
    result = df1_without_mull.except_all(df2)
    assert result.to_pydict() == {"foo": [1]}

    result = df1.except_distinct(df2_without_null)
    assert result.to_pydict() == {"foo": [1, None]}
    result = df1.except_all(df2_without_null)
    assert result.to_pydict() == {"foo": [1, None]}
