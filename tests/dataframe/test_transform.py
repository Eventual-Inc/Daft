from __future__ import annotations

import pytest

import daft


def add_1(df):
    return df.with_column("foo", daft.col("foo") + 1)


def multiply_x(df, x):
    return df.with_column("foo", daft.col("foo") * x)


def concat_dfs(df, df_2):
    return df.concat(df_2)


def invalid_function(df):
    return df.to_pydict()


def test_transform_no_args(make_df):
    df = make_df({"foo": [1, 2, 3]})
    assert df.transform(add_1).to_pydict() == {"foo": [2, 3, 4]}


def test_transform_args(make_df):
    df = make_df({"foo": [1, 2, 3]})
    assert df.transform(multiply_x, 2).to_pydict() == {"foo": [2, 4, 6]}


def test_transform_kwargs(make_df):
    df = make_df({"foo": [1, 2, 3]})
    assert df.transform(multiply_x, x=2).to_pydict() == {"foo": [2, 4, 6]}


def test_transform_multiple_df(make_df):
    df = make_df({"foo": [1, 2, 3]})
    df_2 = make_df({"foo": [4, 5, 6]})
    assert df.transform(concat_dfs, df_2).to_pydict() == {"foo": [1, 2, 3, 4, 5, 6]}


def test_transform_negative_case(make_df):
    df = make_df({"foo": [1, 2, 3]})
    with pytest.raises(AssertionError):
        df.transform(invalid_function)
