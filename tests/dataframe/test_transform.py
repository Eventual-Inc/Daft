from __future__ import annotations

import pytest

from daft import col


def add_1(df):
    return df.with_column("foo", col("foo") + 1)


def multiply_x(df, x):
    return df.with_column("foo", col("foo") * x)


def concat_dfs(df, df_2):
    return df.concat(df_2)


def invalid_function(df):
    return df.to_pydict()


def test_transform_no_args(make_df):
    df = make_df({"foo": [1, 2, 3]})
    with pytest.raises(ValueError):
        df.transform(add_1)


def test_transform_args(make_df):
    df = make_df({"foo": [1, 2, 3]})
    with pytest.raises(ValueError):
        df.transform(multiply_x, 2)


def test_transform_kwargs(make_df):
    df = make_df({"foo": [1, 2, 3]})
    with pytest.raises(ValueError):
        df.transform(multiply_x, x=2)


def test_transform_multiple_df(make_df):
    df = make_df({"foo": [1, 2, 3]})
    df_2 = make_df({"foo": [4, 5, 6]})
    with pytest.raises(ValueError):
        df.transform(concat_dfs, df_2)


def test_transform_negative_case(make_df):
    df = make_df({"foo": [1, 2, 3]})
    try:
        df.transform(invalid_function)
    except Exception as e:
        assert "should have been DataFrame" in str(e)
