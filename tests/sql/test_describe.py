from __future__ import annotations

import datetime

import pytest

import daft

df = daft.from_pydict(
    {
        "integers": [1, 2, 3, 4],
        "floats": [1.5, 2.5, 3.5, 4.5],
        "bools": [True, True, False, False],
        "strings": ["a", "b", "c", "d"],
        "bytes": [b"a", b"b", b"c", b"d"],
        "dates": [
            datetime.date(1994, 1, 1),
            datetime.date(1994, 1, 2),
            datetime.date(1994, 1, 3),
            datetime.date(1994, 1, 4),
        ],
        "lists": [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]],
        "nulls": [None, None, None, None],
    }
)


def assert_eq(df1, df2):
    assert df1.collect().to_pydict() == df2.collect().to_pydict()


def test_describe_table():
    actual_df = daft.sql("DESCRIBE df")
    expect_df = df.describe()
    assert_eq(actual_df, expect_df)


@pytest.mark.skip("DESCRIBE TABLE syntax not supported")
def test_describe_table_with_keyword():
    actual_df = daft.sql("DESCRIBE TABLE df")
    expect_df = df.describe()
    assert_eq(actual_df, expect_df)


def test_describe_select_all():
    actual_df = daft.sql("DESCRIBE SELECT * FROM df")
    expect_df = df.describe()
    assert_eq(actual_df, expect_df)


def test_describe_select_one():
    actual_df = daft.sql("DESCRIBE SELECT integers FROM df")
    expect_df = df.select("integers").describe()
    assert_eq(actual_df, expect_df)


def test_describe_select_some():
    actual_df = daft.sql("DESCRIBE SELECT integers, floats, bools FROM df")
    expect_df = df.select("integers", "floats", "bools").describe()
    assert_eq(actual_df, expect_df)
