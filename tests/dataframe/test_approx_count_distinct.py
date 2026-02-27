from __future__ import annotations

import pandas as pd
import pytest

import daft
from daft import col

TESTS = [
    [[], 0],
    [[None] * 10, 0],
    [[1], 1],
    [["hello"], 1],
    [[1, 2, 3, 4, 3, 2, 1, None, None, None], 4],
    [[10] * 10, 1],
    [["Hello", "hello", "he", "hello"], 3],
    [[True, False, True, False], 2],
]


def make_df(list: list):
    return daft.from_pydict({"a": list})


def make_asymmetric_df(list) -> daft.DataFrame:
    df = daft.from_pydict({"a": list})

    if list:
        # If I just did `daft.from_pydict({ "a": [] })`, I would get a schema of type `Null`.
        # So instead, I create it with a value and then filter against it (thus giving me an empty df).
        first = list[0]
        empty_df = daft.from_pydict({"a": [first]}).where(col("a") != first).collect()
    else:
        # The list is empty so it doesn't matter what the schema is.
        empty_df = daft.from_pydict({"a": []})

    return df.concat(empty_df).into_partitions(2).collect()


def assert_equal(df: daft.DataFrame, expected):
    pd.testing.assert_series_equal(
        pd.Series(df.to_pydict()["a"]),
        pd.Series(expected),
        check_exact=True,
    )


# Purpose:
# Test regular columns with varying sizes of partitions.
# We want to test cases in which the "singular partition" optimization is hit and cases where it's not.
@pytest.mark.parametrize("data_and_expected", TESTS)
@pytest.mark.parametrize("partition_size", [None, 2, 3])
def test_approx_count_distinct(data_and_expected, partition_size, with_morsel_size):
    data, expected = data_and_expected
    df = make_df(data)
    if partition_size:
        df = df.into_partitions(partition_size).collect()
    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)


# Purpose:
# Test creating data with empty partitions against the HLL algo.
# We want to test if empty partitions mess with the final global result.
@pytest.mark.parametrize("data_and_expected", TESTS)
def test_approx_count_distinct_on_dfs_with_empty_partitions(data_and_expected, with_morsel_size):
    data, expected = data_and_expected
    df = make_asymmetric_df(data)
    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)


# Purpose:
# Test the adding `NULL`s values to the existing data.
# We want to test how HLL handles null values.
#
# We should always test `NULL` values as the "absence" of values.
# Therefore, the existence of a `NULL` should never affect the approx_count_distinct value that is returned (even if it's in a column of type `NULL`).
@pytest.mark.parametrize("data_and_expected", TESTS)
def test_approx_count_distinct_on_null_values(data_and_expected, with_morsel_size):
    data, expected = data_and_expected
    data = data + [None] * 10
    df = make_df(data)
    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)


def test_approx_count_distinct_groupby(with_morsel_size):
    df = daft.from_pydict(
        {
            "group": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
            "values": [1, 2, 2, 3, 3, 3, 4, 5, None],
        }
    )
    result = (
        df.groupby("group").agg(col("values").approx_count_distinct().alias("approx_distinct")).sort("group").collect()
    )

    result_dict = result.to_pydict()
    assert result_dict["group"] == ["A", "B", "C"]
    assert result_dict["approx_distinct"] == [2, 1, 2]


def test_approx_count_distinct_groupby_with_nulls(with_morsel_size):
    df = daft.from_pydict(
        {
            "group": ["A", "A", "A", "B", "B", "B"],
            "values": [1, None, None, None, None, None],
        }
    )
    result = (
        df.groupby("group").agg(col("values").approx_count_distinct().alias("approx_distinct")).sort("group").collect()
    )

    result_dict = result.to_pydict()
    assert result_dict["group"] == ["A", "B"]
    assert result_dict["approx_distinct"] == [1, 0]


def test_approx_count_distinct_groupby_multiple_partitions(with_morsel_size):
    df = daft.from_pydict(
        {
            "group": ["A"] * 5 + ["B"] * 5 + ["C"] * 5,
            "values": [1, 2, 3, 4, 5] + [10, 10, 10, 10, 10] + [1, 1, 2, 2, 3],
        }
    ).into_partitions(3)
    result = (
        df.groupby("group").agg(col("values").approx_count_distinct().alias("approx_distinct")).sort("group").collect()
    )

    result_dict = result.to_pydict()
    assert result_dict["group"] == ["A", "B", "C"]
    assert result_dict["approx_distinct"] == [5, 1, 3]
