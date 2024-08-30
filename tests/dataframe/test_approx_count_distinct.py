import pandas as pd
import pytest

import daft
from daft import col

TESTS = [
    [[], 0],
    [[1], 1],
    [["hello"], 1],
    [[1, 2, 3, 4, 3, 2, 1], 4],
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
def test_approx_count_distinct(data_and_expected, partition_size):
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
def test_approx_count_distinct_on_dfs_with_empty_partitions(data_and_expected):
    data, expected = data_and_expected
    df = make_asymmetric_df(data)
    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)


# Purpose:
# Test the adding `NULL`s values to the existing data.
# We want to test how HLL handles null values.
#
# Edge case:
# Assume that a given row's type is `INT64`.
# The presence of a `NULL` value in that row should be thought of as the "absence of a value".
# Therefore, a `NULL` value's existence should *not* affect the count of distinct result.
#
# For example, if we do `daft.from_pydict({ "a": [1, 2, 1] + [None] })`, the schema of column "a" is `INT64`.
# Thus, the presence of `None` should not count towards the final count-distinct value (which will be 2 in this case).
#
# However, now assume that a given row's type is `NULL`.
# The presence of a `NULL` value in that situation should now be thought of as the "presence of a value".
#
# For example, if we do `daft.from_pydict({ "a": [] + [None] })`, the schema of column "a" is `NULL`.
# Here, the presence of `None` *should* count towards the final count-distinct value.
#
# That's why I check `if not data: ...` in the below test.
# If `data` is empty prior to adding `None`s, the type of column "a" will be `NULL`.
# If `data` is *not* empty, the type of column "a" will be whatever the corresponding type of that list is.
@pytest.mark.parametrize("data_and_expected", TESTS)
def test_approx_count_distinct_on_null_values(data_and_expected):
    data, expected = data_and_expected
    if not data:
        expected = expected + 1
    data = data + [None] * 10
    df = make_df(data)
    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)
