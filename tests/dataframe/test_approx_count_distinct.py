import pandas as pd
import pytest

import daft
from daft import col


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


TESTS = [
    [[], 0],
    [[1, 2, 3, 4, 3, 2, 1], 4],
    [[10] * 10, 1],
    [["Hello", "hello", "he", "hello"], 3],
    [[True, False, True, False], 2],
]


@pytest.mark.parametrize("data_and_expected", TESTS)
@pytest.mark.parametrize("partition_size", [None, 2, 3])
def test_approx_count_distinct(data_and_expected, partition_size):
    data, expected = data_and_expected
    df = make_df(data)
    if partition_size:
        df = df.into_partitions(partition_size).collect()

    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_approx_count_distinct_on_dfs_with_empty_partitions(data_and_expected):
    data, expected = data_and_expected
    df = make_asymmetric_df(data)

    df = df.agg(col("a").approx_count_distinct())
    assert_equal(df, expected)