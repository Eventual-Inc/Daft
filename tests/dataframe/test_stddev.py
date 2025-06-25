from __future__ import annotations

import functools
import math
from typing import Any

import pandas as pd
import pytest

import daft


def grouped_stddev(rows) -> tuple[list[Any], list[Any]]:
    map = {}
    for key, data in rows:
        if key not in map:
            map[key] = []
        map[key].append(data)

    keys = []
    stddevs = []
    for key, nums in map.items():
        keys.append(key)
        stddevs.append(stddev(nums))

    return keys, stddevs


def stddev(nums) -> float:
    nums = [num for num in nums if num is not None]

    if not nums:
        return 0.0
    sum_: float = sum(nums)
    count = len(nums)
    mean = sum_ / count

    squared_sums = functools.reduce(lambda acc, num: acc + (num - mean) ** 2, nums, 0)
    stddev = math.sqrt(squared_sums / count)
    return stddev


TESTS = [
    [nums := [0], stddev(nums)],
    [nums := [1], stddev(nums)],
    [nums := [0, 1, 2], stddev(nums)],
    [nums := [100, 100, 100], stddev(nums)],
    [nums := [None, 100, None], stddev(nums)],
    [nums := [None] * 10 + [100], stddev(nums)],
]


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_stddev_with_single_partition(data_and_expected, with_morsel_size):
    data, expected = data_and_expected
    df = daft.from_pydict({"a": data})
    result = df.agg(daft.col("a").stddev()).collect()
    rows = result.iter_rows()
    stddev = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    assert stddev["a"] == expected


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_stddev_with_multiple_partitions(data_and_expected, with_morsel_size):
    data, expected = data_and_expected
    df = daft.from_pydict({"a": data}).into_partitions(2)
    result = df.agg(daft.col("a").stddev()).collect()
    rows = result.iter_rows()
    stddev = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    assert stddev["a"] == expected


GROUPED_TESTS = [
    [rows := [("k1", 0), ("k2", 1), ("k1", 1)], *grouped_stddev(rows)],
    [rows := [("k0", 100), ("k1", 100), ("k2", 100)], *grouped_stddev(rows)],
    [rows := [("k0", 100), ("k0", 100), ("k0", 100)], *grouped_stddev(rows)],
    [rows := [("k0", 0), ("k0", 1), ("k0", 2)], *grouped_stddev(rows)],
    [rows := [("k0", None), ("k0", None), ("k0", 100)], *grouped_stddev(rows)],
]


def unzip_rows(rows: list) -> tuple[list, list]:
    keys = []
    nums = []
    for key, data in rows:
        keys.append(key)
        nums.append(data)
    return keys, nums


@pytest.mark.parametrize("data_and_expected", GROUPED_TESTS)
def test_grouped_stddev_with_single_partition(data_and_expected, with_morsel_size):
    nums, expected_keys, expected_stddevs = data_and_expected
    expected_df = daft.from_pydict({"keys": expected_keys, "data": expected_stddevs})
    keys, data = unzip_rows(nums)
    df = daft.from_pydict({"keys": keys, "data": data})
    result_df = df.groupby("keys").agg(daft.col("data").stddev()).collect()

    result = result_df.to_pydict()
    expected = expected_df.to_pydict()

    pd.testing.assert_series_equal(
        pd.Series(result["keys"]).sort_values(),
        pd.Series(expected["keys"]).sort_values(),
        check_index=False,
    )
    pd.testing.assert_series_equal(
        pd.Series(result["data"]).sort_values(),
        pd.Series(expected["data"]).sort_values(),
        check_index=False,
    )


@pytest.mark.parametrize("data_and_expected", GROUPED_TESTS)
def test_grouped_stddev_with_multiple_partitions(data_and_expected, with_morsel_size):
    nums, expected_keys, expected_stddevs = data_and_expected
    expected_df = daft.from_pydict({"keys": expected_keys, "data": expected_stddevs})
    keys, data = unzip_rows(nums)
    df = daft.from_pydict({"keys": keys, "data": data}).into_partitions(2)
    result_df = df.groupby("keys").agg(daft.col("data").stddev()).collect()

    result = result_df.to_pydict()
    expected = expected_df.to_pydict()

    pd.testing.assert_series_equal(
        pd.Series(result["keys"]).sort_values(),
        pd.Series(expected["keys"]).sort_values(),
        check_index=False,
    )
    pd.testing.assert_series_equal(
        pd.Series(result["data"]).sort_values(),
        pd.Series(expected["data"]).sort_values(),
        check_index=False,
    )
