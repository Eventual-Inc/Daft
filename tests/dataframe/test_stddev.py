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


def stddev_with_ddof(nums, ddof: int = 0) -> float:
    """Calculate standard deviation with delta degrees of freedom."""
    nums = [num for num in nums if num is not None]

    if len(nums) <= ddof:
        return 0.0 if len(nums) == 0 else float("nan")

    sum_: float = sum(nums)
    count = len(nums)
    mean = sum_ / count

    squared_sums = functools.reduce(lambda acc, num: acc + (num - mean) ** 2, nums, 0)
    stddev = math.sqrt(squared_sums / (count - ddof))
    return stddev


def grouped_stddev_with_ddof(rows, ddof: int = 0) -> tuple[list[Any], list[Any]]:
    map = {}
    for key, data in rows:
        if key not in map:
            map[key] = []
        map[key].append(data)

    keys = []
    stddevs = []
    for key, nums in map.items():
        keys.append(key)
        stddevs.append(stddev_with_ddof(nums, ddof))

    return keys, stddevs


TESTS = [
    [nums := [0], stddev(nums)],
    [nums := [1], stddev(nums)],
    [nums := [0, 1, 2], stddev(nums)],
    [nums := [100, 100, 100], stddev(nums)],
    [nums := [None, 100, None], stddev(nums)],
    [nums := [None] * 10 + [100], stddev(nums)],
]

# Test cases for ddof functionality
DDOF_TESTS = [
    # [data, ddof, expected_result]
    [[0, 1, 2], 0, stddev_with_ddof([0, 1, 2], 0)],  # Population stddev
    [[0, 1, 2], 1, stddev_with_ddof([0, 1, 2], 1)],  # Sample stddev
    [[1, 2, 3, 4, 5], 0, stddev_with_ddof([1, 2, 3, 4, 5], 0)],
    [[1, 2, 3, 4, 5], 1, stddev_with_ddof([1, 2, 3, 4, 5], 1)],
    [[10, 20, 30], 0, stddev_with_ddof([10, 20, 30], 0)],
    [[10, 20, 30], 1, stddev_with_ddof([10, 20, 30], 1)],
    [[100, 100, 100], 0, stddev_with_ddof([100, 100, 100], 0)],  # All same values
    [[100, 100, 100], 1, stddev_with_ddof([100, 100, 100], 1)],
    [[None, 1, 2, 3, None], 0, stddev_with_ddof([None, 1, 2, 3, None], 0)],  # With nulls
    [[None, 1, 2, 3, None], 1, stddev_with_ddof([None, 1, 2, 3, None], 1)],
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


@pytest.mark.parametrize("data_ddof_and_expected", DDOF_TESTS)
def test_stddev_with_ddof_single_partition(data_ddof_and_expected, with_morsel_size):
    data, ddof, expected = data_ddof_and_expected
    df = daft.from_pydict({"a": data})
    result = df.agg(daft.col("a").stddev(ddof)).collect()
    rows = result.iter_rows()
    stddev_result = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    if math.isnan(expected):
        assert math.isnan(stddev_result["a"])
    else:
        assert abs(stddev_result["a"] - expected) < 1e-10


@pytest.mark.parametrize("data_ddof_and_expected", DDOF_TESTS)
def test_stddev_with_ddof_multiple_partitions(data_ddof_and_expected, with_morsel_size):
    data, ddof, expected = data_ddof_and_expected
    df = daft.from_pydict({"a": data}).into_partitions(2)
    result = df.agg(daft.col("a").stddev(ddof)).collect()
    rows = result.iter_rows()
    stddev_result = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    if math.isnan(expected):
        assert math.isnan(stddev_result["a"])
    else:
        assert abs(stddev_result["a"] - expected) < 1e-10


def test_stddev_ddof_grouped():
    """Test grouped stddev with ddof parameter."""
    # Test data with multiple groups
    keys = ["a", "a", "a", "b", "b", "c"]
    data = [1, 2, 3, 10, 20, 100]
    df = daft.from_pydict({"keys": keys, "data": data})

    # Test with ddof=0 (population)
    result_pop = df.groupby("keys").agg(daft.col("data").stddev(0)).sort("keys").collect()

    # Test with ddof=1 (sample)
    result_sample = df.groupby("keys").agg(daft.col("data").stddev(1)).sort("keys").collect()

    # Manually calculate expected values
    expected_pop_a = stddev_with_ddof([1, 2, 3], 0)
    expected_sample_a = stddev_with_ddof([1, 2, 3], 1)
    expected_pop_b = stddev_with_ddof([10, 20], 0)
    expected_sample_b = stddev_with_ddof([10, 20], 1)
    expected_pop_c = stddev_with_ddof([100], 0)
    expected_sample_c = stddev_with_ddof([100], 1)

    result_pop_dict = result_pop.to_pydict()
    result_sample_dict = result_sample.to_pydict()

    # Check population stddev results
    assert abs(result_pop_dict["data"][0] - expected_pop_a) < 1e-10  # group 'a'
    assert abs(result_pop_dict["data"][1] - expected_pop_b) < 1e-10  # group 'b'
    assert abs(result_pop_dict["data"][2] - expected_pop_c) < 1e-10  # group 'c'

    # Check sample stddev results
    assert abs(result_sample_dict["data"][0] - expected_sample_a) < 1e-10  # group 'a'
    assert abs(result_sample_dict["data"][1] - expected_sample_b) < 1e-10  # group 'b'
    if math.isnan(expected_sample_c):
        assert math.isnan(result_sample_dict["data"][2])  # group 'c' (only 1 element, ddof=1 -> NaN)
    else:
        assert abs(result_sample_dict["data"][2] - expected_sample_c) < 1e-10


def test_stddev_ddof_default_behavior():
    """Test that stddev() without parameters defaults to ddof=0."""
    data = [1, 2, 3, 4, 5]
    df = daft.from_pydict({"a": data})

    result_default = df.agg(daft.col("a").stddev()).collect()
    result_explicit = df.agg(daft.col("a").stddev(0)).collect()

    default_value = next(result_default.iter_rows())["a"]
    explicit_value = next(result_explicit.iter_rows())["a"]

    assert abs(default_value - explicit_value) < 1e-10


def test_stddev_ddof_edge_cases():
    # Test with empty data
    df_empty = daft.from_pydict({"a": []})
    result_empty = df_empty.agg(daft.col("a").stddev(1)).collect()
    empty_result = next(result_empty.iter_rows())["a"]
    assert empty_result is None

    # Test with single element and ddof=1 (should be NaN)
    df_single = daft.from_pydict({"a": [42]})
    result_single = df_single.agg(daft.col("a").stddev(1)).collect()
    single_result = next(result_single.iter_rows())["a"]
    assert math.isnan(single_result)

    # Test with all null values
    df_nulls = daft.from_pydict({"a": [None, None, None]})
    result_nulls = df_nulls.agg(daft.col("a").stddev(1)).collect()
    nulls_result = next(result_nulls.iter_rows())["a"]
    assert nulls_result is None
