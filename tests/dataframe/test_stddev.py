from __future__ import annotations

import functools
import math
from typing import Any

import pandas as pd
import pytest

import daft


def grouped_stddev(rows, ddof=1) -> tuple[list[Any], list[Any]]:
    map = {}
    for key, data in rows:
        if key not in map:
            map[key] = []
        map[key].append(data)

    keys = []
    stddevs = []
    for key, nums in map.items():
        keys.append(key)
        stddevs.append(stddev(nums, ddof))

    return keys, stddevs


def stddev(nums, ddof=1) -> float | None:
    nums = [num for num in nums if num is not None]

    count = len(nums)
    if count <= ddof:
        return None

    sum_: float = sum(nums)
    mean = sum_ / count

    squared_sums = functools.reduce(lambda acc, num: acc + (num - mean) ** 2, nums, 0)
    stddev = math.sqrt(squared_sums / (count - ddof))
    return stddev


TESTS = [
    [0],
    [1],
    [0, 1, 2],
    [100, 100, 100],
    [None, 100, None],
    [None] * 10 + [100],
]


@pytest.mark.parametrize("nums", TESTS)
@pytest.mark.parametrize("ddof", [0, 1])
def test_stddev_with_single_partition(nums, ddof, with_morsel_size):
    expected = stddev(nums, ddof=ddof)
    df = daft.from_pydict({"a": nums})
    result = df.stddev("a", ddof=ddof).collect()
    rows = result.iter_rows()
    std = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    assert std["a"] == expected


@pytest.mark.parametrize("nums", TESTS)
@pytest.mark.parametrize("ddof", [0, 1])
def test_stddev_with_multiple_partitions(nums, ddof, with_morsel_size):
    expected = stddev(nums, ddof=ddof)
    df = daft.from_pydict({"a": nums}).into_partitions(2)
    result = df.stddev("a", ddof=ddof).collect()
    rows = result.iter_rows()
    std = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    assert std["a"] == expected


GROUPED_TESTS = [
    [("k1", 0), ("k2", 1), ("k1", 1)],
    [("k0", 100), ("k1", 100), ("k2", 100)],
    [("k0", 100), ("k0", 100), ("k0", 100)],
    [("k0", 0), ("k0", 1), ("k0", 2)],
    [("k0", None), ("k0", None), ("k0", 100)],
]


def unzip_rows(rows: list) -> tuple[list, list]:
    keys = []
    nums = []
    for key, data in rows:
        keys.append(key)
        nums.append(data)
    return keys, nums


@pytest.mark.parametrize("nums", GROUPED_TESTS)
@pytest.mark.parametrize("ddof", [0, 1])
def test_grouped_stddev_with_single_partition(nums, ddof, with_morsel_size):
    expected_keys, expected_stddevs = grouped_stddev(nums, ddof=ddof)
    expected_df = daft.from_pydict({"keys": expected_keys, "data": expected_stddevs})
    keys, data = unzip_rows(nums)
    df = daft.from_pydict({"keys": keys, "data": data})
    result_df = df.groupby("keys").stddev("data", ddof=ddof).collect()

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


@pytest.mark.parametrize("nums", GROUPED_TESTS)
@pytest.mark.parametrize("ddof", [0, 1])
def test_grouped_stddev_with_multiple_partitions(nums, ddof, with_morsel_size):
    expected_keys, expected_stddevs = grouped_stddev(nums, ddof=ddof)
    expected_df = daft.from_pydict({"keys": expected_keys, "data": expected_stddevs})
    keys, data = unzip_rows(nums)
    df = daft.from_pydict({"keys": keys, "data": data}).into_partitions(2)
    result_df = df.groupby("keys").stddev("data", ddof=ddof).collect()

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
