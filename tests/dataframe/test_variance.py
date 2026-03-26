from __future__ import annotations

import functools
from typing import Any

import pandas as pd
import pytest

import daft


def grouped_var(rows, ddof=1) -> tuple[list[Any], list[Any]]:
    map = {}
    for key, data in rows:
        if key not in map:
            map[key] = []
        map[key].append(data)

    keys = []
    variances = []
    for key, nums in map.items():
        keys.append(key)
        variances.append(var(nums, ddof))

    return keys, variances


def var(nums, ddof) -> float | None:
    nums = [num for num in nums if num is not None]

    n = len(nums)
    if n <= ddof:
        return None
    sum_: float = sum(nums)
    mean = sum_ / n

    squared_sums = functools.reduce(lambda acc, num: acc + (num - mean) ** 2, nums, 0)
    return squared_sums / (n - ddof)


TESTS = [
    [nums := [0, 1, 2], var(nums, ddof=0), var(nums, ddof=1)],
    [nums := [100, 100, 100], var(nums, ddof=0), var(nums, ddof=1)],
    [nums := [None, 100, None], var(nums, ddof=0), var(nums, ddof=1)],
    [nums := [1, 2, 3, 4, 5], var(nums, ddof=0), var(nums, ddof=1)],
    [nums := [None] * 10 + [100], var(nums, ddof=0), var(nums, ddof=1)],
]


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_var_with_ddof_0_single_partition(data_and_expected, with_morsel_size):
    data, expected_ddof0, _ = data_and_expected
    df = daft.from_pydict({"a": data})
    result = df.agg(daft.col("a").var(ddof=0)).collect()
    rows = result.iter_rows()
    variance = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    if expected_ddof0 is None:
        assert variance["a"] is None
    else:
        assert abs(variance["a"] - expected_ddof0) < 1e-10


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_var_with_ddof_1_single_partition(data_and_expected, with_morsel_size):
    data, _, expected_ddof1 = data_and_expected
    df = daft.from_pydict({"a": data})
    result = df.agg(daft.col("a").var(ddof=1)).collect()
    rows = result.iter_rows()
    variance = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    if expected_ddof1 is None:
        assert variance["a"] is None
    else:
        assert abs(variance["a"] - expected_ddof1) < 1e-10


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_var_default_ddof_single_partition(data_and_expected, with_morsel_size):
    """Test that the default ddof is 1 (sample variance)."""
    data, _, expected_ddof1 = data_and_expected
    df = daft.from_pydict({"a": data})
    result = df.agg(daft.col("a").var()).collect()
    rows = result.iter_rows()
    variance = next(rows)

    if expected_ddof1 is None:
        assert variance["a"] is None
    else:
        assert abs(variance["a"] - expected_ddof1) < 1e-10


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_var_with_ddof_0_multiple_partitions(data_and_expected, with_morsel_size):
    data, expected_ddof0, _ = data_and_expected
    df = daft.from_pydict({"a": data}).into_partitions(2)
    result = df.agg(daft.col("a").var(ddof=0)).collect()
    rows = result.iter_rows()
    variance = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    if expected_ddof0 is None:
        assert variance["a"] is None
    else:
        assert abs(variance["a"] - expected_ddof0) < 1e-10


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_var_with_ddof_1_multiple_partitions(data_and_expected, with_morsel_size):
    data, _, expected_ddof1 = data_and_expected
    df = daft.from_pydict({"a": data}).into_partitions(2)
    result = df.agg(daft.col("a").var(ddof=1)).collect()
    rows = result.iter_rows()
    variance = next(rows)
    try:
        next(rows)
        assert False
    except StopIteration:
        pass

    if expected_ddof1 is None:
        assert variance["a"] is None
    else:
        assert abs(variance["a"] - expected_ddof1) < 1e-10


def test_var_single_value_ddof_0(with_morsel_size):
    """Single value with ddof=0 should return 0."""
    df = daft.from_pydict({"a": [5.0]})
    result = df.agg(daft.col("a").var(ddof=0)).collect()
    row = next(result.iter_rows())
    assert row["a"] == 0.0


def test_var_single_value_ddof_1(with_morsel_size):
    """Single value with ddof=1 should return None (n - ddof = 0)."""
    df = daft.from_pydict({"a": [5.0]})
    result = df.agg(daft.col("a").var(ddof=1)).collect()
    row = next(result.iter_rows())
    assert row["a"] is None


def test_var_empty_array(with_morsel_size):
    """Empty array should return None."""
    df = daft.from_pydict({"a": []})
    result = df.agg(daft.col("a").var()).collect()
    row = next(result.iter_rows())
    assert row["a"] is None


def test_var_all_nulls(with_morsel_size):
    """All nulls should return None."""
    df = daft.from_pydict({"a": [None, None, None]})
    result = df.agg(daft.col("a").var()).collect()
    row = next(result.iter_rows())
    assert row["a"] is None


GROUPED_TESTS = [
    [rows := [("k1", 0), ("k2", 1), ("k1", 1)], *grouped_var(rows)],
    [rows := [("k0", 100), ("k1", 100), ("k2", 100)], *grouped_var(rows)],
    [rows := [("k0", 100), ("k0", 100), ("k0", 100)], *grouped_var(rows)],
    [rows := [("k0", 0), ("k0", 1), ("k0", 2)], *grouped_var(rows)],
    [rows := [("k0", None), ("k0", None), ("k0", 100)], *grouped_var(rows)],
]


def unzip_rows(rows: list) -> tuple[list, list]:
    keys = []
    nums = []
    for key, data in rows:
        keys.append(key)
        nums.append(data)
    return keys, nums


@pytest.mark.parametrize("data_and_expected", GROUPED_TESTS)
def test_grouped_var_with_single_partition(data_and_expected, with_morsel_size):
    nums, expected_keys, expected_variances = data_and_expected
    expected_df = daft.from_pydict({"keys": expected_keys, "data": expected_variances})
    keys, data = unzip_rows(nums)
    df = daft.from_pydict({"keys": keys, "data": data})
    result_df = df.groupby("keys").agg(daft.col("data").var(ddof=1)).collect()

    result = result_df.to_pydict()
    expected = expected_df.to_pydict()

    pd.testing.assert_series_equal(
        pd.Series(result["keys"]).sort_values().reset_index(drop=True),
        pd.Series(expected["keys"]).sort_values().reset_index(drop=True),
        check_index=False,
    )
    pd.testing.assert_series_equal(
        pd.Series(result["data"]).sort_values().reset_index(drop=True),
        pd.Series(expected["data"]).sort_values().reset_index(drop=True),
        check_index=False,
    )


@pytest.mark.parametrize("data_and_expected", GROUPED_TESTS)
def test_grouped_var_with_multiple_partitions(data_and_expected, with_morsel_size):
    nums, expected_keys, expected_variances = data_and_expected
    expected_df = daft.from_pydict({"keys": expected_keys, "data": expected_variances})
    keys, data = unzip_rows(nums)
    df = daft.from_pydict({"keys": keys, "data": data}).into_partitions(2)
    result_df = df.groupby("keys").agg(daft.col("data").var(ddof=1)).collect()

    result = result_df.to_pydict()
    expected = expected_df.to_pydict()

    pd.testing.assert_series_equal(
        pd.Series(result["keys"]).sort_values().reset_index(drop=True),
        pd.Series(expected["keys"]).sort_values().reset_index(drop=True),
        check_index=False,
    )
    pd.testing.assert_series_equal(
        pd.Series(result["data"]).sort_values().reset_index(drop=True),
        pd.Series(expected["data"]).sort_values().reset_index(drop=True),
        check_index=False,
    )


def test_var_stddev_relationship(with_morsel_size):
    """Verify that variance = stddev^2."""
    df = daft.from_pydict({"a": [1, 2, 3, 4, 5]})
    result = df.agg(
        daft.col("a").var().alias("var"),
        daft.col("a").stddev().alias("stddev"),
    ).collect()
    row = next(result.iter_rows())
    assert abs(row["var"] - row["stddev"] ** 2) < 1e-10
