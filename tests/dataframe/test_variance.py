from __future__ import annotations

import functools
import statistics
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
    # Large mean, tiny spread: E(x^2) - E(x)^2 collapses to 0 here (see #7468).
    [nums := [1e9 + 1, 1e9 + 2, 1e9 + 3], var(nums, ddof=0), var(nums, ddof=1)],
    [nums := [1e12, 1e12 + 1, 1e12 + 2], var(nums, ddof=0), var(nums, ddof=1)],
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
    # Large mean, plus a single-row group that must stay NULL at ddof=1 (see #7468).
    [rows := [("k0", 1e9 + 1), ("k0", 1e9 + 2), ("k1", 1e9 + 3)], *grouped_var(rows)],
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


def test_var_large_mean_matches_shifted_data(with_morsel_size):
    """Regression test for https://github.com/Eventual-Inc/Daft/issues/7468.

    The old `E(x^2) - E(x)^2` rewrite collapsed to 0.0 for data with a large
    mean; the Chan parallel merge must match the shifted (small-mean) result.
    """
    data = [1e9 + 1, 1e9 + 2, 1e9 + 3]
    for ddof, expected in [(1, 1.0), (0, 2.0 / 3.0)]:
        df = daft.from_pydict({"a": data, "g": [1, 1, 1]})
        row = next(
            df.select(
                daft.col("a").var(ddof=ddof).alias("var"),
                daft.col("a").stddev(ddof=ddof).alias("std"),
            )
            .collect()
            .iter_rows()
        )
        assert row["var"] == expected
        assert abs(row["std"] - expected**0.5) < 1e-12

        grouped = next(
            df.groupby("g")
            .agg(
                daft.col("a").var(ddof=ddof).alias("var"),
                daft.col("a").stddev(ddof=ddof).alias("std"),
            )
            .collect()
            .iter_rows()
        )
        assert grouped["var"] == expected
        assert abs(grouped["std"] - expected**0.5) < 1e-12


@pytest.mark.parametrize("base", [1e9, 1e12, 1e15])
def test_var_large_mean_large_partition(base, with_default_morsel_size):
    """Large mean *and* a large partition (see #7468).

    Every other large-mean test here uses three values, so whatever the morsel
    size, the per-partition summary is computed over a handful of rows and only
    the cross-partition merge is really exercised. With the default morsel size
    these 20k rows land in a single partial aggregate, so this is the only test
    that exercises the per-partition variance kernel at scale -- which is where
    `E(x^2) - E(x)^2`, a mean reconstructed from a naive sum, and a Welford
    running mean each lose the rest of their accuracy.

    `statistics.variance` is the reference because it is computed in exact
    rational arithmetic, unlike the float `var()` helper above.
    """
    n = 20_000
    data = [base + ((i * 7919) % 1000) / 1000.0 for i in range(n)]
    expected = statistics.variance(data)

    df = daft.from_pydict({"a": data, "g": [1] * n})

    row = next(df.agg(daft.col("a").var().alias("var"), daft.col("a").stddev().alias("std")).collect().iter_rows())
    assert row["var"] == pytest.approx(expected, rel=1e-9)
    assert row["std"] == pytest.approx(expected**0.5, rel=1e-9)

    grouped = next(df.groupby("g").agg(daft.col("a").var().alias("var")).collect().iter_rows())
    assert grouped["var"] == pytest.approx(expected, rel=1e-9)
