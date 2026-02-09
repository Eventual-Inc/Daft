from __future__ import annotations

import functools
import math

import pytest

from daft import Series


def stddev(nums: list[float | None], ddof: int = 0) -> float | None:
    nums = [num for num in nums if num is not None]

    count = len(nums)
    if count <= ddof:
        return None

    mean = sum(nums) / count
    squared_sums = functools.reduce(lambda acc, num: acc + (num - mean) ** 2, nums, 0.0)
    return math.sqrt(squared_sums / (count - ddof))


@pytest.mark.parametrize(
    "data",
    [
        [0.0, 1.0, 2.0],
        [100.0, 100.0, 100.0],
        [None, 100.0, None],
    ],
)
def test_series_stddev_default(data):
    series = Series.from_pylist(data)
    expected = stddev(data)
    result = series.stddev().to_pylist()[0]

    if expected is None:
        assert result is None
    else:
        assert abs(result - expected) < 1e-10


@pytest.mark.parametrize("ddof", [0, 1])
def test_series_stddev_with_ddof(ddof):
    data = [0.0, 1.0, 2.0, None]
    series = Series.from_pylist(data)
    expected = stddev(data, ddof=ddof)
    result = series.stddev(ddof=ddof).to_pylist()[0]

    if expected is None:
        assert result is None
    else:
        assert abs(result - expected) < 1e-10


def test_series_stddev_sample_single_value():
    series = Series.from_pylist([5.0])
    result = series.stddev(ddof=1).to_pylist()[0]
    assert result is None
