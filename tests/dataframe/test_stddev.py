import functools
import math

import pytest

import daft


def stddev(nums) -> float:
    def sum_reducer(acc, num):
        return acc + num if num is not None else acc

    def stddev_reducer(acc, num):
        return acc + (num - mean) ** 2 if num is not None else acc

    if not nums:
        return 0.0
    # sum_: float = sum(nums)
    sum = functools.reduce(sum_reducer, nums, 0)
    count = len(nums)
    mean = sum / count

    squared_sums = functools.reduce(stddev_reducer, nums, 0)
    stddev = math.sqrt(squared_sums / count)
    return stddev


TESTS = [
    [nums := [0], stddev(nums)],
    [nums := [1], stddev(nums)],
    [nums := [0, 1, 2], stddev(nums)],
    [nums := [0, 0, 0], stddev(nums)],
    [nums := [None, 0, None], stddev(nums)],
    [nums := [None] * 10 + [0], stddev(nums)],
]


@pytest.mark.parametrize("data_and_expected", TESTS)
def test_stddev_with_single_partition(data_and_expected):
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
def test_stddev_with_multiple_partitions(data_and_expected):
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
