from __future__ import annotations

import pytest

from daft import col

from . import test_approx_count_distinct


@pytest.mark.parametrize("data_and_expected", test_approx_count_distinct.TESTS)
@pytest.mark.parametrize("partition_size", [None, 2, 3])
def test_count_distinct(data_and_expected, partition_size):
    data, expected = data_and_expected
    df = test_approx_count_distinct.make_df(data)
    if partition_size:
        df = df.into_partitions(partition_size).collect()
    df = df.agg(col("a").count_distinct()).collect()
    test_approx_count_distinct.assert_equal(df, expected)
