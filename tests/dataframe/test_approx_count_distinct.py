import pandas as pd
import pytest

import daft
from daft import col


@pytest.mark.parametrize(
    "data_and_expected",
    [
        [[1, 2, 3, 4, 3, 2, 1], 4],
        [[10] * 10, 1],
    ],
)
@pytest.mark.parametrize("partition_size", [None, 3])
def test_approx_count_distinct_on_singular_partition(data_and_expected, partition_size):
    data, expected = data_and_expected
    df = daft.from_pydict({"a": data})
    if partition_size:
        df.into_partitions(partition_size)
    daft_cols = df.agg(col("a").approx_count_distinct()).to_pydict()
    pd.testing.assert_series_equal(
        pd.Series(daft_cols["a"]),
        pd.Series(expected),
        check_exact=True,
    )
