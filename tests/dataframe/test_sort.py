from __future__ import annotations

import math

import pytest

import daft


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3])
def test_single_float_col_sort(desc, n_partitions):
    df = daft.DataFrame.from_pydict({"A": [1.0, None, 3.0, float("nan"), 2.0]})
    df = df.repartition(n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    def _replace_nan_with_string(l):
        return ["nan" if isinstance(item, float) and math.isnan(item) else item for item in l]

    expected = [1.0, 2.0, 3.0, float("nan"), None]
    if desc:
        expected = list(reversed(expected))

    assert _replace_nan_with_string(sorted_data["A"]) == _replace_nan_with_string(expected)
