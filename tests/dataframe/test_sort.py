from __future__ import annotations

import math

import pytest

import daft
from daft.errors import ExpressionTypeError

###
# Validation tests
###


def test_disallowed_sort_bool():
    df = daft.DataFrame.from_pydict({"A": [True, False]})

    with pytest.raises(ExpressionTypeError):
        df.sort("A")


def test_disallowed_sort_null():
    df = daft.DataFrame.from_pydict({"A": [None, None]})

    with pytest.raises(ExpressionTypeError):
        df.sort("A")


def test_disallowed_sort_bytes():
    df = daft.DataFrame.from_pydict({"A": [b"a", b"b"]})

    with pytest.raises(ExpressionTypeError):
        df.sort("A")


###
# Functional tests
###


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3])
def test_single_float_col_sort(desc: bool, n_partitions: int):
    df = daft.DataFrame.from_pydict({"A": [1.0, None, 3.0, float("nan"), 2.0]})
    df = df.repartition(n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    def _replace_nan_with_string(l):
        return ["nan" if isinstance(item, float) and math.isnan(item) else item for item in l]

    expected = [1.0, 2.0, 3.0]
    if desc:
        expected = list(reversed(expected))
    expected = expected + [float("nan"), None]

    assert _replace_nan_with_string(sorted_data["A"]) == _replace_nan_with_string(expected)


@pytest.mark.parametrize("desc", [True, False])
@pytest.mark.parametrize("n_partitions", [1, 3])
def test_single_string_col_sort(desc: bool, n_partitions: int):
    df = daft.DataFrame.from_pydict({"A": ["0", None, "1", "", "01"]})
    df = df.repartition(n_partitions)
    df = df.sort("A", desc=desc)
    sorted_data = df.to_pydict()

    expected = ["", "0", "01", "1"]
    if desc:
        expected = list(reversed(expected))
    expected = expected + [None]

    assert sorted_data["A"] == expected
