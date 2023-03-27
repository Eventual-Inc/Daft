from __future__ import annotations

import math

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES


@pytest.mark.parametrize("dtype", ARROW_FLOAT_TYPES)
def test_series_float_sorting(dtype) -> None:
    data = pa.array([5.0, 4.0, 1.0, None, 2.0, None, float("nan"), -float("nan"), float("inf"), -float("inf")])
    sorted_order = [-float("inf"), 1.0, 2.0, 4.0, 5.0, float("inf"), -float("nan"), float("nan"), None, None]
    s = Series.from_arrow(data.cast(dtype))
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert all([(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(s_sorted.to_pylist(), sorted_order)])

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert all([(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(taken.to_pylist(), sorted_order)])

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert all(
        [(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(s_sorted.to_pylist(), sorted_order[::-1])]
    )

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert all([(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(taken.to_pylist(), sorted_order[::-1])])


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES)
def test_series_int_sorting(dtype) -> None:
    data = pa.array([5, 4, 1, None, 2, None])
    sorted_order = [1, 2, 4, 5, None, None]
    s = Series.from_arrow(data.cast(dtype))
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order[::-1]

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order[::-1]


def test_series_date_sorting() -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    days = [5, 4, 1, None, 2, None]
    s = Series.from_pylist(list(map(date_maker, days)))
    sorted_order = list(map(date_maker, [1, 2, 4, 5, None, None]))
    s = s.cast(DataType.date())
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order[::-1]

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order[::-1]


def test_series_string_sorting() -> None:
    data = pa.array(["hi", "bye", "thai", None, "2", None, "h", "by"])
    sorted_order = ["2", "by", "bye", "h", "hi", "thai", None, None]
    s = Series.from_arrow(data)
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order[::-1]

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order[::-1]


def test_series_boolean_sorting() -> None:
    data = pa.array([True, False, True, None, False])
    sorted_order = [False, False, True, True, None]
    s = Series.from_arrow(data)
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order[::-1]

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order[::-1]
