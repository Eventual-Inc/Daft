from __future__ import annotations

import numpy as np
import pyarrow as pa

from daft import Series


def test_float_is_nan() -> None:
    s = Series.from_arrow(pa.array([1.0, np.nan, 3.0, float("nan")]))
    result = s.float.is_nan()
    assert result.to_pylist() == [False, True, False, True]


def test_float_is_nan_with_nulls() -> None:
    s = Series.from_arrow(pa.array([1.0, None, np.nan, 3.0, None, float("nan")]))
    result = s.float.is_nan()
    assert result.to_pylist() == [False, None, True, False, None, True]


def test_float_is_nan_empty() -> None:
    s = Series.from_arrow(pa.array([], type=pa.float64()))
    result = s.float.is_nan()
    assert result.to_pylist() == []


def test_float_is_nan_all_null() -> None:
    s = Series.from_arrow(pa.array([None, None, None]))
    result = s.float.is_nan()
    assert result.to_pylist() == [None, None, None]


def test_float_is_inf() -> None:
    s = Series.from_arrow(pa.array([-float("inf"), 0.0, np.inf]))
    result = s.float.is_inf()
    assert result.to_pylist() == [True, False, True]


def test_float_is_inf_with_nulls() -> None:
    s = Series.from_arrow(pa.array([-np.inf, None, 1.0, None, float("inf")]))
    result = s.float.is_inf()
    assert result.to_pylist() == [True, None, False, None, True]


def test_float_is_inf_empty() -> None:
    s = Series.from_arrow(pa.array([], type=pa.float64()))
    result = s.float.is_inf()
    assert result.to_pylist() == []


def test_float_is_inf_all_null() -> None:
    s = Series.from_arrow(pa.array([None, None, None]))
    result = s.float.is_inf()
    assert result.to_pylist() == [None, None, None]
