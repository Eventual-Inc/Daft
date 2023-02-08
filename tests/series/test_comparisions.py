from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from daft import Series

arrow_int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
arrow_string_types = [pa.string(), pa.large_string()]
arrow_float_types = [pa.float32(), pa.float64()]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, repeat=2))
def test_comparisons_int_and_str(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 3, 1, 5, None, None])
    # eq, lt, gt, None, None, None

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (l < r).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (l <= r).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (l == r).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (l != r).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (l >= r).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (l > r).to_pylist()
    assert gt == [False, False, True, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_comparisons_int_and_float(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 3, 1, 5, None, None])
    # eq, lt, gt, None, None, None

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (l < r).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (l <= r).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (l == r).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (l != r).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (l >= r).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (l > r).to_pylist()
    assert gt == [False, False, True, None, None, None]


def test_comparisons_bad_right_value() -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])

    l = Series.from_arrow(l_arrow)
    r = [1, 2, 3, None, 5, None]

    with pytest.raises(ValueError, match="another Series"):
        l < r

    with pytest.raises(ValueError, match="another Series"):
        l <= r

    with pytest.raises(ValueError, match="another Series"):
        l == r

    with pytest.raises(ValueError, match="another Series"):
        l != r

    with pytest.raises(ValueError, match="another Series"):
        l >= r

    with pytest.raises(ValueError, match="another Series"):
        l > r
