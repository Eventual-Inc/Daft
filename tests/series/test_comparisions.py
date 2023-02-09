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


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, repeat=2))
def test_comparisons_int_and_str_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([2])
    r_arrow = pa.array([1, 2, 3, None])
    # gt, eq, lt

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    lt = (l < r).to_pylist()
    assert lt == [False, False, True, None]

    le = (l <= r).to_pylist()
    assert le == [False, True, True, None]

    eq = (l == r).to_pylist()
    assert eq == [False, True, False, None]

    neq = (l != r).to_pylist()
    assert neq == [True, False, True, None]

    ge = (l >= r).to_pylist()
    assert ge == [True, True, False, None]

    gt = (l > r).to_pylist()
    assert gt == [True, False, False, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, repeat=2))
def test_comparisons_int_and_str_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([2])
    # lt, eq, gt, None, gt, None

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (l < r).to_pylist()
    assert lt == [True, False, False, None, False, None]

    le = (l <= r).to_pylist()
    assert le == [True, True, False, None, False, None]

    eq = (l == r).to_pylist()
    assert eq == [False, True, False, None, False, None]

    neq = (l != r).to_pylist()
    assert neq == [True, False, True, None, True, None]

    ge = (l >= r).to_pylist()
    assert ge == [False, True, True, None, True, None]

    gt = (l > r).to_pylist()
    assert gt == [False, False, True, None, True, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, repeat=2))
def test_comparisons_int_and_str_right_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=r_dtype)
    # lt, eq, gt, None, gt, None

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow)
    lt = (l < r).to_pylist()
    assert lt == [None, None, None, None, None, None]

    le = (l <= r).to_pylist()
    assert le == [None, None, None, None, None, None]

    eq = (l == r).to_pylist()
    assert eq == [None, None, None, None, None, None]

    neq = (l != r).to_pylist()
    assert neq == [None, None, None, None, None, None]

    ge = (l >= r).to_pylist()
    assert ge == [None, None, None, None, None, None]

    gt = (l > r).to_pylist()
    assert gt == [None, None, None, None, None, None]


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


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_comparisons_int_and_float_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([2])
    # lt, eq, gt, None, gt, None

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (l < r).to_pylist()
    assert lt == [True, False, False, None, False, None]

    le = (l <= r).to_pylist()
    assert le == [True, True, False, None, False, None]

    eq = (l == r).to_pylist()
    assert eq == [False, True, False, None, False, None]

    neq = (l != r).to_pylist()
    assert neq == [True, False, True, None, True, None]

    ge = (l >= r).to_pylist()
    assert ge == [False, True, True, None, True, None]

    gt = (l > r).to_pylist()
    assert gt == [False, False, True, None, True, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_comparisons_int_and_float_right_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=r_dtype)
    # lt, eq, gt, None, gt, None

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow)
    lt = (l < r).to_pylist()
    assert lt == [None, None, None, None, None, None]

    le = (l <= r).to_pylist()
    assert le == [None, None, None, None, None, None]

    eq = (l == r).to_pylist()
    assert eq == [None, None, None, None, None, None]

    neq = (l != r).to_pylist()
    assert neq == [None, None, None, None, None, None]

    ge = (l >= r).to_pylist()
    assert ge == [None, None, None, None, None, None]

    gt = (l > r).to_pylist()
    assert gt == [None, None, None, None, None, None]


def test_comparisons_boolean_array() -> None:
    l_arrow = pa.array([False, False, None, True, None])
    r_arrow = pa.array([True, False, True, None, None])
    # lt, eq, lt, None

    l = Series.from_arrow(l_arrow)
    r = Series.from_arrow(r_arrow)

    lt = (l < r).to_pylist()
    assert lt == [True, False, None, None, None]

    le = (l <= r).to_pylist()
    assert le == [True, True, None, None, None]

    eq = (l == r).to_pylist()
    assert eq == [False, True, None, None, None]

    neq = (l != r).to_pylist()
    assert neq == [True, False, None, None, None]

    ge = (l >= r).to_pylist()
    assert ge == [False, True, None, None, None]

    gt = (l > r).to_pylist()
    assert gt == [False, False, None, None, None]


def test_comparisons_boolean_array_right_scalar() -> None:
    l_arrow = pa.array([False, True, None])
    r_arrow = pa.array([True])
    # lt, eq, lt, None

    l = Series.from_arrow(l_arrow)
    r = Series.from_arrow(r_arrow)

    lt = (l < r).to_pylist()
    assert lt == [True, False, None]

    le = (l <= r).to_pylist()
    assert le == [True, True, None]

    eq = (l == r).to_pylist()
    assert eq == [False, True, None]

    neq = (l != r).to_pylist()
    assert neq == [True, False, None]

    ge = (l >= r).to_pylist()
    assert ge == [False, True, None]

    gt = (l > r).to_pylist()
    assert gt == [False, False, None]


def test_comparisons_boolean_array_right_scalar() -> None:
    l_arrow = pa.array([True])
    r_arrow = pa.array([False, True, None])
    # lt, eq, lt, None

    l = Series.from_arrow(l_arrow)
    r = Series.from_arrow(r_arrow)

    lt = (l < r).to_pylist()
    assert lt == [False, False, None]

    le = (l <= r).to_pylist()
    assert le == [False, True, None]

    eq = (l == r).to_pylist()
    assert eq == [False, True, None]

    neq = (l != r).to_pylist()
    assert neq == [True, False, None]

    ge = (l >= r).to_pylist()
    assert ge == [True, True, None]

    gt = (l > r).to_pylist()
    assert gt == [True, False, None]


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
