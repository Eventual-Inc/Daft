from __future__ import annotations

import itertools
import operator

import pyarrow as pa
import pytest

from daft import DataType, Series

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

    _and = (l & r).to_pylist()
    assert _and == [False, False, None, None, None]

    _or = (l | r).to_pylist()
    assert _or == [True, False, None, None, None]

    _xor = (l ^ r).to_pylist()
    assert _xor == [True, False, None, None, None]


def test_comparisons_boolean_array_right_scalar() -> None:
    l_arrow = pa.array([False, True, None])
    r_arrow = pa.array([True])

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

    _and = (l & r).to_pylist()
    assert _and == [False, True, None]

    _or = (l | r).to_pylist()
    assert _or == [True, True, None]

    _xor = (l ^ r).to_pylist()
    assert _xor == [True, False, None]

    r_arrow = pa.array([False])
    r = Series.from_arrow(r_arrow)

    lt = (l < r).to_pylist()
    assert lt == [False, False, None]

    le = (l <= r).to_pylist()
    assert le == [True, False, None]

    eq = (l == r).to_pylist()
    assert eq == [True, False, None]

    neq = (l != r).to_pylist()
    assert neq == [False, True, None]

    ge = (l >= r).to_pylist()
    assert ge == [True, True, None]

    gt = (l > r).to_pylist()
    assert gt == [False, True, None]

    _and = (l & r).to_pylist()
    assert _and == [False, False, None]

    _or = (l | r).to_pylist()
    assert _or == [False, True, None]

    _xor = (l ^ r).to_pylist()
    assert _xor == [False, True, None]

    r_arrow = pa.array([None], type=pa.bool_())
    r = Series.from_arrow(r_arrow)

    lt = (l < r).to_pylist()
    assert lt == [None, None, None]

    le = (l <= r).to_pylist()
    assert le == [None, None, None]

    eq = (l == r).to_pylist()
    assert eq == [None, None, None]

    neq = (l != r).to_pylist()
    assert neq == [None, None, None]

    ge = (l >= r).to_pylist()
    assert ge == [None, None, None]

    gt = (l > r).to_pylist()
    assert gt == [None, None, None]

    _and = (l & r).to_pylist()
    assert _and == [None, None, None]

    _or = (l | r).to_pylist()
    assert _or == [None, None, None]

    _xor = (l ^ r).to_pylist()
    assert _xor == [None, None, None]


def test_comparisons_boolean_array_left_scalar() -> None:
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

    _and = (l & r).to_pylist()
    assert _and == [False, True, None]

    _or = (l | r).to_pylist()
    assert _or == [True, True, None]

    _xor = (l ^ r).to_pylist()
    assert _xor == [True, False, None]


def test_comparisons_bad_right_value() -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])

    l = Series.from_arrow(l_arrow)
    r = [1, 2, 3, None, 5, None]

    with pytest.raises(TypeError, match="another Series"):
        l < r

    with pytest.raises(TypeError, match="another Series"):
        l <= r

    with pytest.raises(TypeError, match="another Series"):
        l == r

    with pytest.raises(TypeError, match="another Series"):
        l != r

    with pytest.raises(TypeError, match="another Series"):
        l >= r

    with pytest.raises(TypeError, match="another Series"):
        l > r

    with pytest.raises(TypeError, match="another Series"):
        l & r

    with pytest.raises(TypeError, match="another Series"):
        l | r

    with pytest.raises(TypeError, match="another Series"):
        l ^ r


def test_boolean_array_mismatch_length() -> None:
    l_arrow = pa.array([False, True, None, None])
    r_arrow = pa.array([False, True, False, True, None])

    l = Series.from_arrow(l_arrow)
    r = Series.from_arrow(r_arrow)

    with pytest.raises(ValueError, match="different length"):
        l < r

    with pytest.raises(ValueError, match="different length"):
        l <= r

    with pytest.raises(ValueError, match="different length"):
        l == r

    with pytest.raises(ValueError, match="different length"):
        l != r

    with pytest.raises(ValueError, match="different length"):
        l > r

    with pytest.raises(ValueError, match="different length"):
        l >= r

    with pytest.raises(ValueError, match="different length"):
        l & r

    with pytest.raises(ValueError, match="different length"):
        l | r

    with pytest.raises(ValueError, match="different length"):
        l ^ r


def test_logical_ops_with_non_boolean() -> None:
    l_arrow = pa.array([False, True, None, None])
    r_arrow = pa.array([1, 2, 3, 4])

    l = Series.from_arrow(l_arrow)
    r = Series.from_arrow(r_arrow)

    with pytest.raises(ValueError, match="logic"):
        l & r

    with pytest.raises(ValueError, match="logic"):
        l | r

    with pytest.raises(ValueError, match="logic"):
        l ^ r

    with pytest.raises(ValueError, match="logic"):
        r & l

    with pytest.raises(ValueError, match="logic"):
        r | l

    with pytest.raises(ValueError, match="logic"):
        r ^ l


def test_comparisons_dates() -> None:

    from datetime import date

    l = Series.from_pylist([date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3), None, date(2023, 1, 5), None])
    r = Series.from_pylist([date(2023, 1, 1), date(2023, 1, 3), date(2023, 1, 1), date(2023, 1, 5), None, None])

    # eq, lt, gt, None, None, None

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


class CustomZero:
    def __eq__(self, other):
        if isinstance(other, CustomZero):
            other = 0
        return 0 == other

    def __lt__(self, other):
        if isinstance(other, CustomZero):
            other = 0
        return 0 < other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other):
        return not self.__le__(other)

    def __ge__(self, other):
        return not self.__lt__(other)


@pytest.mark.parametrize(
    ["op", "reflected_op", "expected", "expected_self"],
    [
        (operator.eq, operator.eq, [False, True, False, None, None], [True, True, True, True, None]),
        (operator.ne, operator.ne, [True, False, True, None, None], [False, False, False, False, None]),
        (operator.lt, operator.gt, [False, False, True, None, None], [False, False, False, False, None]),
        (operator.gt, operator.lt, [True, False, False, None, None], [False, False, False, False, None]),
        (operator.le, operator.ge, [False, True, True, None, None], [True, True, True, True, None]),
        (operator.ge, operator.le, [True, True, False, None, None], [True, True, True, True, None]),
    ],
)
def test_comparisons_pyobjects(op, reflected_op, expected, expected_self) -> None:

    custom_zeros = Series.from_pylist([CustomZero(), CustomZero(), CustomZero(), CustomZero(), None])
    values = Series.from_pylist([-1, 0, 1, None, None])

    assert op(custom_zeros, values).datatype() == DataType.bool()
    assert op(custom_zeros, values).to_pylist() == expected
    assert op(custom_zeros, values).to_pylist() == reflected_op(values, custom_zeros).to_pylist()
    assert op(custom_zeros, custom_zeros).to_pylist() == expected_self


class CustomFalse:
    def __and__(self, other):
        if isinstance(other, CustomFalse):
            other = False
        return False & other

    def __or__(self, other):
        if isinstance(other, CustomFalse):
            other = False
        return False or other

    def __xor__(self, other):
        if isinstance(other, CustomFalse):
            other = False
        return False ^ other


@pytest.mark.parametrize(
    ["op", "expected", "expected_self"],
    [
        (operator.and_, [False, False, None, None], [False, False, False, None]),
        (operator.or_, [False, True, None, None], [False, False, False, None]),
        (operator.xor, [False, True, None, None], [False, False, False, None]),
    ],
)
def test_logicalops_pyobjects(op, expected, expected_self) -> None:
    custom_falses = Series.from_pylist([CustomFalse(), CustomFalse(), CustomFalse(), None])
    values = Series.from_pylist([False, True, None, None])

    # (Symmetry is not tested here since Python logicalops are not automatically symmetric.)
    assert op(custom_falses, values).datatype() == DataType.bool()
    assert op(custom_falses, values).to_pylist() == expected
    assert op(custom_falses, custom_falses).to_pylist() == expected_self
