from __future__ import annotations

import itertools
import operator
from datetime import date, datetime

import pyarrow as pa
import pytest
import pytz

from daft import DataType, Series

arrow_int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
arrow_string_types = [pa.string(), pa.large_string()]
arrow_float_types = [pa.float32(), pa.float64()]
arrow_binary_types = [pa.binary(), pa.large_binary()]


VALID_INT_STRING_COMPARISONS = list(itertools.product(arrow_int_types, repeat=2)) + list(
    itertools.product(arrow_string_types, repeat=2)
)


@pytest.mark.parametrize("l_dtype, r_dtype", VALID_INT_STRING_COMPARISONS)
def test_comparisons_int_and_str(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 3, 1, 5, None, None])
    # eq, lt, gt, None, None, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (left < right).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", VALID_INT_STRING_COMPARISONS)
def test_comparisons_int_and_str_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([2])
    r_arrow = pa.array([1, 2, 3, None])
    # gt, eq, lt

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))

    lt = (left < right).to_pylist()
    assert lt == [False, False, True, None]

    le = (left <= right).to_pylist()
    assert le == [False, True, True, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, True, False, None]

    gt = (left > right).to_pylist()
    assert gt == [True, False, False, None]


@pytest.mark.parametrize("l_dtype, r_dtype", VALID_INT_STRING_COMPARISONS)
def test_comparisons_int_and_str_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([2])
    # lt, eq, gt, None, gt, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (left < right).to_pylist()
    assert lt == [True, False, False, None, False, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, False, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [False, True, True, None, True, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, True, None]


@pytest.mark.parametrize("l_dtype, r_dtype", VALID_INT_STRING_COMPARISONS)
def test_comparisons_int_and_str_right_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=r_dtype)
    # lt, eq, gt, None, gt, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow)
    lt = (left < right).to_pylist()
    assert lt == [None, None, None, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [None, None, None, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [None, None, None, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [None, None, None, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [None, None, None, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [None, None, None, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_comparisons_int_and_float(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 3, 1, 5, None, None])
    # eq, lt, gt, None, None, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (left < right).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_comparisons_int_and_float_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([2])
    # lt, eq, gt, None, gt, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (left < right).to_pylist()
    assert lt == [True, False, False, None, False, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, False, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [False, True, True, None, True, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, True, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_comparisons_int_and_float_right_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=r_dtype)
    # lt, eq, gt, None, gt, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow)
    lt = (left < right).to_pylist()
    assert lt == [None, None, None, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [None, None, None, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [None, None, None, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [None, None, None, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [None, None, None, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [None, None, None, None, None, None]


def test_comparisons_boolean_array() -> None:
    l_arrow = pa.array([False, False, None, True, None])
    r_arrow = pa.array([True, False, True, None, None])
    # lt, eq, lt, None

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)

    lt = (left < right).to_pylist()
    assert lt == [True, False, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [False, True, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, None, None, None]

    _and = (left & right).to_pylist()
    assert _and == [False, False, None, None, None]

    _or = (left | right).to_pylist()
    assert _or == [True, False, None, None, None]

    _xor = (left ^ right).to_pylist()
    assert _xor == [True, False, None, None, None]


def test_comparisons_boolean_array_right_scalar() -> None:
    l_arrow = pa.array([False, True, None])
    r_arrow = pa.array([True])

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)

    lt = (left < right).to_pylist()
    assert lt == [True, False, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, None]

    ge = (left >= right).to_pylist()
    assert ge == [False, True, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, None]

    _and = (left & right).to_pylist()
    assert _and == [False, True, None]

    _or = (left | right).to_pylist()
    assert _or == [True, True, None]

    _xor = (left ^ right).to_pylist()
    assert _xor == [True, False, None]

    r_arrow = pa.array([False])
    right = Series.from_arrow(r_arrow)

    lt = (left < right).to_pylist()
    assert lt == [False, False, None]

    le = (left <= right).to_pylist()
    assert le == [True, False, None]

    eq = (left == right).to_pylist()
    assert eq == [True, False, None]

    neq = (left != right).to_pylist()
    assert neq == [False, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, True, None]

    gt = (left > right).to_pylist()
    assert gt == [False, True, None]

    _and = (left & right).to_pylist()
    assert _and == [False, False, None]

    _or = (left | right).to_pylist()
    assert _or == [False, True, None]

    _xor = (left ^ right).to_pylist()
    assert _xor == [False, True, None]

    r_arrow = pa.array([None], type=pa.bool_())
    right = Series.from_arrow(r_arrow)

    lt = (left < right).to_pylist()
    assert lt == [None, None, None]

    le = (left <= right).to_pylist()
    assert le == [None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [None, None, None]

    _and = (left & right).to_pylist()
    assert _and == [None, None, None]

    _or = (left | right).to_pylist()
    assert _or == [None, None, None]

    _xor = (left ^ right).to_pylist()
    assert _xor == [None, None, None]


def test_comparisons_boolean_array_left_scalar() -> None:
    l_arrow = pa.array([True])
    r_arrow = pa.array([False, True, None])
    # lt, eq, lt, None

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)

    lt = (left < right).to_pylist()
    assert lt == [False, False, None]

    le = (left <= right).to_pylist()
    assert le == [False, True, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, True, None]

    gt = (left > right).to_pylist()
    assert gt == [True, False, None]

    _and = (left & right).to_pylist()
    assert _and == [False, True, None]

    _oright = (left | right).to_pylist()
    assert _oright == [True, True, None]

    _xoright = (left ^ right).to_pylist()
    assert _xoright == [True, False, None]


def test_comparisons_bad_right_value() -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])

    left = Series.from_arrow(l_arrow)
    right = [1, 2, 3, None, 5, None]

    with pytest.raises(TypeError, match="another Series"):
        left < right

    with pytest.raises(TypeError, match="another Series"):
        left <= right

    with pytest.raises(TypeError, match="another Series"):
        left == right

    with pytest.raises(TypeError, match="another Series"):
        left != right

    with pytest.raises(TypeError, match="another Series"):
        left >= right

    with pytest.raises(TypeError, match="another Series"):
        left > right

    with pytest.raises(TypeError, match="another Series"):
        left & right

    with pytest.raises(TypeError, match="another Series"):
        left | right

    with pytest.raises(TypeError, match="another Series"):
        left ^ right


def test_boolean_array_mismatch_length() -> None:
    l_arrow = pa.array([False, True, None, None])
    r_arrow = pa.array([False, True, False, True, None])

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)

    with pytest.raises(ValueError, match="different length"):
        left < right

    with pytest.raises(ValueError, match="different length"):
        left <= right

    with pytest.raises(ValueError, match="different length"):
        left == right

    with pytest.raises(ValueError, match="different length"):
        left != right

    with pytest.raises(ValueError, match="different length"):
        left > right

    with pytest.raises(ValueError, match="different length"):
        left >= right

    with pytest.raises(ValueError, match="different length"):
        left & right

    with pytest.raises(ValueError, match="different length"):
        left | right

    with pytest.raises(ValueError, match="different length"):
        left ^ right


def test_logical_ops_with_non_boolean() -> None:
    l_arrow = pa.array([False, True, None, None])
    r_arrow = pa.array([1, 2, 3, 4])

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)

    with pytest.raises(ValueError, match="logic"):
        left & right

    with pytest.raises(ValueError, match="logic"):
        left | right

    with pytest.raises(ValueError, match="logic"):
        left ^ right

    with pytest.raises(ValueError, match="logic"):
        right & left

    with pytest.raises(ValueError, match="logic"):
        right | left

    with pytest.raises(ValueError, match="logic"):
        right ^ left


def test_comparisons_dates() -> None:
    from datetime import date

    left = Series.from_pylist([date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3), None, date(2023, 1, 5), None])
    right = Series.from_pylist([date(2023, 1, 1), date(2023, 1, 3), date(2023, 1, 1), date(2023, 1, 5), None, None])

    # eq, lt, gt, None, None, None

    lt = (left < right).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_binary_types, repeat=2))
def test_comparisons_binary(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([b"1", b"22", b"333", None, b"55555", None])
    r_arrow = pa.array([b"1", b"333", b"1", b"55555", None, None])
    # eq, lt, gt, None, None, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (left < right).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_binary_types, repeat=2))
def test_comparisons_binary_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([b"22"])
    r_arrow = pa.array([b"1", b"22", b"333", None])
    # gt, eq, lt

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))

    lt = (left < right).to_pylist()
    assert lt == [False, False, True, None]

    le = (left <= right).to_pylist()
    assert le == [False, True, True, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, True, False, None]

    gt = (left > right).to_pylist()
    assert gt == [True, False, False, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_binary_types, repeat=2))
def test_comparisons_binary_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([b"1", b"22", b"333", None, b"55555", None])
    r_arrow = pa.array([b"22"])
    # lt, eq, gt, None, gt, None

    left = Series.from_arrow(l_arrow.cast(l_dtype))
    right = Series.from_arrow(r_arrow.cast(r_dtype))
    lt = (left < right).to_pylist()
    assert lt == [True, False, False, None, False, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, False, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [False, True, True, None, True, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, True, None]


def test_comparisons_fixed_size_binary() -> None:
    l_arrow = pa.array([b"11111", b"22222", b"33333", None, b"12345", None], type=pa.binary(5))
    r_arrow = pa.array([b"11111", b"33333", b"11111", b"12345", None, None], type=pa.binary(5))
    # eq, lt, gt, None, None, None

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)
    lt = (left < right).to_pylist()
    assert lt == [False, True, False, None, None, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, None, None]

    eq = (left == right).to_pylist()
    assert eq == [True, False, False, None, None, None]

    neq = (left != right).to_pylist()
    assert neq == [False, True, True, None, None, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, False, True, None, None, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, None, None]


def test_comparisons_fixed_size_binary_left_scalar() -> None:
    l_arrow = pa.array([b"222"], type=pa.binary(3))
    r_arrow = pa.array([b"111", b"222", b"333", None], type=pa.binary(3))
    # gt, eq, lt

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)

    lt = (left < right).to_pylist()
    assert lt == [False, False, True, None]

    le = (left <= right).to_pylist()
    assert le == [False, True, True, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [True, True, False, None]

    gt = (left > right).to_pylist()
    assert gt == [True, False, False, None]


def test_comparisons_fixed_size_binary_right_scalar() -> None:
    l_arrow = pa.array([b"111", b"222", b"333", None, b"555", None], type=pa.binary(3))
    r_arrow = pa.array([b"222"], type=pa.binary(3))
    # lt, eq, gt, None, gt, None

    left = Series.from_arrow(l_arrow)
    right = Series.from_arrow(r_arrow)
    lt = (left < right).to_pylist()
    assert lt == [True, False, False, None, False, None]

    le = (left <= right).to_pylist()
    assert le == [True, True, False, None, False, None]

    eq = (left == right).to_pylist()
    assert eq == [False, True, False, None, False, None]

    neq = (left != right).to_pylist()
    assert neq == [True, False, True, None, True, None]

    ge = (left >= right).to_pylist()
    assert ge == [False, True, True, None, True, None]

    gt = (left > right).to_pylist()
    assert gt == [False, False, True, None, True, None]


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


@pytest.mark.parametrize("tu1, tu2", itertools.product(["ns", "us", "ms"], repeat=2))
def test_compare_timestamps_no_tz(tu1, tu2):
    tz1 = Series.from_pylist([datetime(2022, 1, 1)])
    assert (tz1.cast(DataType.timestamp(tu1)) == tz1.cast(DataType.timestamp(tu2))).to_pylist() == [True]


def test_compare_timestamps_no_tz_date():
    tz1 = Series.from_pylist([datetime(2022, 1, 1)])
    Series.from_pylist([date(2022, 1, 1)])
    assert (tz1 == tz1).to_pylist() == [True]


def test_compare_timestamps_one_tz():
    tz1 = Series.from_pylist([datetime(2022, 1, 1)])
    tz2 = Series.from_pylist([datetime(2022, 1, 1, tzinfo=pytz.utc)])
    with pytest.raises(ValueError, match="Cannot perform comparison on types"):
        assert (tz1 == tz2).to_pylist() == [True]


def test_compare_timestamps_and_int():
    tz1 = Series.from_pylist([datetime(2022, 1, 1)])
    tz2 = Series.from_pylist([5])
    with pytest.raises(ValueError, match="Cannot perform comparison on types"):
        assert (tz1 == tz2).to_pylist() == [True]


def test_compare_timestamps_tz_date():
    tz1 = Series.from_pylist([datetime(2022, 1, 1, tzinfo=pytz.utc)])
    tz2 = Series.from_pylist([date(2022, 1, 1)])
    assert (tz1 == tz2).to_pylist() == [True]


def test_compare_lt_timestamps_tz_date():
    tz1 = Series.from_pylist([datetime(2022, 1, 1, tzinfo=pytz.utc)])
    tz2 = Series.from_pylist([date(2022, 1, 6)])
    assert (tz1 < tz2).to_pylist() == [True]


def test_compare_lt_timestamps_tz_date_same():
    tz1 = Series.from_pylist([datetime(2022, 1, 6, tzinfo=pytz.utc)])
    tz2 = Series.from_pylist([date(2022, 1, 6)])
    assert (tz1 < tz2).to_pylist() == [False]


@pytest.mark.parametrize("tu1, tu2", itertools.product(["ns", "us", "ms"], repeat=2))
def test_compare_timestamps_same_tz(tu1, tu2):
    tz1 = Series.from_pylist([datetime(2022, 1, 1, tzinfo=pytz.utc)]).cast(DataType.timestamp(tu1, "UTC"))
    tz2 = Series.from_pylist([datetime(2022, 1, 1, tzinfo=pytz.utc)]).cast(DataType.timestamp(tu2, "UTC"))
    assert (tz1 == tz2).to_pylist() == [True]


@pytest.mark.parametrize("tu1, tu2", itertools.product(["ns", "us", "ms"], repeat=2))
def test_compare_timestamps_diff_tz(tu1, tu2):
    utc = datetime(2022, 1, 1, tzinfo=pytz.utc)
    eastern = utc.astimezone(pytz.timezone("US/Eastern"))
    tz1 = Series.from_pylist([utc]).cast(DataType.timestamp(tu1, "UTC"))
    tz2 = Series.from_pylist([eastern]).cast(DataType.timestamp(tu1, "US/Eastern"))
    assert (tz1 == tz2).to_pylist() == [True]


@pytest.mark.parametrize("op", [operator.eq, operator.ne, operator.lt, operator.gt, operator.le, operator.ge])
def test_numeric_and_string_compare_raises_error(op):
    left = Series.from_pylist([1, 2, 3])
    right = Series.from_pylist(["1", "2", "3"])
    with pytest.raises(ValueError, match="Cannot perform comparison on types:"):
        op(left, right)

    with pytest.raises(ValueError, match="Cannot perform comparison on types:"):
        op(right, left)
