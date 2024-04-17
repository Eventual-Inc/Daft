from __future__ import annotations

import itertools
import operator

import pyarrow as pa
import pytest

from daft import DataType, Series

arrow_int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
arrow_string_types = [pa.string(), pa.large_string()]
arrow_float_types = [pa.float32(), pa.float64()]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_array(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == [2, 6, 4, None, None, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = (left - right).to_pylist()
        assert sub == [0, -2, 2, None, None, None]

    mul = left * right
    assert mul.name() == left.name()
    assert mul.to_pylist() == [1, 8, 3, None, None, None]

    div = left / right
    assert div.name() == left.name()
    assert div.to_pylist() == [1.0, 0.5, 3.0, None, None, None]

    # mod = (l % r)
    # assert mod.name() == l.name()
    # assert mod.to_pylist() == [0, 2, 0, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == [2, 5, 2, 6, None, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = left - right
        assert sub.name() == left.name()
        assert sub.to_pylist() == [0, -3, 0, -4, None, None]

    mul = left * right
    assert mul.name() == left.name()
    assert mul.to_pylist() == [1, 4, 1, 5, None, None]

    div = left / right
    assert div.name() == left.name()
    assert div.to_pylist() == [1.0, 0.25, 1.0, 0.2, None, None]

    mod = left % right
    assert mod.name() == left.name()
    assert mod.to_pylist() == [0, 1, 0, 1, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1])

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == [2, 3, 4, None, 6, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = left - right
        assert sub.name() == left.name()
        assert sub.to_pylist() == [0, 1, 2, None, 4, None]

    mul = left * right
    assert mul.name() == left.name()
    assert mul.to_pylist() == [1, 2, 3, None, 5, None]

    div = left / right
    assert div.name() == left.name()
    assert div.to_pylist() == [1.0, 2.0, 3.0, None, 5.0, None]

    mod = left % right
    assert mod.name() == left.name()
    assert mod.to_pylist() == [0, 0, 0, None, 0, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=r_dtype)

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow, name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == [None, None, None, None, None, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = left - right
        assert sub.name() == left.name()
        assert sub.to_pylist() == [None, None, None, None, None, None]

    mul = left * right
    assert mul.name() == left.name()
    assert mul.to_pylist() == [None, None, None, None, None, None]

    div = left / right
    assert div.name() == left.name()
    assert div.to_pylist() == [None, None, None, None, None, None]

    mod = left % right
    assert mod.name() == left.name()
    assert mod.to_pylist() == [None, None, None, None, None, None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_array(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == ["11", "24", "31", None, None, None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == ["11", "14", "11", "15", None, None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1])

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == ["11", "21", "31", None, "51", None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=pa.string())

    left = Series.from_arrow(l_arrow.cast(l_dtype), name="left")
    right = Series.from_arrow(r_arrow.cast(r_dtype), name="right")

    add = left + right
    assert add.name() == left.name()
    assert add.to_pylist() == [None, None, None, None, None, None]


def test_comparisons_bad_right_value() -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])

    left = Series.from_arrow(l_arrow, name="left")
    right = [1, 2, 3, None, 5, None]

    with pytest.raises(TypeError, match="another Series"):
        left + right

    with pytest.raises(TypeError, match="another Series"):
        left - right

    with pytest.raises(TypeError, match="another Series"):
        left / right

    with pytest.raises(TypeError, match="another Series"):
        left * right

    with pytest.raises(TypeError, match="another Series"):
        left % right


def test_arithmetic_numbers_array_mismatch_length() -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None])

    left = Series.from_arrow(l_arrow, name="left")
    right = Series.from_arrow(r_arrow, name="right")

    with pytest.raises(ValueError, match="different lengths"):
        left + right

    with pytest.raises(ValueError, match="different lengths"):
        left - right

    with pytest.raises(ValueError, match="different lengths"):
        left * right

    with pytest.raises(ValueError, match="different lengths"):
        left / right

    with pytest.raises(ValueError, match="different lengths"):
        left % right


class FakeFive:
    def __add__(self, other):
        if isinstance(other, FakeFive):
            other = 5
        return 5 + other

    def __sub__(self, other):
        if isinstance(other, FakeFive):
            other = 5
        return 5 - other

    def __mul__(self, other):
        if isinstance(other, FakeFive):
            other = 5
        return 5 * other

    def __truediv__(self, other):
        if isinstance(other, FakeFive):
            other = 5
        return 5 / other

    def __mod__(self, other):
        if isinstance(other, FakeFive):
            other = 5
        return 5 % other


@pytest.mark.parametrize(
    ["op", "expected_datatype", "expected", "expected_self"],
    [
        (operator.add, DataType.int64(), [7, None, None], [10, 10, None]),
        (operator.sub, DataType.int64(), [3, None, None], [0, 0, None]),
        (operator.mul, DataType.int64(), [10, None, None], [25, 25, None]),
        (operator.truediv, DataType.float64(), [2.5, None, None], [1.0, 1.0, None]),
        (operator.mod, DataType.int64(), [1, None, None], [0, 0, None]),
    ],
)
def test_arithmetic_pyobjects(op, expected_datatype, expected, expected_self) -> None:
    fake_fives = Series.from_pylist([FakeFive(), FakeFive(), None])
    values = Series.from_pylist([2, None, None])

    assert op(fake_fives, values).datatype() == expected_datatype
    assert op(fake_fives, values).to_pylist() == expected
    assert op(fake_fives, fake_fives).to_pylist() == expected_self
