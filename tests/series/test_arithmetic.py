from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from daft import Series

arrow_int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
arrow_string_types = [pa.string(), pa.large_string()]
arrow_float_types = [pa.float32(), pa.float64()]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_array(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == [2, 6, 4, None, None, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = (l - r).to_pylist()
        assert sub == [0, -2, 2, None, None, None]

    mul = (l * r).to_pylist()
    assert mul == [1, 8, 3, None, None, None]

    div = (l / r).to_pylist()
    assert div == [1.0, 0.5, 3.0, None, None, None]

    # mod = (l % r).to_pylist()
    # assert mod == [0, 2, 0, None, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == [2, 5, 2, 6, None, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = (l - r).to_pylist()
        assert sub == [0, -3, 0, -4, None, None]

    mul = (l * r).to_pylist()
    assert mul == [1, 4, 1, 5, None, None]

    div = (l / r).to_pylist()
    assert div == [1.0, 0.25, 1.0, 0.2, None, None]

    # mod = (l % r).to_pylist()
    # assert mod == [0, 1, 0, 1, None, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == [2, 3, 4, None, 6, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = (l - r).to_pylist()
        assert sub == [0, 1, 2, None, 4, None]

    mul = (l * r).to_pylist()
    assert mul == [1, 2, 3, None, 5, None]

    div = (l / r).to_pylist()
    assert div == [1.0, 2.0, 3.0, None, 5.0, None]

    mod = (l % r).to_pylist()
    assert mod == [0, 0, 0, None, 0, None]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=r_dtype)

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow)

    add = (l + r).to_pylist()
    assert add == [None, None, None, None, None, None]

    if pa.types.is_signed_integer(l_dtype) or pa.types.is_signed_integer(r_dtype):
        sub = (l - r).to_pylist()
        assert sub == [None, None, None, None, None, None]

    mul = (l * r).to_pylist()
    assert mul == [None, None, None, None, None, None]

    div = (l / r).to_pylist()
    assert div == [None, None, None, None, None, None]

    mod = (l % r).to_pylist()
    assert mod == [None, None, None, None, None, None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_array(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == ["11", "24", "31", None, None, None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_left_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == ["11", "14", "11", "15", None, None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_right_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == ["11", "21", "31", None, "51", None]


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string_null_scalar(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([None], type=pa.string())

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == [None, None, None, None, None, None]


def test_arithmetic_numbers_array_mismatch_length() -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None])

    l = Series.from_arrow(l_arrow)
    r = Series.from_arrow(r_arrow)

    with pytest.raises(ValueError, match="different lengths"):
        l + r

    with pytest.raises(ValueError, match="different lengths"):
        l - r

    with pytest.raises(ValueError, match="different lengths"):
        l * r

    with pytest.raises(ValueError, match="different lengths"):
        l / r

    with pytest.raises(ValueError, match="different lengths"):
        l % r
