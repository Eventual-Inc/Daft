from __future__ import annotations

import itertools

import pyarrow as pa
import pytest

from daft import Series

arrow_int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
arrow_string_types = [pa.string(), pa.large_string()]
arrow_float_types = [pa.float32(), pa.float64()]


@pytest.mark.parametrize("l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_arithmetic_numbers(l_dtype, r_dtype) -> None:
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


@pytest.mark.parametrize(
    "l_dtype, r_dtype", itertools.product(arrow_int_types + arrow_string_types, arrow_string_types)
)
def test_add_for_int_and_string(l_dtype, r_dtype) -> None:
    l_arrow = pa.array([1, 2, 3, None, 5, None])
    r_arrow = pa.array([1, 4, 1, 5, None, None])

    l = Series.from_arrow(l_arrow.cast(l_dtype))
    r = Series.from_arrow(r_arrow.cast(r_dtype))

    add = (l + r).to_pylist()
    assert add == ["11", "24", "31", None, None, None]
