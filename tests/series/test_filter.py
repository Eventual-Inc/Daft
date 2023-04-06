from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES + ARROW_STRING_TYPES)
def test_series_filter(dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(dtype))
    pymask = [False, True, None, True, False, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()
    expected = [val for val, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected


def test_series_filter_on_null() -> None:
    s = Series.from_pylist([None, None, None, None, None, None])
    pymask = [False, True, None, True, False, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()

    expected = [None, None]
    assert result.to_pylist() == expected


def test_series_filter_on_bool() -> None:

    s = Series.from_pylist([True, False, None, True, None, False])
    pymask = [False, True, True, None, False, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()

    expected = [False, None]
    assert result.to_pylist() == expected


def test_series_filter_on_binary() -> None:

    s = Series.from_pylist([b"Y", b"N", None, b"Y", None, b"N"])
    pymask = [False, True, True, None, False, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()

    expected = [b"N", None]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES + ARROW_STRING_TYPES)
def test_series_filter_broadcast(dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(dtype))

    mask = Series.from_pylist([False])
    result = s.filter(mask)

    assert s.datatype() == result.datatype()

    assert result.to_pylist() == []

    mask = Series.from_pylist([None]).cast(DataType.bool())
    result = s.filter(mask)
    assert s.datatype() == result.datatype()
    assert result.to_pylist() == []

    mask = Series.from_pylist([True])
    result = s.filter(mask)

    assert s.datatype() == result.datatype()

    assert result.to_pylist() == s.to_pylist()


def test_series_filter_bad_input() -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data)

    mask = Series.from_pylist([False, False])
    with pytest.raises(ValueError, match="Lengths for filter do not match"):
        s.filter(mask).to_pylist()

    mask = Series.from_pylist([1])
    with pytest.raises(ValueError, match="We can only filter a Series with Boolean Series"):
        s.filter(mask).to_pylist()
