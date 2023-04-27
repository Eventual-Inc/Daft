from __future__ import annotations

import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES + ARROW_STRING_TYPES)
def test_series_take(dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(dtype))
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    original_data = s.to_pylist()
    expected = [original_data[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


def test_series_date_take() -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    days = list(map(date_maker, [5, 4, 1, None, 2, None]))
    s = Series.from_pylist(days)
    taken = s.take(Series.from_pylist([5, 4, 3, 2, 1, 0]))
    assert taken.datatype() == DataType.date()
    assert taken.to_pylist() == days[::-1]


def test_series_binary_take() -> None:
    data = pa.array([b"1", b"2", b"3", None, b"5", None])

    s = Series.from_arrow(data)
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    original_data = s.to_pylist()
    expected = [original_data[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


def test_series_list_take() -> None:
    data = pa.array([[1, 2], [None], None, [7, 8, 9], [10, None], [11]], type=pa.list_(pa.int64()))

    s = Series.from_arrow(data)
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    original_data = s.to_pylist()
    expected = [original_data[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


def test_series_fixed_size_list_take() -> None:
    data = pa.array([[1, 2], [None, 4], None, [7, 8], [9, None], [11, 12]], type=pa.list_(pa.int64(), 2))

    s = Series.from_arrow(data)
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    original_data = s.to_pylist()
    expected = [original_data[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


def test_series_struct_take() -> None:
    dtype = pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()})
    data = pa.array(
        [
            {"a": 1, "b": 2},
            {"b": 3, "c": "4"},
            None,
            {"a": 5, "b": 6, "c": "7"},
            {"a": 8, "b": None, "c": "10"},
            {"b": 11, "c": None},
        ],
        type=dtype,
    )

    s = Series.from_arrow(data)
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    original_data = s.to_pylist()
    expected = [original_data[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


def test_series_deeply_nested_take() -> None:
    # Test take on a Series with a deeply nested type: struct of list of struct of list of strings.
    data = pa.array([{"a": [{"b": ["foo", "bar"]}]}, {"a": [{"b": ["baz", "quux"]}]}])
    dtype = pa.struct([("a", pa.large_list(pa.struct([("b", pa.large_list(pa.large_string()))])))])

    s = Series.from_arrow(data)
    assert s.datatype() == DataType.from_arrow_type(dtype)
    idx = Series.from_pylist([1])

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 1

    original_data = s.to_pylist()
    expected = [original_data[1]]
    assert result.to_pylist() == expected
