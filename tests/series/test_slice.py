from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES + ARROW_STRING_TYPES)
def test_series_slice(dtype) -> None:
    data = pa.array([10, 20, 33, None, 50, None])

    s = Series.from_arrow(data.cast(dtype))

    result = s.slice(2, 4)
    assert result.datatype() == s.datatype()
    assert len(result) == 2

    original_data = s.to_pylist()
    expected = original_data[2:4]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("fixed", [False, True])
def test_series_slice_list_array(fixed) -> None:
    dtype = pa.list_(pa.int64(), list_size=2 if fixed else -1)
    data = pa.array([[10, 20], [33, None], [43, 45], None, [50, 52], None], type=dtype)

    s = Series.from_arrow(data)
    result = s.slice(2, 4)
    assert result.datatype() == s.datatype()
    assert len(result) == 2

    if not fixed:
        data = data.cast(pa.large_list(pa.int64()))
    assert result.to_arrow() == data.slice(2, 2)

    original_data = s.to_pylist()
    expected = original_data[2:4]
    assert result.to_pylist() == expected


def test_series_slice_list_array_to_fixed_size_with_nulls() -> None:
    data = [[10, 20], [33, None], [43, 45], None, [50, 52], None]

    s = Series.from_pylist(data)
    result = s.slice(1, 4).cast(DataType.fixed_size_list(DataType.int64(), 2))
    assert result.datatype() == DataType.fixed_size_list(DataType.int64(), 2)
    assert len(result) == 3

    expected = [[33, None], [43, 45], None]
    assert result.to_pylist() == expected


def test_series_slice_list_array_to_fixed_size_without_nulls() -> None:
    data = [[10, 20], [33, 34], [43, 45], [50, 52], [60, 62]]

    s = Series.from_pylist(data)
    result = s.slice(1, 4).cast(DataType.fixed_size_list(DataType.int64(), 2))
    assert result.datatype() == DataType.fixed_size_list(DataType.int64(), 2)
    assert len(result) == 3

    expected = [[33, 34], [43, 45], [50, 52]]
    assert result.to_pylist() == expected


def test_series_slice_struct_array() -> None:
    dtype = pa.struct({"a": pa.int64(), "b": pa.float64()})
    data = pa.array([{"a": 10, "b": 20}, {"a": 33}, {"a": 43, "b": 45}, None, {"a": 50, "b": 52}, None], type=dtype)

    s = Series.from_arrow(data)

    result = s.slice(2, 4)
    assert result.datatype() == s.datatype()
    assert len(result) == 2

    assert result.to_arrow() == data.slice(2, 2)

    original_data = s.to_pylist()
    expected = original_data[2:4]
    assert result.to_pylist() == expected


def test_series_slice_bad_input() -> None:
    data = pa.array([10, 20, 33, None, 50, None])

    s = Series.from_arrow(data)

    with pytest.raises(ValueError, match="slice length can not be negative:"):
        s.slice(3, 2)

    with pytest.raises(ValueError, match="slice start can not be negative"):
        s.slice(-1, 2)

    with pytest.raises(ValueError, match="slice end can not be negative"):
        s.slice(0, -1)
