from __future__ import annotations

import itertools
from collections import Counter

import pyarrow as pa
import pytest

from daft import DataType, Series

arrow_int_types = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
arrow_string_types = [pa.string(), pa.large_string()]
arrow_float_types = [pa.float32(), pa.float64()]


def test_series_arrow_array_round_trip() -> None:
    arrow = pa.array([1, 2, 3, None, 5, None])
    s = Series.from_arrow(arrow)
    back_to_arrow = s.to_arrow()
    assert arrow == back_to_arrow


def test_series_arrow_chunked_array_round_trip() -> None:
    arrow = pa.chunked_array([[1, 2, 3], [None, 5, None]])
    s = Series.from_arrow(arrow)
    back_to_arrow = s.to_arrow()
    assert arrow.combine_chunks() == back_to_arrow


def test_series_pylist_round_trip() -> None:
    data = [1, 2, 3, 4, None]
    s = Series.from_pylist(data)
    back_to_list = s.to_pylist()
    assert data == back_to_list


@pytest.mark.parametrize("dtype", arrow_int_types + arrow_float_types + arrow_string_types)
def test_series_pylist_round_trip(dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(dtype))

    words = Counter(repr(s).split())
    assert s.name() in words
    assert words[s.name()] == 1
    assert words["None"] == 2


@pytest.mark.parametrize("source_dtype, dest_dtype", itertools.product(arrow_int_types + arrow_float_types, repeat=2))
def test_series_casting_numeric(source_dtype, dest_dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(source_dtype))

    assert s.datatype() == DataType.from_arrow_type(source_dtype)
    target_dtype = DataType.from_arrow_type(dest_dtype)
    t = s.cast(target_dtype)
    assert t.datatype() == target_dtype
    assert t.to_pylist() == [1, 2, 3, None, 5, None]


@pytest.mark.parametrize(
    "source_dtype, dest_dtype", itertools.product(arrow_int_types + arrow_float_types, arrow_string_types)
)
def test_series_casting_to_string(source_dtype, dest_dtype) -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data.cast(source_dtype))

    assert s.datatype() == DataType.from_arrow_type(source_dtype)
    target_dtype = DataType.from_arrow_type(dest_dtype)
    t = s.cast(target_dtype)
    assert t.datatype() == target_dtype

    if pa.types.is_floating(source_dtype):
        assert t.to_pylist() == ["1.0", "2.0", "3.0", None, "5.0", None]
    else:
        assert t.to_pylist() == ["1", "2", "3", None, "5", None]
