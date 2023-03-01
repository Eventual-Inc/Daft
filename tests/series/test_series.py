from __future__ import annotations

import itertools
import math
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


def test_series_pylist_round_trip_null() -> None:
    data = pa.array([None, None])

    s = Series.from_arrow(data)

    words = Counter(repr(s).split())
    assert s.name() in words
    assert words["None"] == 2


@pytest.mark.parametrize("dtype", arrow_int_types + arrow_float_types + arrow_string_types)
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


@pytest.mark.parametrize("dtype", arrow_int_types + arrow_float_types + arrow_string_types)
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


@pytest.mark.parametrize("dtype", arrow_int_types + arrow_float_types + arrow_string_types)
def test_series_take_numeric(dtype) -> None:
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


@pytest.mark.parametrize("dtype", arrow_float_types)
def test_series_float_sorting(dtype) -> None:
    data = pa.array([5.0, 4.0, 1.0, None, 2.0, None, float("nan"), -float("nan"), float("inf"), -float("inf")])
    sorted_order = [-float("inf"), 1.0, 2.0, 4.0, 5.0, float("inf"), -float("nan"), float("nan"), None, None]
    s = Series.from_arrow(data.cast(dtype))
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert all([(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(s_sorted.to_pylist(), sorted_order)])

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert all([(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(taken.to_pylist(), sorted_order)])

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert all(
        [(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(s_sorted.to_pylist(), sorted_order[::-1])]
    )

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert all([(l == r) or (math.isnan(l) and math.isnan(r)) for l, r in zip(taken.to_pylist(), sorted_order[::-1])])


@pytest.mark.parametrize("dtype", arrow_int_types)
def test_series_int_sorting(dtype) -> None:
    data = pa.array([5, 4, 1, None, 2, None])
    sorted_order = [1, 2, 4, 5, None, None]
    s = Series.from_arrow(data.cast(dtype))
    s_sorted = s.sort()
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order

    s_argsorted = s.argsort()
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order

    ## Descending
    s_sorted = s.sort(descending=True)
    assert len(s_sorted) == len(s)
    assert s_sorted.datatype() == s.datatype()
    assert s_sorted.to_pylist() == sorted_order[::-1]

    s_argsorted = s.argsort(descending=True)
    assert len(s_argsorted) == len(s)

    taken = s.take(s_argsorted)
    assert len(taken) == len(s)
    assert taken.to_pylist() == sorted_order[::-1]
