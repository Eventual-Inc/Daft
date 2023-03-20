from __future__ import annotations

import copy
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


@pytest.mark.parametrize("dtype", arrow_int_types + arrow_float_types + arrow_string_types)
def test_series_slice(dtype) -> None:
    data = pa.array([10, 20, 33, None, 50, None])

    s = Series.from_arrow(data.cast(dtype))

    result = s.slice(2, 4)
    assert result.datatype() == s.datatype()
    assert len(result) == 2

    original_data = s.to_pylist()
    expected = original_data[2:4]
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


def test_series_date_sorting() -> None:
    from datetime import date

    def date_maker(d):
        if d is None:
            return None
        return date(2023, 1, d)

    days = [5, 4, 1, None, 2, None]
    s = Series.from_pylist(list(map(date_maker, days)))
    sorted_order = list(map(date_maker, [1, 2, 4, 5, None, None]))
    s = s.cast(DataType.date())
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


def test_series_string_sorting() -> None:
    data = pa.array(["hi", "bye", "thai", None, "2", None, "h", "by"])
    sorted_order = ["2", "by", "bye", "h", "hi", "thai", None, None]
    s = Series.from_arrow(data)
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


def test_series_boolean_sorting() -> None:
    data = pa.array([True, False, True, None, False])
    sorted_order = [False, False, True, True, None]
    s = Series.from_arrow(data)
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


@pytest.mark.parametrize("dtype, size", itertools.product(arrow_int_types + arrow_float_types, [0, 1, 2, 8, 9, 16]))
def test_series_numeric_size_bytes(dtype, size) -> None:
    pydata = list(range(size))
    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    assert s.size_bytes() == data.nbytes


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_string_size_bytes(size) -> None:

    pydata = list(str(i) for i in range(size))
    data = pa.array(pydata, pa.large_string())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.string()
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.large_string())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.string()
    assert s.size_bytes() == data.nbytes


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_boolean_size_bytes(size) -> None:

    pydata = [True if i % 2 else False for i in range(size)]

    data = pa.array(pydata, pa.bool_())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.bool()
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.bool_())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.bool()
    assert s.size_bytes() == data.nbytes


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
def test_series_date_size_bytes(size) -> None:
    from datetime import date

    pydata = [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]
    data = pa.array(pydata)
    s = Series.from_arrow(data)

    assert s.datatype() == DataType.date()
    assert s.size_bytes() == data.nbytes

    ## with nulls
    if size > 0:
        pydata = pydata[:-1] + [None]
    data = pa.array(pydata, pa.date32())
    s = Series.from_arrow(data)

    assert s.datatype() == DataType.date()
    assert s.size_bytes() == data.nbytes


@pytest.mark.parametrize("dtype", arrow_int_types + arrow_float_types)
def test_series_numeric_abs(dtype) -> None:
    if pa.types.is_unsigned_integer(dtype):
        pydata = list(range(0, 10))
    else:
        pydata = list(range(-10, 10))

    data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)

    abs_s = abs(s)

    assert abs_s.datatype() == DataType.from_arrow_type(dtype)

    assert abs_s.to_pylist() == list(map(abs, pydata))


def test_table_abs_bad_input() -> None:
    series = Series.from_pylist(["a", "b", "c"])

    with pytest.raises(ValueError, match="abs not implemented"):
        abs(series)


@pytest.mark.parametrize(
    "dtype, chunks", itertools.product(arrow_float_types + arrow_int_types + arrow_string_types, [1, 2, 3, 10])
)
def test_series_concat(dtype, chunks) -> None:
    series = []
    for i in range(chunks):
        series.append(Series.from_pylist([i * j for j in range(i)]).cast(dtype=DataType.from_arrow_type(dtype)))

    concated = Series.concat(series)

    assert concated.datatype() == DataType.from_arrow_type(dtype)
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            val = i * j
            assert float(concated_list[counter]) == val
            counter += 1


def test_series_concat_bad_input() -> None:
    mix_types_series = [Series.from_pylist([1, 2, 3]), []]
    with pytest.raises(TypeError, match="Expected a Series for concat"):
        Series.concat(mix_types_series)

    with pytest.raises(ValueError, match="Need at least 1 series"):
        Series.concat([])


def test_series_concat_dtype_mismatch() -> None:
    mix_types_series = [Series.from_pylist([1, 2, 3]), Series.from_pylist([1.0, 2.0, 3.0])]

    with pytest.raises(ValueError, match="concat requires all data types to match"):
        Series.concat(mix_types_series)


def test_series_slice_bad_input() -> None:
    data = pa.array([10, 20, 33, None, 50, None])

    s = Series.from_arrow(data)

    with pytest.raises(ValueError, match="slice length can not be negative:"):
        s.slice(3, 2)

    with pytest.raises(ValueError, match="slice start can not be negative"):
        s.slice(-1, 2)

    with pytest.raises(ValueError, match="slice end can not be negative"):
        s.slice(0, -1)


@pytest.mark.parametrize("dtype", arrow_float_types + arrow_int_types + arrow_string_types)
def test_series_pickling(dtype) -> None:
    s = Series.from_pylist([1, 2, 3, None]).cast(DataType.from_arrow_type(dtype))
    copied_s = copy.deepcopy(s)
    assert s.datatype() == copied_s.datatype()
    assert s.to_pylist() == copied_s.to_pylist()
