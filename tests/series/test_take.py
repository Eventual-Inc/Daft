from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.conftest import get_tests_daft_runner_name
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


@pytest.mark.parametrize("time_unit", ["us", "ns"])
def test_series_time_take(time_unit) -> None:
    from datetime import time

    def time_maker(h, m, s, us):
        if us is None:
            return None
        return time(h, m, s, us)

    times = list(map(time_maker, [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [5, 4, 1, None, 2, None]))
    s = Series.from_pylist(times)
    s = s.cast(DataType.time(time_unit))
    taken = s.take(Series.from_pylist([5, 4, 3, 2, 1, 0]))
    assert taken.datatype() == DataType.time(time_unit)
    assert taken.to_pylist() == times[::-1]


@pytest.mark.parametrize("type", [pa.binary(), pa.binary(1)])
def test_series_binary_take(type) -> None:
    data = pa.array([b"1", b"2", b"3", None, b"5", None], type=type)

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


@pytest.mark.parametrize("dtype", [pa.int64(), pa.float16(), pa.float32()])
@pytest.mark.parametrize("dimension", [2, 768])
@pytest.mark.parametrize("slice_source", ["none", "arrow", "series"])
@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("null_indices", [False, True])
def test_series_fixed_size_list_take(dtype, dimension, slice_source, nullable, null_indices) -> None:
    pydata = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10], [11, 12]]
    pydata = [row * (dimension // 2) for row in pydata]
    if nullable:
        pydata[1][0] = None
        pydata[2] = None
        pydata[4][1] = None
    data = pa.array([[99] * dimension, *pydata, [99] * dimension], type=pa.list_(pa.int64(), dimension)).cast(
        pa.list_(dtype, dimension)
    )

    if slice_source == "series":
        s = Series.from_arrow(data, name="vectors").slice(1, 7)
    else:
        data = (
            data.slice(1, 6)
            if slice_source == "arrow"
            else pa.array(pydata, type=pa.list_(pa.int64(), dimension)).cast(pa.list_(dtype, dimension))
        )
        s = Series.from_arrow(data, name="vectors")
    pyidx = [5, 2, 1, None if null_indices else 3, 4, 0, 5]
    idx = Series.from_arrow(pa.array([99, *pyidx, 99], type=pa.uint64()).slice(1, len(pyidx)))

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert result.name() == s.name()
    assert len(result) == len(pyidx)

    expected = [pydata[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


@pytest.mark.parametrize(
    "child_dtype",
    [pa.float16(), pa.float32(), pa.list_(pa.int64()), pa.list_(pa.int64(), 2), pa.struct({"a": pa.int64()})],
)
@pytest.mark.parametrize("empty_source", [False, True])
@pytest.mark.parametrize("pyidx", [[], [None, None]])
@pytest.mark.parametrize("dimension", [2, 32])
def test_series_fixed_size_list_take_empty_or_null_indices(child_dtype, empty_source, pyidx, dimension) -> None:
    data = pa.array([] if empty_source else [[None] * dimension], type=pa.list_(child_dtype, dimension))
    s = Series.from_arrow(data, name="vectors")

    result = s.take(Series.from_arrow(pa.array(pyidx, type=pa.uint64())))

    assert result.datatype() == s.datatype()
    assert result.name() == s.name()
    assert result.to_pylist() == [None] * len(pyidx)


@pytest.mark.parametrize(
    "child_dtype, pydata",
    [
        (pa.list_(pa.int64()), [[[1, 2], None], None, [[], [3]], [[4], [5, None]]]),
        (pa.list_(pa.int64(), 2), [[[1, 2], None], None, [[3, None], [5, 6]], [[7, 8], [9, 10]]]),
        (
            pa.struct({"a": pa.int64()}),
            [[{"a": 1}, None], None, [{"a": None}, {"a": 2}], [{"a": 3}, {"a": 4}]],
        ),
    ],
)
@pytest.mark.parametrize("dimension", [2, 32])
def test_series_fixed_size_list_take_nested_child(child_dtype, pydata, dimension) -> None:
    pydata = [row * (dimension // 2) if row is not None else None for row in pydata]
    data = pa.array(pydata, type=pa.list_(child_dtype, dimension))
    s = Series.from_arrow(data, name="vectors").slice(1, 4)
    pyidx = [2, 0, None, 1, 2]

    result = s.take(Series.from_arrow(pa.array(pyidx, type=pa.uint64())))

    assert result.datatype() == s.datatype()
    assert result.name() == s.name()
    expected = [pydata[1:4][i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("dtype", [pa.float16(), pa.float32(), pa.null(), pa.bool_()])
@pytest.mark.parametrize("empty_source, pyidx", [(True, [0]), (False, [1]), (False, [None, 1]), (False, [2**64 - 1])])
@pytest.mark.parametrize("dimension", [2, 32])
def test_series_fixed_size_list_take_out_of_bounds(dtype, empty_source, pyidx, dimension) -> None:
    data = pa.array([] if empty_source else [[None] * dimension], type=pa.list_(dtype, dimension))
    s = Series.from_arrow(data)

    with pytest.raises(ValueError, match="out of bounds"):
        s.take(Series.from_arrow(pa.array(pyidx, type=pa.uint64())))


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


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="pyarrow extension types aren't supported on Ray clusters.",
)
def test_series_extension_type_take(uuid_ext_type) -> None:
    pydata = [f"{i}".encode() for i in range(6)]
    pydata[2] = None
    storage = pa.array(pydata)
    data = pa.ExtensionArray.from_storage(uuid_ext_type, storage)

    s = Series.from_arrow(data)
    assert s.datatype() == DataType.extension(
        uuid_ext_type.NAME, DataType.from_arrow_type(uuid_ext_type.storage_type), ""
    )
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    expected = [pydata[i] if i is not None else None for i in pyidx]
    assert result.to_pylist() == expected


def test_series_canonical_tensor_extension_type_take() -> None:
    pydata = np.arange(24).reshape((6, 4)).tolist()
    pydata[2] = None
    storage = pa.array(pydata, pa.list_(pa.int64(), 4))
    shape = (2, 2)
    tensor_type = pa.fixed_shape_tensor(pa.int64(), shape)
    data = pa.FixedShapeTensorArray.from_storage(tensor_type, storage)

    s = Series.from_arrow(data)
    assert s.datatype() == DataType.tensor(DataType.from_arrow_type(tensor_type.storage_type.value_type), shape)
    pyidx = [2, 0, None, 5]
    idx = Series.from_pylist(pyidx)

    result = s.take(idx)
    assert result.datatype() == s.datatype()
    assert len(result) == 4

    original_data = s.to_pylist()
    expected = [original_data[i] if i is not None else None for i in pyidx]
    np.testing.assert_equal(result.to_pylist(), expected)


def test_series_take_sparse_union() -> None:
    type_ids = pa.array([0, 1, 2, 0, 1, 2], type=pa.int8())
    int_child = pa.array([10, 0, 0, 40, 0, 0], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0, 0.0, 5.5, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c", "", "", "f"], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 'c', 40, 5.5, 'f']
    idx = Series.from_pylist([5, 2, 0, 4])

    result = s.take(idx)

    assert result.datatype() == s.datatype()
    assert result.to_pylist() == ["f", "c", 10, 5.5]


def test_series_take_dense_union() -> None:
    type_ids = pa.array([0, 1, 0, 0, 1], type=pa.int8())
    offsets = pa.array([0, 0, 1, 2, 1], type=pa.int32())
    int_child = pa.array([10, 30, 40], type=pa.int32())
    float_child = pa.array([2.2, 5.5], type=pa.float64())
    arrow_arr = pa.UnionArray.from_dense(type_ids, offsets, [int_child, float_child], field_names=["i", "f"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 30, 40, 5.5]
    idx = Series.from_pylist([4, 1, 0])

    result = s.take(idx)

    assert result.datatype() == s.datatype()
    assert result.to_pylist() == [5.5, 2.2, 10]


def test_series_take_sparse_union_null_indices() -> None:
    """None indices in take produce null slots in the result."""
    type_ids = pa.array([0, 1, 2, 0], type=pa.int8())
    int_child = pa.array([10, 0, 0, 40], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c", ""], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 'c', 40]
    idx = Series.from_pylist([0, None, 2, None, 3])

    result = s.take(idx)

    assert result.datatype() == s.datatype()
    assert result.to_pylist() == [10, None, "c", None, 40]


def test_series_take_dense_union_null_indices() -> None:
    """None indices in take produce null slots in the result."""
    type_ids = pa.array([0, 1, 0, 1], type=pa.int8())
    offsets = pa.array([0, 0, 1, 1], type=pa.int32())
    int_child = pa.array([10, 30], type=pa.int32())
    float_child = pa.array([2.2, 5.5], type=pa.float64())
    arrow_arr = pa.UnionArray.from_dense(type_ids, offsets, [int_child, float_child], field_names=["i", "f"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 30, 5.5]
    idx = Series.from_pylist([None, 0, None, 3])

    result = s.take(idx)

    assert result.datatype() == s.datatype()
    assert result.to_pylist() == [None, 10, None, 5.5]


def test_series_take_sparse_union_repeated_indices() -> None:
    """Repeated indices in take work for sparse unions."""
    type_ids = pa.array([0, 1, 2], type=pa.int8())
    int_child = pa.array([10, 0, 0], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c"], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 'c']
    idx = Series.from_pylist([0, 0, 2, 1, 2])

    result = s.take(idx)

    assert result.datatype() == s.datatype()
    assert result.to_pylist() == [10, 10, "c", 2.2, "c"]


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
