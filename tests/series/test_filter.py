from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.conftest import get_tests_daft_runner_name
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


@pytest.mark.parametrize("type", [pa.binary(), pa.binary(1)])
def test_series_filter_on_binary(type) -> None:
    s = Series.from_arrow(pa.array([b"Y", b"N", None, b"Y", None, b"N"], type=type))
    pymask = [False, True, True, None, False, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()

    expected = [b"N", None]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_series_filter_on_list_array(dtype) -> None:
    data = pa.array([[1], [2, None], None, [5, 6, 7], [8]], type=pa.list_(dtype))

    s = Series.from_arrow(data)
    pymask = [False, True, True, None, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()
    expected = [val for val, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_series_filter_on_fixed_size_list_array(dtype) -> None:
    data = pa.array([[1, 2], [3, None], None, [8, 9], [None, 11]], type=pa.list_(dtype, 2))

    s = Series.from_arrow(data)
    pymask = [False, True, True, None, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()
    expected = [val for val, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected


def test_series_filter_on_struct_array() -> None:
    dtype = pa.struct({"a": pa.int64(), "b": pa.float64(), "c": pa.string()})
    data = pa.array(
        [{"a": 1, "b": 2}, {"b": 3, "c": "4"}, None, {"a": 5, "b": 6, "c": "7"}, {"a": 8, "b": None, "c": "10"}],
        type=dtype,
    )

    s = Series.from_arrow(data.cast(dtype))
    pymask = [False, True, True, None, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()
    expected = [val for val, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="pyarrow extension types aren't supported on Ray clusters.",
)
def test_series_filter_on_extension_array(uuid_ext_type) -> None:
    arr = pa.array(f"{i}".encode() for i in range(5))
    data = pa.ExtensionArray.from_storage(uuid_ext_type, arr)

    s = Series.from_arrow(data)
    pymask = [False, True, True, None, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()
    expected = [val for val, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected


def test_series_filter_on_canonical_tensor_extension_array() -> None:
    arr = np.arange(20).reshape((5, 2, 2))
    data = pa.FixedShapeTensorArray.from_numpy_ndarray(arr)

    s = Series.from_arrow(data)
    assert s.datatype() == DataType.tensor(DataType.int64(), (2, 2))
    pymask = [False, True, True, None, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert s.datatype() == result.datatype()
    expected = [val for val, keep in zip(s.to_pylist(), pymask) if keep]
    np.testing.assert_equal(result.to_pylist(), expected)


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


def test_series_filter_sparse_union() -> None:
    type_ids = pa.array([0, 1, 2, 0, 1, 2], type=pa.int8())
    int_child = pa.array([10, 0, 0, 40, 0, 0], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0, 0.0, 5.5, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c", "", "", "f"], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 'c', 40, 5.5, 'f']
    pymask = [True, False, True, False, True, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert result.datatype() == s.datatype()
    expected = [v for v, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected  # [10, 'c', 5.5]


def test_series_filter_dense_union() -> None:
    type_ids = pa.array([0, 1, 0, 0, 1], type=pa.int8())
    offsets = pa.array([0, 0, 1, 2, 1], type=pa.int32())
    int_child = pa.array([10, 30, 40], type=pa.int32())
    float_child = pa.array([2.2, 5.5], type=pa.float64())
    arrow_arr = pa.UnionArray.from_dense(type_ids, offsets, [int_child, float_child], field_names=["i", "f"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 30, 40, 5.5]
    pymask = [True, False, True, True, False]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert result.datatype() == s.datatype()
    expected = [v for v, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected  # [10, 30, 40]


def test_series_filter_sparse_union_none_mask() -> None:
    """None in the mask is treated as False (element is dropped)."""
    type_ids = pa.array([0, 1, 2], type=pa.int8())
    int_child = pa.array([10, 0, 0], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c"], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    # s = [10, 2.2, 'c']; None mask → dropped
    pymask = [True, None, True]
    mask = Series.from_pylist(pymask)

    result = s.filter(mask)

    assert result.datatype() == s.datatype()
    expected = [v for v, keep in zip(s.to_pylist(), pymask) if keep]
    assert result.to_pylist() == expected  # [10, 'c']


def test_series_filter_bad_input() -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data)

    mask = Series.from_pylist([False, False])
    with pytest.raises(ValueError, match="Lengths for filter do not match"):
        s.filter(mask).to_pylist()

    mask = Series.from_pylist([1])
    with pytest.raises(ValueError, match="We can only filter a Series with Boolean Series"):
        s.filter(mask).to_pylist()
