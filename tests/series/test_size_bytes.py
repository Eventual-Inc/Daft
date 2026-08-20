from __future__ import annotations

import itertools

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.conftest import get_tests_daft_runner_name
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES


def get_total_buffer_size(arr: pa.Array) -> int:
    """Helper to get total buffer size because older versions of Arrow don't have this method."""
    return sum([buf.size if buf is not None else 0 for buf in arr.buffers()])


@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_numeric_size_bytes(dtype, size, with_nulls) -> None:
    pydata = list(range(size))

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], dtype)
    else:
        data = pa.array(pydata, dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(dtype)
    assert s.size_bytes() == data.nbytes


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_string_size_bytes(size, with_nulls) -> None:
    pydata = list(str(i) for i in range(size))

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.large_string())
    else:
        data = pa.array(pydata, pa.large_string())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.string()
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_boolean_size_bytes(size, with_nulls) -> None:
    pydata = [True if i % 2 else False for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.bool_())
    else:
        data = pa.array(pydata, pa.bool_())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.bool()
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_date_size_bytes(size, with_nulls) -> None:
    from datetime import date

    pydata = [date(2023, 1, i + 1) for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.date32())
    else:
        data = pa.array(pydata, pa.date32())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.date()
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
@pytest.mark.parametrize("precision", ["us", "ns"])
def test_series_time_size_bytes(size, with_nulls, precision) -> None:
    from datetime import time

    pydata = [time(i, i, i, i) for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.time64(precision))
    else:
        data = pa.array(pydata, pa.time64(precision))

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.time(precision)
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_binary_size_bytes(size, with_nulls) -> None:
    pydata = [str(i).encode("utf-8") for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.large_binary())
    else:
        data = pa.array(pydata, pa.large_binary())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.binary()
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("size", [1, 2, 8, 9])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_fixed_size_binary_size_bytes(size, with_nulls) -> None:
    import random

    pydata = ["".join([str(random.randint(0, 9)) for _ in range(size)]).encode() for _ in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.binary(size))
    else:
        data = pa.array(pydata, pa.binary(size))

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.fixed_size_binary(size)
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_list_size_bytes(dtype, size, with_nulls) -> None:
    list_dtype = pa.list_(dtype)
    pydata = [[2 * i] if i % 2 == 0 else [2 * i, 2 * i + 1] for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], list_dtype)
    else:
        data = pa.array(pydata, list_dtype)

    s = Series.from_arrow(data)

    # Offset array is increased in bit width from 32 to 64 when converting to large_list
    # N rows have N+1 offset values, each going from 4 bytes (int32) to 8 bytes (int64)
    conversion_to_large_list_bytes = (len(data) + 1) * 4

    size_bytes = s.size_bytes()

    assert s.datatype() == DataType.from_arrow_type(list_dtype)
    assert size_bytes == get_total_buffer_size(data) + conversion_to_large_list_bytes


@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_fixed_size_list_size_bytes(dtype, size, with_nulls) -> None:
    list_dtype = pa.list_(dtype, 2)
    pydata = [[2 * i, 2 * i + 1] for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], list_dtype)
    else:
        data = pa.array(pydata, list_dtype)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.from_arrow_type(list_dtype)
    assert s.size_bytes() == get_total_buffer_size(data)


@pytest.mark.parametrize("size", [0, 1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_struct_size_bytes(size, with_nulls) -> None:
    dtype1, dtype2, dtype3 = pa.int64(), pa.float64(), pa.string()
    dtype = pa.struct({"a": dtype1, "b": dtype2, "c": dtype3})
    pydata = [
        {"a": 3 * i, "b": 3 * i + 1, "c": str(3 * i + 2)} if i % 2 == 0 else {"a": 3 * i, "c": str(3 * i + 2)}
        for i in range(size)
    ]

    # Artificially inject nulls if required as a top-level StructArray Null entry
    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], type=dtype)
    else:
        data = pa.array(pydata, type=dtype)

    s = Series.from_arrow(data)
    assert s.datatype() == DataType.from_arrow_type(dtype)

    # Offset buffer for child array "c" has length (len + 1), and is increased in bit width from 32 to 64 when converting to large_string
    conversion_to_large_string_bytes = (len(data) + 1) * 4

    assert s.size_bytes() == get_total_buffer_size(data) + conversion_to_large_string_bytes


@pytest.mark.skipif(
    get_tests_daft_runner_name() == "ray",
    reason="pyarrow extension types aren't supported on Ray clusters.",
)
@pytest.mark.parametrize("size", [1, 2, 8, 9, 16])
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_extension_type_size_bytes(uuid_ext_type, size, with_nulls) -> None:
    pydata = [f"{i}".encode() for i in range(size)]

    # TODO(Clark): Change to size > 0 condition when pyarrow extension arrays support generic construction on null arrays.
    if with_nulls and size > 1:
        pydata = pydata[:-1] + [None]
    storage = pa.array(pydata)
    data = pa.ExtensionArray.from_storage(uuid_ext_type, storage)

    s = Series.from_arrow(data)

    size_bytes = s.size_bytes()

    assert s.datatype() == DataType.extension(
        uuid_ext_type.NAME, DataType.from_arrow_type(uuid_ext_type.storage_type), ""
    )
    post_daft_cast_data = storage.cast(pa.large_binary())
    assert size_bytes == get_total_buffer_size(post_daft_cast_data)


def test_series_sparse_union_size_bytes() -> None:
    type_ids = pa.array([0, 1, 2, 0, 1, 2], type=pa.int8())
    int_child = pa.array([10, 0, 0, 40, 0, 0], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0, 0.0, 5.5, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c", "", "", "f"], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    assert s.size_bytes() == get_total_buffer_size(arrow_arr)


def test_series_dense_union_size_bytes() -> None:
    type_ids = pa.array([0, 1, 0, 0, 1], type=pa.int8())
    offsets = pa.array([0, 0, 1, 2, 1], type=pa.int32())
    int_child = pa.array([10, 30, 40], type=pa.int32())
    float_child = pa.array([2.2, 5.5], type=pa.float64())
    arrow_arr = pa.UnionArray.from_dense(type_ids, offsets, [int_child, float_child], field_names=["i", "f"])
    s = Series.from_arrow(arrow_arr)
    assert s.size_bytes() == get_total_buffer_size(arrow_arr)


def test_series_empty_sparse_union_size_bytes() -> None:
    arrow_arr = pa.UnionArray.from_sparse(
        pa.array([], type=pa.int8()),
        [pa.array([], type=pa.int32()), pa.array([], type=pa.float64())],
        field_names=["i", "f"],
    )
    s = Series.from_arrow(arrow_arr)
    assert s.size_bytes() == get_total_buffer_size(arrow_arr)


def test_series_sliced_sparse_union_size_bytes() -> None:
    """size_bytes on a sliced sparse union reflects only the slice, not the full buffers."""
    type_ids = pa.array([0, 1, 2, 0, 1, 2], type=pa.int8())
    int_child = pa.array([10, 0, 0, 40, 0, 0], type=pa.int32())
    float_child = pa.array([0.0, 2.2, 0.0, 0.0, 5.5, 0.0], type=pa.float64())
    str_child = pa.array(["", "", "c", "", "", "f"], type=pa.large_utf8())
    arrow_arr = pa.UnionArray.from_sparse(type_ids, [int_child, float_child, str_child], field_names=["i", "f", "s"])
    s = Series.from_arrow(arrow_arr)
    sliced = s.slice(1, 4)  # 3-element series
    full = s

    assert sliced.size_bytes() < full.size_bytes()
    assert sliced.size_bytes() > 0


@pytest.mark.parametrize("dtype, size", itertools.product(ARROW_INT_TYPES + ARROW_FLOAT_TYPES, [0, 1, 2, 8, 9, 16]))
@pytest.mark.parametrize("with_nulls", [True, False])
def test_series_canonical_tensor_extension_type_size_bytes(dtype, size, with_nulls) -> None:
    tensor_type = pa.fixed_shape_tensor(pa.int64(), (2, 2))
    if size == 0:
        storage = pa.array([], pa.list_(pa.int64(), 4))
        data = pa.FixedShapeTensorArray.from_storage(tensor_type, storage)
    elif with_nulls:
        pydata = np.arange(4 * size).reshape((size, 4)).tolist()[:-1] + [None]
        storage = pa.array(pydata, pa.list_(pa.int64(), 4))
        data = pa.FixedShapeTensorArray.from_storage(tensor_type, storage)
    else:
        arr = np.arange(4 * size).reshape((size, 2, 2))
        data = pa.FixedShapeTensorArray.from_numpy_ndarray(arr)

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.tensor(DataType.from_arrow_type(data.type.storage_type.value_type), (2, 2))
    assert s.size_bytes() == get_total_buffer_size(data)
