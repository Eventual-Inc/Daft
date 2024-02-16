from __future__ import annotations

import itertools
import math

import numpy as np
import pyarrow as pa
import pytest

from daft.context import get_context
from daft.datatype import DataType
from daft.series import Series
from daft.utils import pyarrow_supports_fixed_shape_tensor
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())
PYARROW_GE_7_0_0 = ARROW_VERSION >= (7, 0, 0)


def get_total_buffer_size(arr: pa.Array) -> int:
    """Helper to get total buffer size because older versions of Arrow don't have the Array.get_total_buffer_size() method"""
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
def test_series_binary_size_bytes(size, with_nulls) -> None:
    pydata = [str(i).encode("utf-8") for i in range(size)]

    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], pa.large_binary())
    else:
        data = pa.array(pydata, pa.large_binary())

    s = Series.from_arrow(data)

    assert s.datatype() == DataType.binary()
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
    conversion_to_large_list_bytes = len(data) * 4

    size_bytes = s.size_bytes()

    # TODO(jay): There is an off-by-1 error in the arrow2 estimated_bytes_size API for List and LargeList:
    # https://github.com/jorgecarleitao/arrow2/blob/64d8ec203f991468032025a13a4f971f1f2cfc14/src/compute/aggregate/memory.rs#LL76C1-L76C1
    size_bytes = size_bytes + 4

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

    # Artifically inject nulls if required as a top-level StructArray Null entry
    if with_nulls and size > 0:
        data = pa.array(pydata[:-1] + [None], type=dtype)
    else:
        data = pa.array(pydata, type=dtype)

    s = Series.from_arrow(data)
    assert s.datatype() == DataType.from_arrow_type(dtype)

    # Offset buffer for child array "c" has length (len + 1), and is increased in bit width from 32 to 64 when converting to large_string
    conversion_to_large_string_bytes = (len(data) + 1) * 4

    # If nulls were injected, check validity bitmaps were were properly propagated to children
    if with_nulls and size > 0:
        child_arrays = [data.field(i) for i in range(data.type.num_fields)]
        child_arrays_without_validity_buffer = len(
            [child for child in child_arrays if any([b is None for b in child.buffers()])]
        )
        additional_validity_buffer_bytes = child_arrays_without_validity_buffer * math.ceil(size / 8)
        assert s.size_bytes() == (
            get_total_buffer_size(data) + additional_validity_buffer_bytes + conversion_to_large_string_bytes
        )
    else:
        assert s.size_bytes() == get_total_buffer_size(data) + conversion_to_large_string_bytes


@pytest.mark.skipif(
    get_context().runner_config.name == "ray",
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


@pytest.mark.skipif(
    not pyarrow_supports_fixed_shape_tensor(),
    reason=f"Arrow version {ARROW_VERSION} doesn't support the canonical tensor extension type.",
)
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
