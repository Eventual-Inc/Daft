from __future__ import annotations

import itertools

import numpy as np
import pyarrow as pa
import pytest
from ray.data.extensions import ArrowTensorArray

from daft import DataType, Series
from daft.context import get_context
from daft.utils import pyarrow_supports_fixed_shape_tensor
from tests.conftest import *
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


class MockObject:
    def __init__(self, test_val):
        self.test_val = test_val


@pytest.mark.parametrize(
    "dtype, chunks", itertools.product(ARROW_FLOAT_TYPES + ARROW_INT_TYPES + ARROW_STRING_TYPES, [1, 2, 3, 10])
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


@pytest.mark.parametrize("fixed", [False, True])
@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_list_array(chunks, fixed) -> None:
    series = []
    arrow_type = pa.list_(pa.int64(), list_size=2 if fixed else -1)
    for i in range(chunks):
        series.append(Series.from_arrow(pa.array([[i + j, i * j] for j in range(i)], type=arrow_type)))

    concated = Series.concat(series)

    if fixed:
        assert concated.datatype() == DataType.fixed_size_list(DataType.int64(), 2)
    else:
        assert concated.datatype() == DataType.list(DataType.int64())
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            assert concated_list[counter][0] == i + j
            assert concated_list[counter][1] == i * j
            counter += 1


@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_struct_array(chunks) -> None:
    series = []
    for i in range(chunks):
        series.append(
            Series.from_arrow(
                pa.array(
                    [{"a": i + j, "b": float(i * j)} for j in range(i)],
                    type=pa.struct({"a": pa.int64(), "b": pa.float64()}),
                )
            )
        )

    concated = Series.concat(series)

    assert concated.datatype() == DataType.struct({"a": DataType.int64(), "b": DataType.float64()})
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            assert concated_list[counter]["a"] == i + j
            assert concated_list[counter]["b"] == float(i * j)
            counter += 1


@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_tensor_array_ray(chunks) -> None:
    element_shape = (2, 2)
    num_elements_per_tensor = np.prod(element_shape)
    chunk_size = 3
    chunk_shape = (chunk_size,) + element_shape
    chunks = [
        np.arange(
            i * chunk_size * num_elements_per_tensor, (i + 1) * chunk_size * num_elements_per_tensor, dtype=np.int64
        ).reshape(chunk_shape)
        for i in range(chunks)
    ]
    series = [Series.from_arrow(ArrowTensorArray.from_numpy(chunk)) for chunk in chunks]

    concated = Series.concat(series)

    assert concated.datatype() == DataType.tensor(DataType.int64(), element_shape)
    expected = [chunk[i] for chunk in chunks for i in range(len(chunk))]
    np.testing.assert_equal(concated.to_pylist(), expected)


@pytest.mark.skipif(
    not pyarrow_supports_fixed_shape_tensor(),
    reason=f"Arrow version {ARROW_VERSION} doesn't support the canonical tensor extension type.",
)
@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_tensor_array_canonical(chunks) -> None:
    element_shape = (2, 2)
    num_elements_per_tensor = np.prod(element_shape)
    chunk_size = 3
    chunk_shape = (chunk_size,) + element_shape
    chunks = [
        np.arange(i * chunk_size * num_elements_per_tensor, (i + 1) * chunk_size * num_elements_per_tensor).reshape(
            chunk_shape
        )
        for i in range(chunks)
    ]
    ext_arrays = [pa.FixedShapeTensorArray.from_numpy_ndarray(chunk) for chunk in chunks]
    series = [Series.from_arrow(ext_array) for ext_array in ext_arrays]

    concated = Series.concat(series)

    assert concated.datatype() == DataType.tensor(
        DataType.from_arrow_type(ext_arrays[0].type.storage_type.value_type), (2, 2)
    )
    expected = [chunk[i] for chunk in chunks for i in range(len(chunk))]
    concated_arrow = concated.to_arrow()
    assert isinstance(concated_arrow.type, pa.FixedShapeTensorType)
    np.testing.assert_equal(concated_arrow.to_numpy_ndarray(), expected)


@pytest.mark.skipif(
    get_context().runner_config.name == "ray",
    reason="pyarrow extension types aren't supported on Ray clusters.",
)
@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_extension_type(uuid_ext_type, chunks) -> None:
    chunk_size = 3
    storage_arrays = [
        pa.array([f"{i}".encode() for i in range(j * chunk_size, (j + 1) * chunk_size)]) for j in range(chunks)
    ]
    ext_arrays = [pa.ExtensionArray.from_storage(uuid_ext_type, storage) for storage in storage_arrays]
    series = [Series.from_arrow(ext_array) for ext_array in ext_arrays]

    concated = Series.concat(series)

    assert concated.datatype() == DataType.extension(
        uuid_ext_type.NAME, DataType.from_arrow_type(uuid_ext_type.storage_type), ""
    )
    concated_arrow = concated.to_arrow()
    assert isinstance(concated_arrow.type, UuidType)
    assert concated_arrow.type == uuid_ext_type

    expected = uuid_ext_type.wrap_array(pa.concat_arrays(storage_arrays))

    assert concated_arrow == expected


@pytest.mark.parametrize("chunks", [1, 2, 3, 10])
def test_series_concat_pyobj(chunks) -> None:
    series = []
    for i in range(chunks):
        series.append(Series.from_pylist([MockObject(i * j) for j in range(i)], pyobj="force"))

    concated = Series.concat(series)

    assert concated.datatype() == DataType.python()
    concated_list = concated.to_pylist()

    counter = 0
    for i in range(chunks):
        for j in range(i):
            val = i * j
            assert concated_list[counter].test_val == val
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
