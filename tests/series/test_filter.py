from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.context import get_context
from daft.datatype import DataType
from daft.series import Series
from daft.utils import pyarrow_supports_fixed_shape_tensor
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES, ARROW_STRING_TYPES

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


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


def test_series_filter_on_binary() -> None:
    s = Series.from_pylist([b"Y", b"N", None, b"Y", None, b"N"])
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
    get_context().runner_config.name == "ray",
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


@pytest.mark.skipif(
    not pyarrow_supports_fixed_shape_tensor(),
    reason=f"Arrow version {ARROW_VERSION} doesn't support the canonical tensor extension type.",
)
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


def test_series_filter_bad_input() -> None:
    data = pa.array([1, 2, 3, None, 5, None])

    s = Series.from_arrow(data)

    mask = Series.from_pylist([False, False])
    with pytest.raises(ValueError, match="Lengths for filter do not match"):
        s.filter(mask).to_pylist()

    mask = Series.from_pylist([1])
    with pytest.raises(ValueError, match="We can only filter a Series with Boolean Series"):
        s.filter(mask).to_pylist()
