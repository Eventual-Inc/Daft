from __future__ import annotations

import copy

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DaftExtension, DataType
from daft.series import Series
from daft.utils import pyarrow_supports_fixed_shape_tensor
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES
from tests.utils import ANSI_ESCAPE

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_tensor_roundtrip(dtype):
    np_dtype = dtype.to_pandas_dtype()
    data = [
        np.arange(8, dtype=np_dtype).reshape((2, 2, 2)),
        np.arange(8, 32, dtype=np_dtype).reshape((2, 2, 3, 2)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="allow")

    daft_dtype = DataType.tensor(DataType.from_arrow_type(dtype))

    assert s.datatype() == daft_dtype

    # Test pylist roundtrip.
    back_dtype = DataType.python()
    back = s.cast(back_dtype)

    assert back.datatype() == back_dtype

    out = back.to_pylist()
    np.testing.assert_equal(out, data)

    # Test Arrow roundtrip.
    arrow_arr = s.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(arrow_arr)

    assert from_arrow.datatype() == s.datatype()
    np.testing.assert_equal(from_arrow.to_pylist(), s.to_pylist())

    s_copy = copy.deepcopy(s)
    assert s_copy.datatype() == s.datatype()
    np.testing.assert_equal(s_copy.to_pylist(), s.to_pylist())


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_fixed_shape_tensor_roundtrip(dtype):
    np_dtype = dtype.to_pandas_dtype()
    shape = (3, 2, 2)
    data = [
        np.arange(12, dtype=np_dtype).reshape(shape),
        np.arange(12, 24, dtype=np_dtype).reshape(shape),
        None,
    ]
    s = Series.from_pylist(data, pyobj="allow")

    target_dtype = DataType.tensor(DataType.from_arrow_type(dtype), shape)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    # Test pylist roundtrip.
    back_dtype = DataType.python()
    back = t.cast(back_dtype)

    assert back.datatype() == back_dtype

    out = back.to_pylist()
    np.testing.assert_equal(out, data)

    # Test Arrow roundtrip.
    arrow_arr = t.to_arrow()

    if pyarrow_supports_fixed_shape_tensor():
        assert arrow_arr.type == pa.fixed_shape_tensor(dtype, shape)
    else:
        assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    np.testing.assert_equal(from_arrow.to_pylist(), t.to_pylist())

    if ARROW_VERSION >= (12, 0, 0):
        # Can't deepcopy pyarrow's fixed-shape tensor type.
        t_copy = t
    else:
        t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    np.testing.assert_equal(t_copy.to_pylist(), t.to_pylist())


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
@pytest.mark.parametrize("fixed_shape", [True, False])
def test_tensor_numpy_inference(dtype, fixed_shape):
    np_dtype = dtype.to_pandas_dtype()
    if fixed_shape:
        shape = (2, 2)
        arr = np.arange(np.prod(shape), dtype=np_dtype).reshape(shape)
        arrs = [arr, arr, None]
    else:
        shape1 = (2, 2)
        shape2 = (3, 3)
        arr1 = np.arange(np.prod(shape1), dtype=np_dtype).reshape(shape1)
        arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2), dtype=np_dtype).reshape(shape2)
        arrs = [arr1, arr2, None]
    s = Series.from_pylist(arrs, pyobj="allow")
    assert s.datatype() == DataType.tensor(DataType.from_arrow_type(dtype))
    out = s.to_pylist()
    np.testing.assert_equal(out, arrs)


def test_tensor_repr():
    arr = np.arange(np.prod((2, 2)), dtype=np.int64).reshape((2, 2))
    arrs = [arr, arr, None]
    s = Series.from_pylist(arrs, pyobj="allow")

    out_repr = ANSI_ESCAPE.sub("", repr(s))
    assert (
        out_repr.replace("\r", "")
        == """╭───────────────────────╮
│ list_series           │
│ ---                   │
│ Tensor(Int64)         │
╞═══════════════════════╡
│ <Tensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <Tensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ None                  │
╰───────────────────────╯
"""
    )
