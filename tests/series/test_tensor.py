from __future__ import annotations

import copy

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType, get_super_ext_type
from daft.series import Series
from daft.utils import pyarrow_supports_fixed_shape_tensor
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES
from tests.utils import ANSI_ESCAPE

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())
DaftExtension = get_super_ext_type()


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
│ Tensor[Int64]         │
╞═══════════════════════╡
│ <Tensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <Tensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ None                  │
╰───────────────────────╯
"""
    )


def test_tensor_bfloat16_from_torch():
    torch = pytest.importorskip("torch")

    if not hasattr(torch, "bfloat16"):
        pytest.skip("torch.bfloat16 not available")

    tensor = torch.randn(4, dtype=torch.bfloat16)

    # Explicitly request a Tensor[BFloat16] column so that the logical dtype is BF16,
    # while the physical storage stays Float32 (as per DataType::BFloat16.to_physical()).
    s = Series.from_pylist(
        [tensor],
        dtype=DataType.tensor(DataType.bfloat16()),
        pyobj="allow",
    )

    assert s.datatype() == DataType.tensor(DataType.bfloat16())

    out = s.to_pylist()
    assert len(out) == 1
    arr_out = out[0]

    if isinstance(arr_out, torch.Tensor):
        assert arr_out.dtype == torch.bfloat16
        arr_out = arr_out.float().numpy()

    assert isinstance(arr_out, np.ndarray)
    assert arr_out.shape == (4,)
    # Values should be representable as float32 without crashing, even without NumPy BF16.
    np.asarray(arr_out, dtype=np.float32)


def test_tensor_bfloat16_from_torch_without_numpy_bfloat16():
    """When torch.bfloat16 exists but NumPy lacks a BF16 dtype (no ml_dtypes.bfloat16),
    we should still be able to ingest the tensor as Tensor[BFloat16] and materialize
    a NumPy array that can be viewed as float32.
    """

    torch = pytest.importorskip("torch")

    if not hasattr(torch, "bfloat16"):
        pytest.skip("torch.bfloat16 not available")

    # If NumPy already has a BF16 dtype via ml_dtypes, this environment is not testing the
    # runtime fallback path, so we skip.
    try:  # pragma: no cover - optional dependency
        import ml_dtypes  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - optional dependency
        ml_dtypes = None

    if getattr(ml_dtypes, "bfloat16", None) is not None:  # pragma: no cover - optional dependency
        pytest.skip("NumPy BF16 dtype is available; fallback is not exercised")

    tensor = torch.randn(4, dtype=torch.bfloat16)

    s = Series.from_pylist(
        [tensor],
        dtype=DataType.tensor(DataType.bfloat16()),
        pyobj="allow",
    )

    assert s.datatype() == DataType.tensor(DataType.bfloat16())

    out = s.to_pylist()
    assert len(out) == 1
    arr_out = out[0]

    if isinstance(arr_out, torch.Tensor):
        assert arr_out.dtype == torch.bfloat16
        arr_out = arr_out.float().numpy()

    assert isinstance(arr_out, np.ndarray)
    assert arr_out.shape == (4,)
    # Values should be representable as float32 without crashing, even without NumPy BF16.
    np.asarray(arr_out, dtype=np.float32)


def test_tensor_bfloat16_from_numpy_ml_dtypes():
    try:  # pragma: no cover - optional dependency
        import ml_dtypes  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - optional dependency
        pytest.skip("ml_dtypes is not available")

    ml_bfloat16 = getattr(ml_dtypes, "bfloat16", None)
    if ml_bfloat16 is None:
        pytest.skip("ml_dtypes.bfloat16 is not available")

    data = np.asarray([[1.0, 2.0], [3.0, 4.0]], dtype=ml_bfloat16)

    s = Series.from_pylist(
        [data],
        dtype=DataType.tensor(DataType.bfloat16()),
        pyobj="allow",
    )

    assert s.datatype() == DataType.tensor(DataType.bfloat16())

    out = s.to_pylist()
    assert len(out) == 1
    arr_out = out[0]

    # If we get a torch tensor, convert it to numpy for verification
    try:
        import torch
        if isinstance(arr_out, torch.Tensor):
            assert arr_out.dtype == torch.bfloat16
            arr_out = arr_out.float().numpy()
    except ImportError:
        pass

    assert isinstance(arr_out, np.ndarray)
    assert arr_out.shape == (2, 2)
    # Numerical values should survive a BF16->F32 upcast round-trip.
    np.testing.assert_allclose(
        np.asarray(arr_out, dtype=np.float32),
        np.asarray(data, dtype=np.float32),
        rtol=1e-2,
        atol=1e-2,
    )