from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES
from tests.utils import ANSI_ESCAPE

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())


@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
@pytest.mark.parametrize("use_offset_indices", [None, False, True])
def test_sparse_tensor_roundtrip(dtype, use_offset_indices):
    np_dtype = dtype.to_pandas_dtype()
    data = [
        np.array([[0, 1, 0, 0], [0, 0, 0, 0]], dtype=np_dtype),
        None,
        np.array([[0, 0, 0, 0], [0, 0, 0, 0]], dtype=np_dtype),
        np.array([[0, 0, 0, 0], [0, 0, 4, 0]], dtype=np_dtype),
    ]
    s = Series.from_pylist(data, pyobj="allow")

    tensor_dtype = DataType.tensor(DataType.from_arrow_type(dtype))

    t = s.cast(tensor_dtype)
    assert t.datatype() == tensor_dtype

    # Test sparse tensor roundtrip.
    if use_offset_indices is None:
        sparse_tensor_dtype = DataType.sparse_tensor(dtype=DataType.from_arrow_type(dtype))
    else:
        sparse_tensor_dtype = DataType.sparse_tensor(
            dtype=DataType.from_arrow_type(dtype), use_offset_indices=use_offset_indices
        )
    sparse_tensor_series = t.cast(sparse_tensor_dtype)
    assert sparse_tensor_series.datatype() == sparse_tensor_dtype
    back = sparse_tensor_series.cast(tensor_dtype)
    out = back.to_pylist()
    np.testing.assert_equal(out, data)


def test_sparse_tensor_repr():
    arr = np.arange(np.prod((2, 2)), dtype=np.int64).reshape((2, 2))
    arrs = [arr, arr, None]
    s = Series.from_pylist(arrs, pyobj="allow")
    s = s.cast(DataType.sparse_tensor(dtype=DataType.from_arrow_type(pa.int64())))
    out_repr = ANSI_ESCAPE.sub("", repr(s))
    assert (
        out_repr.replace("\r", "")
        == """╭────────────────────────────────────────────╮
│ list_series                                │
│ ---                                        │
│ SparseTensor[Int64; indices_offset: false] │
╞════════════════════════════════════════════╡
│ <SparseTensor shape=(2, 2)>                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <SparseTensor shape=(2, 2)>                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ None                                       │
╰────────────────────────────────────────────╯
"""
    )


@pytest.mark.parametrize("indices_dtype", [np.uint8, np.uint16])
def test_minimal_indices_dtype_for_fixed_shape_sparse(indices_dtype: np.dtype):
    largest_index_possible = np.iinfo(indices_dtype).max
    tensor_shape = (largest_index_possible + 1, 1)

    series = Series.from_pylist([np.zeros(shape=tensor_shape)]).cast(
        DataType.tensor(DataType.float32(), shape=tensor_shape)
    )
    sparse_series = series.cast(DataType.sparse_tensor(DataType.float32(), shape=tensor_shape))

    received_tensor = sparse_series.to_pylist().pop()
    assert received_tensor["values"].dtype == np.float32
    assert received_tensor["indices"].dtype == indices_dtype
    assert received_tensor["shape"] == list(tensor_shape)
