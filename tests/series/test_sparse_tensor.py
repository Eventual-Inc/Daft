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
def test_sparse_tensor_roundtrip(dtype):
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
    sparse_tensor_dtype = DataType.sparse_tensor(dtype=DataType.from_arrow_type(dtype))
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
        == """╭─────────────────────────────╮
│ list_series                 │
│ ---                         │
│ SparseTensor(Int64)         │
╞═════════════════════════════╡
│ <SparseTensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <SparseTensor shape=(2, 2)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ None                        │
╰─────────────────────────────╯
"""
    )
