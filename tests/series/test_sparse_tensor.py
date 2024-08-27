from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.datatype import DataType
from daft.series import Series
from tests.series import ARROW_FLOAT_TYPES, ARROW_INT_TYPES

ARROW_VERSION = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric())

@pytest.mark.parametrize("dtype", ARROW_INT_TYPES + ARROW_FLOAT_TYPES)
def test_coo_sparse_tensor_roundtrip(dtype):
    np_dtype = dtype.to_pandas_dtype()
    shape = (3, 2, 2)
    data = [
        np.arange(12, dtype=np_dtype).reshape(shape),
        np.arange(12, 24, dtype=np_dtype).reshape(shape),
        None
    ]
    s = Series.from_pylist(data, pyobj="allow")

    tensor_dtype = DataType.tensor(DataType.from_arrow_type(dtype))

    t = s.cast(tensor_dtype)
    assert t.datatype() == tensor_dtype

    # Test sparse tensor roundtrip.
    sparse_tensor_dtype = DataType.coo_sparse_tensor(dtype=DataType.from_arrow_type(dtype))
    sparse_tensor_series = t.cast(sparse_tensor_dtype)

    assert sparse_tensor_series.datatype() == sparse_tensor_dtype
    back = sparse_tensor_series.cast(tensor_dtype)
    out = back.to_pylist()
    np.testing.assert_equal(out, data)