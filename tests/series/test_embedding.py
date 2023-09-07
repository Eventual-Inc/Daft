from __future__ import annotations

import copy

import numpy as np
import pandas as pd

from daft.datatype import DaftExtension, DataType
from daft.series import Series


def test_embedding_arrow_round_trip():
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2, 3]), (1, 2, 3), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.embedding(DataType.int32(), 3)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    arrow_arr = t.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    np.testing.assert_equal(from_arrow.to_pylist(), t.to_pylist())

    t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    np.testing.assert_equal(t_copy.to_pylist(), t.to_pylist())
