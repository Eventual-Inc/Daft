from __future__ import annotations

import copy

import numpy as np

from daft.datatype import DaftExtension, DataType
from daft.series import Series


def test_image_arrow_round_trip():
    data = [np.arange(4).reshape((2, 2)), np.arange(4, 13).reshape((3, 3)), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image(DataType.int32())

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    arrow_arr = t.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    assert from_arrow.to_pylist() == t.to_pylist()

    t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    assert t_copy.to_pylist() == t.to_pylist()


def test_fixed_shape_image_arrow_round_trip():
    shape = (2, 2, 1)
    data = [np.arange(4).reshape(shape), np.arange(4, 8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image(DataType.int32(), shape)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    arrow_arr = t.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    assert from_arrow.to_pylist() == t.to_pylist()

    t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    assert t_copy.to_pylist() == t.to_pylist()
