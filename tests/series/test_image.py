from __future__ import annotations

import copy

import numpy as np
from PIL import Image

from daft.datatype import DaftExtension, DataType
from daft.series import Series


def test_image_arrow_round_trip():
    data = [np.arange(12).reshape((2, 2, 3)), np.arange(12, 39).reshape((3, 3, 3)), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")

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


def test_image_resize():

    first = np.ones((2, 2, 3), dtype=np.uint8)
    first[..., 1] = 2
    first[..., 2] = 3

    second = np.arange(12, dtype=np.uint8).reshape((1, 4, 3))
    data = [first, second, None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    resized = t.image.resize(5, 5)

    as_py = resized.to_pylist()

    assert resized.datatype() == target_dtype

    first_resized = np.array(as_py[0]["data"]).reshape(5, 5, 3)
    assert np.all(first_resized[..., 0] == 1)
    assert np.all(first_resized[..., 1] == 2)
    assert np.all(first_resized[..., 2] == 3)

    sec_resized = np.array(as_py[1]["data"]).reshape(5, 5, 3)
    sec_resized_gt = np.asarray(Image.fromarray(second).resize((5, 5)))
    assert np.all(sec_resized == sec_resized_gt)

    assert as_py[2] == None


def test_fixed_shape_image_arrow_round_trip():
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12).reshape(shape), np.arange(12, 24).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

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
