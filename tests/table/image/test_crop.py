from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft.table import Table

MODES = ["L", "LA", "RGB", "RGBA"]
MODE_TO_NP_DTYPE = {
    "L": np.uint8,
    "LA": np.uint8,
    "RGB": np.uint8,
    "RGBA": np.uint8,
    "L16": np.uint16,
    "LA16": np.uint16,
    "RGB16": np.uint16,
    "RGBA16": np.uint16,
    "RGB32F": np.float32,
    "RGBA32F": np.float32,
}
MODE_TO_NUM_CHANNELS = {
    "L": 1,
    "LA": 2,
    "RGB": 3,
    "RGBA": 4,
    "L16": 1,
    "LA16": 2,
    "RGB16": 3,
    "RGBA16": 4,
    "RGB32F": 3,
    "RGBA32F": 4,
}


@pytest.fixture(scope="session", params=MODES)
def fixed_shape_data_fixture(request):
    mode = request.param
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]

    height = 3
    width = 4
    dtype = daft.DataType.image(mode, height, width)
    shape = (height, width, num_channels)
    arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
    arrs = [arr, arr, None]

    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255

    s = daft.Series.from_pylist(arrs, pyobj="force")
    return s.cast(dtype)


@pytest.fixture(scope="session", params=MODES)
def mixed_shape_data_fixture(request):
    mode = request.param
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]

    dtype = daft.DataType.image(mode)
    shape1 = (2, 3, num_channels)
    shape2 = (3, 4, num_channels)
    arr1 = np.arange(np.prod(shape1)).reshape(shape1).astype(np_dtype)
    arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2)).reshape(shape2).astype(np_dtype)
    arrs = [arr1, arr2, None]

    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255

    s = daft.Series.from_pylist(arrs, pyobj="force")
    return s.cast(dtype)


def test_image_crop_mixed_shape_same_mode(mixed_shape_data_fixture):
    table = Table.from_pydict({"images": mixed_shape_data_fixture})
    result = table.eval_expression_list([daft.col("images").image.crop((1, 1, 1, 1))])
    result = result.to_pydict()

    def crop(arr):
        return arr[1:2, 1:2, :]

    expected = [crop(arr) if arr is not None else None for arr in mixed_shape_data_fixture.to_pylist()]
    np.testing.assert_equal(result["images"], expected)


def test_image_crop_mixed_shape_same_mode_crop_col(mixed_shape_data_fixture):
    # TODO(jay): Need to fix nested casts -- for now we workaround by creating a nested uint32
    bboxes = pa.array([[1, 1, 1, 1], None, None], type=pa.list_(pa.uint32()))
    table = Table.from_pydict(
        {
            "images": mixed_shape_data_fixture,
            "bboxes": bboxes,
        }
    )
    result = table.eval_expression_list([daft.col("images").image.crop(daft.col("bboxes"))])
    result = result.to_pydict()

    def crop(arr):
        return arr[1:2, 1:2, :]

    expected = [crop(mixed_shape_data_fixture.to_pylist()[0]), None, None]
    np.testing.assert_equal(result["images"], expected)


def test_image_crop_fixed_shape_same_mode(fixed_shape_data_fixture):
    table = Table.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([daft.col("images").image.crop((1, 1, 1, 1))])
    result = result.to_pydict()

    def crop(arr):
        return arr[1:2, 1:2, :]

    expected = [crop(arr) if arr is not None else None for arr in fixed_shape_data_fixture.to_pylist()]
    np.testing.assert_equal(result["images"], expected)


def test_image_crop_fixed_shape_same_mode_crop_col(fixed_shape_data_fixture):
    # TODO(jay): Need to fix nested casts -- for now we workaround by creating a nested uint32
    bboxes = pa.array([[1, 1, 1, 1], None, None], type=pa.list_(pa.uint32()))
    table = Table.from_pydict(
        {
            "images": fixed_shape_data_fixture,
            "bboxes": bboxes,
        }
    )
    result = table.eval_expression_list([daft.col("images").image.crop(daft.col("bboxes"))])
    result = result.to_pydict()

    def crop(arr):
        return arr[1:2, 1:2, :]

    expected = [crop(fixed_shape_data_fixture.to_pylist()[0]), None, None]
    np.testing.assert_equal(result["images"], expected)


def test_bad_expr_input():
    table = Table.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})

    # Test bad Expression calls
    with pytest.raises(ValueError):
        daft.col("x").image.crop("foo")

    with pytest.raises(ValueError):
        daft.col("x").image.crop([1, 2, 3, 4, 5])

    with pytest.raises(ValueError):
        daft.col("x").image.crop([1.0, 2.0, 3.0, 4.0])

    # Test calling on bad types
    with pytest.raises(ValueError):
        table.eval_expression_list([daft.col("x").image.crop((1, 2, 3, 4))])
