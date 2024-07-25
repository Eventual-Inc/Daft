from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import daft
from daft.table import MicroPartition

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


def test_image_crop_mixed_shape_same_mode(mixed_shape_data_fixture):
    table = MicroPartition.from_pydict({"images": mixed_shape_data_fixture})
    result = table.eval_expression_list([daft.col("images").image.crop((1, 1, 1, 1))])
    result = result.to_pydict()

    def crop(arr):
        return arr[1:2, 1:2, :]

    expected = [crop(arr) if arr is not None else None for arr in mixed_shape_data_fixture.to_pylist()]
    np.testing.assert_equal(result["images"], expected)


def test_image_crop_mixed_shape_same_mode_crop_col(mixed_shape_data_fixture):
    # TODO(jay): Need to fix nested casts -- for now we workaround by creating a nested uint32
    bboxes = pa.array([[1, 1, 1, 1], None, None], type=pa.list_(pa.uint32()))
    table = MicroPartition.from_pydict(
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
    table = MicroPartition.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([daft.col("images").image.crop((1, 1, 1, 1))])
    result = result.to_pydict()

    def crop(arr):
        return arr[1:2, 1:2, :]

    expected = [crop(arr) if arr is not None else None for arr in fixed_shape_data_fixture.to_pylist()]
    np.testing.assert_equal(result["images"], expected)


def test_image_crop_fixed_shape_same_mode_crop_col(fixed_shape_data_fixture):
    # TODO(jay): Need to fix nested casts -- for now we workaround by creating a nested uint32
    bboxes = pa.array([[1, 1, 1, 1], None, None], type=pa.list_(pa.uint32()))
    table = MicroPartition.from_pydict(
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
    table = MicroPartition.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})

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
