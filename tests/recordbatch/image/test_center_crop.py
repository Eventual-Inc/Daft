from __future__ import annotations

import numpy as np
import pytest

from daft import DataType, col
from daft.recordbatch import MicroPartition


def _center_crop_np(arr, w: int, h: int):
    if arr is None:
        return None
    height, width = arr.shape[0], arr.shape[1]
    cw = min(w, width)
    ch = min(h, height)
    x = (width - cw) // 2
    y = (height - ch) // 2
    return arr[y : y + ch, x : x + cw, :]


def test_image_center_crop_mixed_shape_same_mode(mixed_shape_data_fixture):
    table = MicroPartition.from_pydict({"images": mixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").center_crop(1, 1)])
    result = result.to_pydict()["images"]

    expected = [_center_crop_np(arr, 1, 1) if arr is not None else None for arr in mixed_shape_data_fixture.to_pylist()]
    np.testing.assert_equal(result, expected)


def test_image_center_crop_fixed_shape_dtype_and_values(fixed_shape_data_fixture):
    table = MicroPartition.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").center_crop(2, 2)])

    image_dtype = fixed_shape_data_fixture.datatype()
    mode = image_dtype.image_mode

    assert result.schema()["images"].dtype == DataType.image(mode, 2, 2)

    result = result.to_pydict()["images"]
    expected = [_center_crop_np(arr, 2, 2) if arr is not None else None for arr in fixed_shape_data_fixture.to_pylist()]
    np.testing.assert_equal(result, expected)


def test_image_center_crop_fixed_shape_clamp(fixed_shape_data_fixture):
    # Request a crop larger than the underlying image dimensions; this should
    # clamp to the original image size and effectively return the original
    # images unchanged.
    table = MicroPartition.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").center_crop(10, 10)])

    image_dtype = fixed_shape_data_fixture.datatype()

    assert result.schema()["images"].dtype == image_dtype

    result = result.to_pydict()["images"]
    expected = fixed_shape_data_fixture.to_pylist()
    np.testing.assert_equal(result, expected)


def test_image_center_crop_bad_expr_input():
    table = MicroPartition.from_pydict({"x": [1, 2, 3], "y": ["a", "b", "c"]})

    # Non-numeric widths/heights should raise when evaluated
    with pytest.raises(ValueError):
        table.eval_expression_list([col("x").center_crop("foo", 1)])

    with pytest.raises(ValueError):
        table.eval_expression_list([col("x").center_crop(1, "bar")])

    # Negative dimensions should also raise
    with pytest.raises(ValueError):
        table.eval_expression_list([col("x").center_crop(-1, 1)])

    # Calling center_crop on a non-image column should raise when evaluated
    with pytest.raises(ValueError):
        table.eval_expression_list([col("x").center_crop(1, 1)])
