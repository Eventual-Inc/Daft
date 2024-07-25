from __future__ import annotations

import pytest

import daft
from daft import DataType
from daft.table import MicroPartition

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


@pytest.mark.parametrize("out_mode", ["L", "LA", "RGB", "RGBA"])
def test_image_to_mode_mixed_shape(mixed_shape_data_fixture, out_mode):
    table = MicroPartition.from_pydict({"images": mixed_shape_data_fixture})
    result = table.eval_expression_list([daft.col("images").image.to_mode(out_mode)])
    assert result.schema()["images"].dtype == DataType.image(out_mode)
    result = result.to_pydict()
    assert [x.shape[2] == MODE_TO_NUM_CHANNELS[out_mode] if x is not None else None for x in result["images"]]


@pytest.mark.parametrize("out_mode", ["L", "LA", "RGB", "RGBA"])
def test_image_crop_fixed_shape(fixed_shape_data_fixture, out_mode):
    table = MicroPartition.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([daft.col("images").image.to_mode(out_mode)])
    assert result.schema()["images"].dtype == DataType.image(out_mode, 3, 4)
    result = result.to_pydict()
    assert [x.shape == (3, 4, MODE_TO_NUM_CHANNELS[out_mode]) if x is not None else None for x in result["images"]]
