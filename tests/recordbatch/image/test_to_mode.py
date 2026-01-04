from __future__ import annotations

import pytest

from daft import DataType, col
from daft.recordbatch import MicroPartition
from tests.recordbatch.image.conftest import MODE_TO_NUM_CHANNELS


@pytest.mark.parametrize("out_mode", ["L", "LA", "RGB", "RGBA"])
def test_image_to_mode_mixed_shape(mixed_shape_data_fixture, out_mode):
    table = MicroPartition.from_pydict({"images": mixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").convert_image(out_mode)])
    assert result.schema()["images"].dtype == DataType.image(out_mode)
    result = result.to_pydict()
    assert [x.shape[2] == MODE_TO_NUM_CHANNELS[out_mode] if x is not None else None for x in result["images"]]


@pytest.mark.parametrize("out_mode", ["L", "LA", "RGB", "RGBA"])
def test_image_crop_fixed_shape(fixed_shape_data_fixture, out_mode):
    table = MicroPartition.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").convert_image(out_mode)])
    assert result.schema()["images"].dtype == DataType.image(out_mode, 3, 4)
    result = result.to_pydict()
    assert [x.shape == (3, 4, MODE_TO_NUM_CHANNELS[out_mode]) if x is not None else None for x in result["images"]]
