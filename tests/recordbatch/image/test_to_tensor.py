from __future__ import annotations

from daft import DataType, col
from daft.recordbatch import MicroPartition
from tests.recordbatch.image.conftest import MODE_TO_NUM_CHANNELS


def test_image_to_tensor_mixed_shape(mixed_shape_data_fixture):
    table = MicroPartition.from_pydict({"images": mixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").image_to_tensor()])

    assert result.schema()["images"].dtype == DataType.tensor(DataType.uint8())
    result = result.to_pydict()

    image_dtype = mixed_shape_data_fixture.datatype()
    mode_name = str(image_dtype.image_mode).split(".")[-1]
    expected_channels = MODE_TO_NUM_CHANNELS[mode_name]

    # Fixture shapes are (2, 3, C) and (3, 4, C)
    expected_shapes = [(2, 3, expected_channels), (3, 4, expected_channels), None]
    assert [x.shape if x is not None else None for x in result["images"]] == expected_shapes


def test_image_to_tensor_fixed_shape(fixed_shape_data_fixture):
    table = MicroPartition.from_pydict({"images": fixed_shape_data_fixture})
    result = table.eval_expression_list([col("images").image_to_tensor()])

    image_dtype = fixed_shape_data_fixture.datatype()
    mode_name = str(image_dtype.image_mode).split(".")[-1]
    expected_channels = MODE_TO_NUM_CHANNELS[mode_name]

    assert result.schema()["images"].dtype == DataType.tensor(DataType.uint8(), shape=(3, 4, expected_channels))
    result = result.to_pydict()
    expected_shapes = [(3, 4, expected_channels), (3, 4, expected_channels), None]
    assert [x.shape if x is not None else None for x in result["images"]] == expected_shapes
