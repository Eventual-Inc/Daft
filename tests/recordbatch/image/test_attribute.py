from __future__ import annotations

import pytest

import daft
from tests.recordbatch.image.conftest import MODE_TO_NUM_CHANNELS


@pytest.mark.parametrize("attr", ["width", "height", "channel", "mode"])
def test_image_attribute_mixed_shape(mixed_shape_data_fixture, attr):
    table = daft.from_pydict({"images": mixed_shape_data_fixture})
    table = table.with_column(attr, daft.col("images").image.attribute(attr))
    values = table.to_pydict()[attr]

    image_dtype = mixed_shape_data_fixture.datatype()
    mode = image_dtype.image_mode
    mode_name = str(mode).split(".")[-1]

    if attr == "width":
        assert all(x in (3, 4) for x in values if x is not None)
    elif attr == "height":
        assert all(x in (2, 3) for x in values if x is not None)
    elif attr == "channel":
        expected_channels = MODE_TO_NUM_CHANNELS[mode_name]
        assert all(x == expected_channels for x in values if x is not None)
    elif attr == "mode":
        expected_mode = MODE_TO_NUM_CHANNELS[mode_name]
        assert all(x == expected_mode for x in values if x is not None)


def test_image_direct_attribute_methods(mixed_shape_data_fixture):
    table = daft.from_pydict({"images": mixed_shape_data_fixture})

    table = table.with_columns(
        {
            "width": table["images"].image.width(),
            "height": table["images"].image.height(),
            "channel": table["images"].image.channel(),
            "mode": table["images"].image.mode(),
            "width_old": table["images"].image.attribute("width"),
            "height_old": table["images"].image.attribute("height"),
            "channel_old": table["images"].image.attribute("channel"),
            "mode_old": table["images"].image.attribute("mode"),
        }
    )

    result = table.to_pydict()

    assert result["width"] == result["width_old"]
    assert result["height"] == result["height_old"]
    assert result["channel"] == result["channel_old"]
    assert result["mode"] == result["mode_old"]
