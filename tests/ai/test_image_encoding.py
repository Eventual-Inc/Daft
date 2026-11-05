"""Regression guard for encoding image arrays as PNG, as the AI prompters do.

Pillow 13 changed how PNG encoding treats some integer modes. These tests pin the
behavior for the array layouts that Daft image columns produce, independent of any
AI provider package.
"""

from __future__ import annotations

import io

import pytest

np = pytest.importorskip("numpy")
PIL_Image = pytest.importorskip("PIL.Image")

from daft.ai.utils import encode_image_array_as_png

IMAGE_ARRAY_LAYOUTS = {
    "L": ((6, 5), "uint8"),
    "LA": ((6, 5, 2), "uint8"),
    "RGB": ((6, 5, 3), "uint8"),
    "RGBA": ((6, 5, 4), "uint8"),
    "L16": ((6, 5), "uint16"),
}


@pytest.mark.parametrize("layout", list(IMAGE_ARRAY_LAYOUTS))
def test_encode_image_array_as_png_roundtrips_exact_pixels(layout):
    shape, dtype = IMAGE_ARRAY_LAYOUTS[layout]
    max_value = np.iinfo(dtype).max
    image = (np.arange(np.prod(shape)) * (max_value // np.prod(shape))).reshape(shape).astype(dtype)

    png = encode_image_array_as_png(image)

    decoded = PIL_Image.open(io.BytesIO(png))
    assert decoded.format == "PNG"
    assert np.array_equal(np.asarray(decoded), image)
