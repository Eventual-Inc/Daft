from __future__ import annotations

import pathlib
import tempfile

import numpy as np
from PIL import Image

import daft


def test_decode_all_empty():
    df = daft.from_pydict({"foo": [b"not an image", None]})
    df = df.with_column("image", daft.col("foo").decode_image(on_error="null"))
    df.collect()

    assert df.to_pydict() == {
        "foo": [b"not an image", None],
        "image": [None, None],
    }


def test_decode_8bit_and_16bit_images():
    """Test that 8-bit and 16-bit images can be written to tempdir, read with Daft, and decoded successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = pathlib.Path(tmpdir)

        # Create an 8-bit grayscale image
        img_8bit_array = np.random.randint(0, 256, size=(10, 10), dtype=np.uint8)
        img_8bit = Image.fromarray(img_8bit_array, mode="L")
        img_8bit_path = tmpdir_path / "image_8bit.png"
        img_8bit.save(img_8bit_path)

        # Create a 16-bit grayscale image (TIFF format supports 16-bit)
        img_16bit_array = np.random.randint(0, 65536, size=(10, 10), dtype=np.uint16)
        img_16bit = Image.fromarray(img_16bit_array, mode="I;16")
        img_16bit_path = tmpdir_path / "image_16bit.tiff"
        img_16bit.save(img_16bit_path, format="TIFF")

        # Read with Daft and decode images
        df = daft.from_pydict({"path": [str(img_8bit_path), str(img_16bit_path)]})
        df = df.with_column("bytes", df["path"].download())
        df = df.with_column("image", df["bytes"].decode_image())

        # Collect and verify it succeeds
        result = df.collect()
        result_dict = result.to_pydict()

        # Verify that images were decoded (should not be None)
        assert len(result_dict["image"]) == 2
        assert result_dict["image"][0] is not None
        assert result_dict["image"][1] is not None
