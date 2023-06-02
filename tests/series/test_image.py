from __future__ import annotations

import copy
import io

import cv2
import numpy as np
import pyarrow as pa
import pytest
from PIL import Image

from daft.datatype import DaftExtension, DataType, ImageMode
from daft.series import Series

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

MODE_TO_OPENCV_COLOR_CONVERSION = {
    "RGB": cv2.COLOR_RGB2BGR,
    "RGBA": cv2.COLOR_RGBA2BGRA,
    "RGB16": cv2.COLOR_RGB2BGR,
    "RGBA16": cv2.COLOR_RGBA2BGRA,
    "RGB32F": cv2.COLOR_RGB2BGR,
    "RGBA32F": cv2.COLOR_RGBA2BGRA,
}


def test_image_arrow_round_trip():
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint8).reshape((3, 3, 3)),
        None,
    ]
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


@pytest.mark.parametrize(
    ["mode", "file_format"],
    [
        ("L", "png"),
        ("L", "tiff"),
        ("LA", "png"),
        # Image crate doesn't support 2 samples per pixel.
        # ("LA", "tiff"),
        ("RGB", "png"),
        ("RGB", "tiff"),
        ("RGB", "bmp"),
        ("RGBA", "png"),
        ("RGBA", "tiff"),
        # Not supported by Daft or PIL.
        # "L16", "LA16", "RGB16", "RGBA16", "RGB32F", "RGBA32F"
    ],
)
def test_image_decode_pil(mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    img_mode = ImageMode.from_mode_string(mode)
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    shape = (4, 4)
    if num_channels > 1:
        shape += (num_channels,)
    arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
    img = Image.fromarray(arr, mode=mode)
    img_bytes = io.BytesIO()
    img.save(img_bytes, file_format)
    img_bytes = img_bytes.getvalue()
    arrow_arr = pa.array([img_bytes, img_bytes, img_bytes], type=pa.binary())
    s = Series.from_arrow(arrow_arr)
    t = s.image.decode()
    # TODO(Clark): Infer type-leve mode if all images are the same mode.
    assert t.datatype() == DataType.image()
    for py_img in t.to_pylist():
        assert py_img["channel"] == num_channels
        assert py_img["height"] == shape[0]
        assert py_img["width"] == shape[1]
        assert py_img["mode"] == img_mode
        np.testing.assert_equal(np.array(py_img["data"]).reshape(shape).astype(np_dtype), arr)


@pytest.mark.parametrize(
    ["mode", "file_format"],
    [
        ("L", "png"),
        ("L", "tiff"),
        # OpenCV doesn't support 2-channel images.
        # ("LA", "png"),
        ("RGB", "png"),
        ("RGB", "tiff"),
        ("RGB", "bmp"),
        ("RGBA", "png"),
        ("RGBA", "tiff"),
        ("RGBA", "webp"),
        # TODO(Clark): Support uint16 images.
        # ("L16", "png"),
        # OpenCV doesn't support 2-channel images.
        # ("LA16", "png"),
        # TODO(Clark): Support uint16 images.
        # ("RGB16", "png"),
        # ("RGB16", "tiff"),
        # ("RGBA16", "png"),
        # ("RGBA16", "tiff"),
        # Image crate doesn't support LogLuv HDR encoding.
        # ("RGB32F", "tiff"),
        # ("RGBA32F", "tiff"),
    ],
)
def test_image_decode_opencv(mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    img_mode = ImageMode.from_mode_string(mode)
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    shape = (4, 4, num_channels)
    arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
    cv2_arr = arr
    color_conv = MODE_TO_OPENCV_COLOR_CONVERSION.get(mode)
    if color_conv is not None:
        cv2_arr = cv2.cvtColor(arr, color_conv)
    encoded_arr = cv2.imencode(f".{file_format}", cv2_arr)[1]
    img_bytes = encoded_arr.tobytes()
    arrow_arr = pa.array([img_bytes, img_bytes, img_bytes], type=pa.binary())
    s = Series.from_arrow(arrow_arr)
    t = s.image.decode()
    # TODO(Clark): Support constructing an Image type with an unknown mode by known dtype.
    if np_dtype == np.uint8:
        # TODO(Clark): Infer type-leve mode if all images are the same mode.
        assert t.datatype() == DataType.image()
    for py_img in t.to_pylist():
        assert py_img["channel"] == num_channels
        assert py_img["height"] == shape[0]
        assert py_img["width"] == shape[1]
        assert py_img["mode"] == img_mode
        np.testing.assert_equal(np.array(py_img["data"]).reshape(shape).astype(np_dtype), arr)


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
    sec_resized_gt = np.asarray(Image.fromarray(second).resize((5, 5), resample=Image.BILINEAR))
    assert np.all(sec_resized == sec_resized_gt)

    assert as_py[2] == None


def test_image_resize_mixed_modes():
    rgba = np.ones((2, 2, 4), dtype=np.uint8)
    rgba[..., 1] = 2
    rgba[..., 2] = 3
    rgba[..., 3] = 4

    data = [
        rgba[..., :3],  # rgb
        rgba,  # RGBA
        np.arange(12, dtype=np.uint8).reshape((1, 4, 3)),  # RGB
        np.arange(12, dtype=np.uint8).reshape((3, 4)) * 10,  # L
        np.ones(24, dtype=np.uint8).reshape((3, 4, 2)) * 10,  # LA
        None,
    ]

    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image()

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    resized = t.image.resize(5, 5)

    as_py = resized.to_pylist()

    assert resized.datatype() == target_dtype

    first_resized = np.array(as_py[0]["data"]).reshape(5, 5, 3)
    assert np.all(first_resized[..., 0] == 1)
    assert np.all(first_resized[..., 1] == 2)
    assert np.all(first_resized[..., 2] == 3)

    second_resized = np.array(as_py[1]["data"]).reshape(5, 5, 4)
    assert np.all(second_resized[..., 0] == 1)
    assert np.all(second_resized[..., 1] == 2)
    assert np.all(second_resized[..., 2] == 3)
    assert np.all(second_resized[..., 3] == 4)

    for i in range(2, 4):
        resized_i = np.array(as_py[i]["data"]).reshape(5, 5, -1)
        resized_i_gt = np.asarray(Image.fromarray(data[i]).resize((5, 5), resample=Image.BILINEAR)).reshape(5, 5, -1)
        assert np.all(resized_i == resized_i_gt), f"{i} does not match"

    # LA sampling doesn't work for some reason in PIL
    resized_i = np.array(as_py[4]["data"]).reshape(5, 5, -1)
    assert np.all(resized_i == 10)

    assert as_py[-1] == None


def test_fixed_shape_image_arrow_round_trip():
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
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


def test_bad_cast_image():
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint64).reshape((3, 3, 3)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB")
    with pytest.raises(ValueError, match="Expected Numpy array to be of type: UInt8"):
        s.cast(target_dtype)


# Add enforcement for fixed sized list
@pytest.mark.skip()
def test_bad_cast_fixed_shape_image():
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint64).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    with pytest.raises(ValueError, match="Expected Numpy array to be of type: UInt8"):
        s.cast(target_dtype)
