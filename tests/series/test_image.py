from __future__ import annotations

import copy
import io

import cv2
import numpy as np
import pyarrow as pa
import pytest
from PIL import Image, ImageSequence

from daft.datatype import DaftExtension, DataType
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

NUM_CHANNELS_TO_MODE = {
    1: "L",
    2: "LA",
    3: "RGB",
    4: "RGBA",
}

MODE_TO_OPENCV_COLOR_CONVERSION = {
    "RGB": cv2.COLOR_RGB2BGR,
    "RGBA": cv2.COLOR_RGBA2BGRA,
    "RGB16": cv2.COLOR_RGB2BGR,
    "RGBA16": cv2.COLOR_RGBA2BGRA,
    "RGB32F": cv2.COLOR_RGB2BGR,
    "RGBA32F": cv2.COLOR_RGBA2BGRA,
}

MODE_TO_OPENCV_COLOR_CONVERSION_DECODE = {
    "RGB": cv2.COLOR_BGR2RGB,
    "RGBA": cv2.COLOR_BGRA2RGBA,
    "RGB16": cv2.COLOR_BGR2RGB,
    "RGBA16": cv2.COLOR_BGRA2RGBA,
    "RGB32F": cv2.COLOR_BGR2RGB,
    "RGBA32F": cv2.COLOR_BGRA2RGBA,
}


@pytest.mark.parametrize("give_mode", [True, False])
def test_image_round_trip(give_mode):
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint8).reshape((3, 3, 3)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    mode = "RGB" if give_mode else None
    target_dtype = DataType.image(mode)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    # Test pylist roundtrip.
    back_dtype = DataType.python()
    back = t.cast(back_dtype)

    assert back.datatype() == back_dtype

    out = back.to_pylist()
    np.testing.assert_equal(out, data)

    # Test Arrow roundtrip.
    arrow_arr = t.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    np.testing.assert_equal(from_arrow.to_pylist(), t.to_pylist())

    t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    np.testing.assert_equal(t_copy.to_pylist(), t.to_pylist())


def test_fixed_shape_image_round_trip():
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [
        np.arange(12, dtype=np.uint8).reshape(shape),
        np.arange(12, 24, dtype=np.uint8).reshape(shape),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    # Test pylist roundtrip.
    back_dtype = DataType.python()
    back = t.cast(back_dtype)

    assert back.datatype() == back_dtype

    out = back.to_pylist()
    np.testing.assert_equal(out, data)

    # Test Arrow roundtrip.
    arrow_arr = t.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    np.testing.assert_equal(from_arrow.to_pylist(), t.to_pylist())

    t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    np.testing.assert_equal(t_copy.to_pylist(), t.to_pylist())


@pytest.mark.parametrize(
    "mode",
    [
        "L",
        "LA",
        "RGB",
        "RGBA",
    ],
)
@pytest.mark.parametrize("fixed_shape", [True, False])
def test_image_pil_inference(fixed_shape, mode):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    if fixed_shape:
        height = 4
        width = 4
        shape = (height, width)
        if num_channels > 1:
            shape += (num_channels,)
        arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
        arrs = [arr, arr, None]
    else:
        shape1 = (2, 2)
        shape2 = (3, 3)
        if num_channels > 1:
            shape1 += (num_channels,)
            shape2 += (num_channels,)
        arr1 = np.arange(np.prod(shape1)).reshape(shape1).astype(np_dtype)
        arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2)).reshape(shape2).astype(np_dtype)
        arrs = [arr1, arr2, None]
    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255
    imgs = [Image.fromarray(arr, mode=mode) if arr is not None else None for arr in arrs]
    s = Series.from_pylist(imgs, pyobj="allow")
    assert s.datatype() == DataType.image(mode)
    out = s.to_pylist()
    if num_channels == 1:
        arrs = [np.expand_dims(arr, -1) for arr in arrs]
    np.testing.assert_equal(out, arrs)


def test_image_pil_inference_mixed():
    rgba = np.ones((2, 2, 4), dtype=np.uint8)
    rgba[..., 1] = 2
    rgba[..., 2] = 3
    rgba[..., 3] = 4

    arrs = [
        rgba[..., :3],  # RGB
        rgba,  # RGBA
        np.arange(12, dtype=np.uint8).reshape((1, 4, 3)),  # RGB
        np.arange(12, dtype=np.uint8).reshape((3, 4)) * 10,  # L
        np.ones(24, dtype=np.uint8).reshape((3, 4, 2)) * 10,  # LA
        None,
    ]
    imgs = [
        Image.fromarray(arr, mode=NUM_CHANNELS_TO_MODE[arr.shape[-1] if arr.ndim == 3 else 1])
        if arr is not None
        else None
        for arr in arrs
    ]

    # Forcing should still create Python Series
    s = Series.from_pylist(imgs, pyobj="force")
    assert s.datatype() == DataType.python()

    s = Series.from_pylist(imgs, pyobj="allow")
    assert s.datatype() == DataType.image()
    out = s.to_pylist()
    arrs[3] = np.expand_dims(arrs[3], axis=-1)
    np.testing.assert_equal(out, arrs)


@pytest.mark.parametrize(
    ["mode", "file_format"],
    [
        ("L", "png"),
        ("L", "tiff"),
        ("L", "jpeg"),
        ("LA", "png"),
        # Image crate doesn't support 2 samples per pixel.
        # ("LA", "tiff"),
        ("RGB", "png"),
        ("RGB", "tiff"),
        ("RGB", "bmp"),
        ("RGB", "gif"),
        ("RGB", "jpeg"),
        ("RGBA", "png"),
        ("RGBA", "tiff"),
        ("RGBA", "gif"),
        # Not supported by Daft or PIL.
        # "L16", "LA16", "RGB16", "RGBA16", "RGB32F", "RGBA32F"
    ],
)
@pytest.mark.parametrize("fixed_shape", [True, False])
def test_image_encode_pil(fixed_shape, mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    if fixed_shape:
        height = 4
        width = 4
        dtype = DataType.image(mode, height, width)
        shape = (height, width)
        if num_channels > 1:
            shape += (num_channels,)
        arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
        arrs = [arr, arr, None]
    else:
        dtype = DataType.image()
        shape1 = (2, 2)
        shape2 = (3, 3)
        if num_channels > 1:
            shape1 += (num_channels,)
            shape2 += (num_channels,)
        arr1 = np.arange(np.prod(shape1)).reshape(shape1).astype(np_dtype)
        arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2)).reshape(shape2).astype(np_dtype)
        arrs = [arr1, arr2, None]
    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255

    s = Series.from_pylist(arrs)
    t = s.cast(dtype)
    assert t.datatype() == dtype

    u = t.image.encode(file_format.upper())
    pil_imgs = [Image.open(io.BytesIO(bytes_)) if bytes_ is not None else None for bytes_ in u.to_pylist()]

    def pil_img_to_ndarray(img):
        if file_format == "gif":
            frames = [np.asarray(frame.copy().convert(mode), dtype=np.uint8) for frame in ImageSequence.Iterator(img)]
            if len(frames) == 1:
                return frames[0]
            else:
                return np.array(frames)
        else:
            return np.asarray(img)

    pil_decoded_imgs = [pil_img_to_ndarray(img) if img is not None else None for img in pil_imgs]
    if file_format == "jpeg":
        # Do lossy check; JPEG format is encoded at 0.75 quality.
        assert len(pil_decoded_imgs) == len(arrs)
        assert pil_decoded_imgs[-1] == arrs[-1]
        for pil_decoded_img, arr in zip(pil_decoded_imgs[:-1], arrs[:-1]):
            np.testing.assert_allclose(pil_decoded_img, arr, rtol=1, atol=4)
    else:
        np.testing.assert_equal(pil_decoded_imgs, arrs)


PIL_DECODE_MODES = [
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
]


@pytest.mark.parametrize(
    ["mode", "file_format"],
    PIL_DECODE_MODES,
)
def test_image_decode_pil(mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
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
    out = t.cast(DataType.python()).to_pylist()
    expected_arrs = [arr, arr, arr]
    if num_channels == 1:
        expected_arrs = [np.expand_dims(arr, -1) for arr in expected_arrs]
    np.testing.assert_equal(out, expected_arrs)


@pytest.mark.parametrize("output_mode", ["L", "LA", "RGB", "RGBA"])
def test_image_decode_pil_multi_mode(output_mode):
    imgs = []
    for mode, file_format in PIL_DECODE_MODES:
        np_dtype = MODE_TO_NP_DTYPE[mode]
        num_channels = MODE_TO_NUM_CHANNELS[mode]
        shape = (4, 4)
        if num_channels > 1:
            shape += (num_channels,)
        arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
        img = Image.fromarray(arr, mode=mode)
        img_bytes = io.BytesIO()
        img.save(img_bytes, file_format)
        img_bytes = img_bytes.getvalue()
        imgs.append(img_bytes)

    arrow_arr = pa.array(imgs, type=pa.binary())
    s = Series.from_arrow(arrow_arr)
    t = s.image.decode(mode=output_mode)
    assert t.datatype() == DataType.image(output_mode)
    out = t.cast(DataType.python()).to_pylist()
    for o in out:
        assert o.shape == (4, 4, MODE_TO_NUM_CHANNELS[output_mode])


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
@pytest.mark.parametrize("fixed_shape", [True, False])
def test_image_encode_decode_pil_roundtrip(fixed_shape, mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    if fixed_shape:
        height = 4
        width = 4
        shape = (height, width)
        if num_channels > 1:
            shape += (num_channels,)
        arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
        arrs = [arr, arr, None]
    else:
        shape1 = (2, 2)
        shape2 = (3, 3)
        if num_channels > 1:
            shape1 += (num_channels,)
            shape2 += (num_channels,)
        arr1 = np.arange(np.prod(shape1)).reshape(shape1).astype(np_dtype)
        arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2)).reshape(shape2).astype(np_dtype)
        arrs = [arr1, arr2, None]
    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255
    imgs_bytes = []
    for arr in arrs:
        if arr is not None:
            img = Image.fromarray(arr, mode=mode)
            img_bytes = io.BytesIO()
            img.save(img_bytes, file_format)
            img_bytes = img_bytes.getvalue()
        else:
            img_bytes = None
        imgs_bytes.append(img_bytes)
    arrow_arr = pa.array(imgs_bytes, type=pa.binary())
    s = Series.from_arrow(arrow_arr)
    t = s.image.decode()
    # TODO(Clark): Infer type-leve mode if all images are the same mode.
    assert t.datatype() == DataType.image()

    u = t.image.encode(file_format.upper())
    pil_decoded_imgs = [
        np.asarray(Image.open(io.BytesIO(bytes_))) if bytes_ is not None else None for bytes_ in u.to_pylist()
    ]
    np.testing.assert_equal(pil_decoded_imgs, arrs)


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
        # Rust image crate doesn't support WebP encoding.
        # ("RGBA", "webp"),
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
def test_image_encode_opencv(mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    shape = (4, 4, num_channels)
    arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
    arrs = [arr, arr, arr]

    s = Series.from_pylist(arrs)
    t = s.cast(DataType.image(mode))
    assert t.datatype() == DataType.image(mode)
    # TODO(Clark): Support constructing an Image type with an unknown mode by known dtype.
    if np_dtype == np.uint8:
        # TODO(Clark): Infer type-leve mode if all images are the same mode.
        assert t.datatype() == DataType.image(mode)

    u = t.image.encode(file_format.upper())
    opencv_decoded_imgs = [
        cv2.imdecode(np.frombuffer(bytes_, dtype=np.uint8), cv2.IMREAD_UNCHANGED) for bytes_ in u.to_pylist()
    ]
    if num_channels == 1:
        opencv_decoded_imgs = [np.expand_dims(arr, -1) for arr in opencv_decoded_imgs]
    color_conv = MODE_TO_OPENCV_COLOR_CONVERSION_DECODE.get(mode)
    if color_conv is not None:
        opencv_decoded_imgs = [cv2.cvtColor(arr, color_conv) for arr in opencv_decoded_imgs]
    np.testing.assert_equal(opencv_decoded_imgs, arrs)


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
    out = t.cast(DataType.python()).to_pylist()
    expected_arrs = [arr, arr, arr]
    np.testing.assert_equal(out, expected_arrs)


# check support for decoding even from unsupported types
@pytest.mark.parametrize("output_mode", ["L", "LA", "RGB", "RGBA"])
def test_image_decode_opencv_multi_modes(output_mode):
    INPUT_MODES = [
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
        ("L16", "png"),
        # OpenCV doesn't support 2-channel images.
        # ("LA16", "png"),
        ("RGB16", "png"),
        ("RGB16", "tiff"),
        ("RGBA16", "png"),
        ("RGBA16", "tiff"),
        # Image crate doesn't support LogLuv HDR encoding.
        # ("RGB32F", "tiff"),
        # ("RGBA32F", "tiff"),
    ]
    imgs = []
    for mode, file_format in INPUT_MODES:
        np_dtype = MODE_TO_NP_DTYPE[mode]
        num_channels = MODE_TO_NUM_CHANNELS[mode]
        shape = (4, 4, num_channels)
        arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
        cv2_arr = arr
        color_conv = MODE_TO_OPENCV_COLOR_CONVERSION.get(mode)
        if color_conv is not None:
            cv2_arr = cv2.cvtColor(arr, color_conv)
        encoded_arr = cv2.imencode(f".{file_format}", cv2_arr)[1]
        img_bytes = encoded_arr.tobytes()
        imgs.append(img_bytes)
    arrow_arr = pa.array(imgs, type=pa.binary())
    s = Series.from_arrow(arrow_arr)
    t = s.image.decode(mode=output_mode)
    assert t.datatype() == DataType.image(output_mode)
    out = t.to_pylist()
    for o in out:
        assert o.shape == (4, 4, MODE_TO_NUM_CHANNELS[output_mode])


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
        # Rust image crate doesn't support WebP encoding.
        # ("RGBA", "webp"),
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
def test_image_encode_decode_opencv_roundtrip(mode, file_format):
    np_dtype = MODE_TO_NP_DTYPE[mode]
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

    u = t.image.encode(file_format.upper())
    opencv_decoded_imgs = [
        cv2.imdecode(np.frombuffer(bytes_, dtype=np.uint8), cv2.IMREAD_UNCHANGED) for bytes_ in u.to_pylist()
    ]
    if num_channels == 1:
        opencv_decoded_imgs = [np.expand_dims(arr, -1) for arr in opencv_decoded_imgs]
    color_conv = MODE_TO_OPENCV_COLOR_CONVERSION_DECODE.get(mode)
    if color_conv is not None:
        opencv_decoded_imgs = [cv2.cvtColor(arr, color_conv) for arr in opencv_decoded_imgs]
    np.testing.assert_equal(opencv_decoded_imgs, [arr, arr, arr])


@pytest.mark.parametrize("mode", ["L", "LA", "RGB", "RGBA"])
@pytest.mark.parametrize("fixed_shape", [True, False])
def test_image_resize_same_mode(fixed_shape, mode):
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]
    if fixed_shape:
        height = 3
        width = 4
        dtype = DataType.image(mode, height, width)
        shape = (height, width, num_channels)
        arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
        arrs = [arr, arr, None]
    else:
        dtype = DataType.image(mode)
        shape1 = (2, 3, num_channels)
        shape2 = (3, 4, num_channels)
        arr1 = np.arange(np.prod(shape1)).reshape(shape1).astype(np_dtype)
        arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2)).reshape(shape2).astype(np_dtype)
        arrs = [arr1, arr2, None]

    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255

    s = Series.from_pylist(arrs, pyobj="force")
    t = s.cast(dtype)
    assert t.datatype() == dtype

    resized = t.image.resize(5, 5)
    resized_dtype = DataType.image(mode, 5, 5)
    assert resized.datatype() == resized_dtype
    out = resized.cast(DataType.python()).to_pylist()

    def resize(arr):
        # Use opencv as a resizing baseline.
        return cv2.resize(arr, dsize=(5, 5), interpolation=cv2.INTER_LINEAR_EXACT)

    expected = [resize(arr) if arr is not None else None for arr in arrs]
    if num_channels == 1:
        expected = [np.expand_dims(arr, -1) for arr in expected]

    np.testing.assert_equal(out, expected)


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

    out = resized.cast(DataType.python()).to_pylist()

    def resize(arr):
        # Use opencv as a resizing baseline.
        arr = cv2.resize(arr, dsize=(5, 5), interpolation=cv2.INTER_LINEAR_EXACT)
        if arr.ndim == 2:
            arr = np.expand_dims(arr, -1)
        return arr

    np.testing.assert_equal(out, [resize(arr) if arr is not None else None for arr in data])


def test_fixed_shape_image_roundtrip():
    height = 2
    width = 2
    shape = (height, width, 3)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    s = Series.from_pylist(data, pyobj="force")

    target_dtype = DataType.image("RGB", height, width)

    t = s.cast(target_dtype)

    assert t.datatype() == target_dtype

    # Test pylist roundtrip.
    back_dtype = DataType.python()
    back = t.cast(back_dtype)

    assert back.datatype() == back_dtype

    out = back.to_pylist()
    np.testing.assert_equal(out, data)

    # Test Arrow roundtrip.
    arrow_arr = t.to_arrow()

    assert isinstance(arrow_arr.type, DaftExtension)
    from_arrow = Series.from_arrow(t.to_arrow())

    assert from_arrow.datatype() == t.datatype()
    np.testing.assert_equal(from_arrow.to_pylist(), t.to_pylist())

    t_copy = copy.deepcopy(t)
    assert t_copy.datatype() == t.datatype()
    np.testing.assert_equal(t_copy.to_pylist(), t.to_pylist())


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


def test_on_error_image_decode():
    s = Series.from_pylist([b"not an image"])

    with pytest.raises(NotImplementedError, match="Unimplemented on_error option"):
        s.image.decode(on_error="NOT IMPLEMENTED")

    with pytest.raises(ValueError, match="Decoding image from bytes failed"):
        s.image.decode(on_error="raise")

    s.image.decode(on_error="null").to_pylist() == [None]


mode_cast_modes = ["L", "LA", "RGB", "RGBA"]


@pytest.mark.parametrize(
    ["input_mode", "output_mode"],
    [(a, b) for a in mode_cast_modes for b in mode_cast_modes],
)
def test_image_to_mode(input_mode, output_mode):
    channels = MODE_TO_NUM_CHANNELS[input_mode]
    data = [
        np.arange(4 * channels, dtype=np.uint8).reshape((2, 2, channels)),
        np.arange(4 * channels, 13 * channels, dtype=np.uint8).reshape((3, 3, channels)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    s = s.cast(DataType.image(input_mode)).image.to_mode(output_mode)
    assert s.datatype() == DataType.image(output_mode)
    assert s.to_pylist()[0].shape[2] == MODE_TO_NUM_CHANNELS[output_mode]


@pytest.mark.parametrize(
    ["input_mode", "output_mode"],
    [(a, b) for a in mode_cast_modes for b in mode_cast_modes],
)
def test_image_to_mode_fixed_size(input_mode, output_mode):
    channels = MODE_TO_NUM_CHANNELS[input_mode]
    data = [
        np.arange(4 * channels, dtype=np.uint8).reshape((2, 2, channels)),
        np.arange(4 * channels, 8 * channels, dtype=np.uint8).reshape((2, 2, channels)),
        None,
    ]
    s = Series.from_pylist(data, pyobj="force")

    s = s.cast(DataType.image(input_mode, 2, 2)).image.to_mode(output_mode)
    assert s.datatype() == DataType.image(output_mode, 2, 2)
    assert s.to_pylist()[0].shape[2] == MODE_TO_NUM_CHANNELS[output_mode]
