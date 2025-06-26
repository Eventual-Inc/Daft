from __future__ import annotations

import numpy as np
import pytest

import daft

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


@pytest.fixture(scope="session", params=MODES)
def fixed_shape_data_fixture(request):
    mode = request.param
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]

    height = 3
    width = 4
    dtype = daft.DataType.image(mode, height, width)
    shape = (height, width, num_channels)
    arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
    arrs = [arr, arr, None]

    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255

    s = daft.Series.from_pylist(arrs, pyobj="force")
    return s.cast(dtype)


@pytest.fixture(scope="session", params=MODES)
def mixed_shape_data_fixture(request):
    mode = request.param
    np_dtype = MODE_TO_NP_DTYPE[mode]
    num_channels = MODE_TO_NUM_CHANNELS[mode]

    dtype = daft.DataType.image(mode)
    shape1 = (2, 3, num_channels)
    shape2 = (3, 4, num_channels)
    arr1 = np.arange(np.prod(shape1)).reshape(shape1).astype(np_dtype)
    arr2 = np.arange(np.prod(shape1), np.prod(shape1) + np.prod(shape2)).reshape(shape2).astype(np_dtype)
    arrs = [arr1, arr2, None]

    if mode in ("LA", "RGBA"):
        for arr in arrs:
            if arr is not None:
                arr[..., -1] = 255

    s = daft.Series.from_pylist(arrs, pyobj="force")
    return s.cast(dtype)
