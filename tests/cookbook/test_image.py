from __future__ import annotations

import io

import numpy as np
from PIL import Image

import daft
from daft import col
from daft.datatype import DataType
from daft.series import Series
from tests.cookbook.assets import ASSET_FOLDER


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
    df = daft.from_pydict({"img": s})

    target_dtype = DataType.image()
    df = df.select(df["img"].cast(target_dtype))

    assert df.schema()["img"].dtype == target_dtype

    df = df.with_column("resized", df["img"].image.resize(5, 5))

    assert df.schema()["resized"].dtype == target_dtype

    as_py = df.to_pydict()["resized"]

    first_resized = as_py[0]
    assert np.all(first_resized[..., 0] == 1)
    assert np.all(first_resized[..., 1] == 2)
    assert np.all(first_resized[..., 2] == 3)

    second_resized = as_py[1]
    assert np.all(second_resized[..., 0] == 1)
    assert np.all(second_resized[..., 1] == 2)
    assert np.all(second_resized[..., 2] == 3)
    assert np.all(second_resized[..., 3] == 4)

    for i in range(2, 4):
        resized_i = as_py[i]
        resized_i_gt = np.asarray(Image.fromarray(data[i]).resize((5, 5), resample=Image.BILINEAR)).reshape(5, 5, -1)
        assert np.all(resized_i == resized_i_gt), f"{i} does not match"

    # LA sampling doesn't work for some reason in PIL
    resized_i = as_py[4]
    assert np.all(resized_i == 10)

    assert as_py[-1] == None


def test_image_decode() -> None:
    df = (
        daft.from_glob_path(f"{ASSET_FOLDER}/images/**")
        .into_partitions(2)
        .with_column("image", col("path").url.download().image.decode().image.resize(10, 10))
    )
    target_dtype = DataType.image()
    assert df.schema()["image"].dtype == target_dtype
    df.collect()


def test_image_encode() -> None:
    file_format = "png"
    mode = "RGB"
    np_dtype = np.uint8
    shape = (4, 4, 3)
    arr = np.arange(np.prod(shape)).reshape(shape).astype(np_dtype)
    arrs = [arr, arr, arr]

    s = Series.from_pylist(arrs, pyobj="force")

    df = daft.from_pydict({"img": s}).into_partitions(2)
    target_dtype = DataType.image(mode)
    df = df.select(df["img"].cast(target_dtype))
    assert df.schema()["img"].dtype == target_dtype

    df = df.with_column("encoded", df["img"].image.encode(file_format))
    assert df.schema()["encoded"].dtype == DataType.binary()

    pil_decoded_imgs = [np.asarray(Image.open(io.BytesIO(bytes_))) for bytes_ in df.to_pydict()["encoded"]]
    np.testing.assert_equal(pil_decoded_imgs, arrs)
