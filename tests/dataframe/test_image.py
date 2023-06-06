from __future__ import annotations

import numpy as np
from PIL import Image

import daft
from daft.datatype import DataType
from daft.series import Series


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

    import ipdb

    ipdb.set_trace()

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
