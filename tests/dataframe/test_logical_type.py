from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from PIL import Image

import daft
from daft import DataType, Series, col
from daft.datatype import DaftExtension


def test_embedding_type_df() -> None:
    data = [[1, 2, 3], np.arange(3), ["1", "2", "3"], [1, "2", 3.0], pd.Series([1.1, 2, 3]), (1, 2, 3), None]
    df = daft.from_pydict({"index": np.arange(len(data)), "embeddings": Series.from_pylist(data, pyobj="force")})

    target = DataType.embedding("arr", DataType.float32(), 3)
    df = df.select(col("index"), col("embeddings").cast(target))
    df = df.repartition(4, "index")
    df = df.sort("index")
    df = df.collect()
    arrow_table = df.to_arrow()
    assert isinstance(arrow_table["embeddings"].type, DaftExtension)


@pytest.mark.parametrize("from_pil_imgs", [True, False])
def test_image_type_df(from_pil_imgs) -> None:
    data = [
        np.arange(12, dtype=np.uint8).reshape((2, 2, 3)),
        np.arange(12, 39, dtype=np.uint8).reshape((3, 3, 3)),
        None,
    ]
    if from_pil_imgs:
        data = [Image.fromarray(arr, mode="RGB") if arr is not None else None for arr in data]
    df = daft.from_pydict({"index": np.arange(len(data)), "image": Series.from_pylist(data, pyobj="force")})

    image_expr = col("image")
    if not from_pil_imgs:
        target = DataType.image("RGB")
        image_expr = image_expr.cast(target)
    df = df.select(col("index"), image_expr)
    df = df.repartition(4, "index")
    df = df.sort("index")
    df = df.collect()
    arrow_table = df.to_arrow()
    assert isinstance(arrow_table["image"].type, DaftExtension)


def test_fixed_shape_image_type_df() -> None:
    height = 2
    width = 2
    shape = (3, height, width)
    data = [np.arange(12, dtype=np.uint8).reshape(shape), np.arange(12, 24, dtype=np.uint8).reshape(shape), None]
    df = daft.from_pydict({"index": np.arange(len(data)), "image": Series.from_pylist(data, pyobj="force")})

    target = DataType.image("RGB", height, width)
    df = df.select(col("index"), col("image").cast(target))
    df = df.repartition(4, "index")
    df = df.sort("index")
    df = df.collect()
    arrow_table = df.to_arrow()
    assert isinstance(arrow_table["image"].type, DaftExtension)
