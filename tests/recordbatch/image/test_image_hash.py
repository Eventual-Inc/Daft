"""Python-level tests for image_hash, covering Image and FixedShapeImage types."""

from __future__ import annotations

import pytest

import daft
from daft import col
from daft.datatype import DataType


@pytest.mark.parametrize(
    "method",
    ["phash", "phash_simple", "dhash", "dhash_vertical", "ahash", "whash"],
)
def test_image_hash_mixed_shape(mixed_shape_data_fixture, method):
    """image_hash works on variable-shape Image columns and returns FixedSizeBinary(8)."""
    table = daft.from_pydict({"images": mixed_shape_data_fixture})
    result = table.with_column("h", col("images").image_hash(method=method))
    dtype = result.schema()["h"].dtype
    assert dtype == DataType.fixed_size_binary(8), f"Got {dtype}"

    values = result.to_pydict()["h"]
    # third element is None in the fixture
    assert values[0] is not None and len(values[0]) == 8
    assert values[1] is not None and len(values[1]) == 8
    assert values[2] is None


@pytest.mark.parametrize(
    "method",
    ["phash", "phash_simple", "dhash", "dhash_vertical", "ahash", "whash"],
)
def test_image_hash_fixed_shape(fixed_shape_data_fixture, method):
    """image_hash works on fixed-shape FixedShapeImage columns."""
    table = daft.from_pydict({"images": fixed_shape_data_fixture})
    result = table.with_column("h", col("images").image_hash(method=method))
    dtype = result.schema()["h"].dtype
    assert dtype == DataType.fixed_size_binary(8), f"Got {dtype}"

    values = result.to_pydict()["h"]
    assert values[0] is not None and len(values[0]) == 8
    assert values[1] is not None and len(values[1]) == 8
    assert values[2] is None


@pytest.mark.parametrize("hash_size", [4, 8, 16])
def test_image_hash_hash_size(mixed_shape_data_fixture, hash_size):
    """hash_size controls the output byte length: hash_size²/8 bytes."""
    expected_bytes = (hash_size * hash_size) // 8
    table = daft.from_pydict({"images": mixed_shape_data_fixture})
    result = table.with_column("h", col("images").image_hash(hash_size=hash_size))
    dtype = result.schema()["h"].dtype
    assert dtype == DataType.fixed_size_binary(expected_bytes), f"Got {dtype}"


def test_image_hash_identical_images_same_hash(mixed_shape_data_fixture):
    """Two identical images in the same batch produce identical hashes."""
    # Use the first two non-null rows — they might differ, but repeating one guarantees equality.
    import numpy as np

    from daft import DataType as DT
    from daft.series import Series

    mode_str = str(mixed_shape_data_fixture.datatype().image_mode).split(".")[-1]
    h, w = 4, 4
    channels = {"L": 1, "LA": 2, "RGB": 3, "RGBA": 4}[mode_str]
    arr = np.arange(h * w * channels, dtype=np.uint8).reshape(h, w, channels)

    s = Series.from_pylist([arr, arr], dtype=DT.python()).cast(DT.image(mode_str))
    table = daft.from_pydict({"images": s})
    result = table.with_column("h", col("images").image_hash()).collect()
    hashes = result.to_pydict()["h"]
    assert hashes[0] == hashes[1], "Identical images must produce the same hash"


def test_image_hash_expression_method_equals_function():
    """col.image_hash() and daft.functions.image_hash() produce identical results."""
    import numpy as np

    from daft.functions import image_hash
    from daft.series import Series

    arr = np.arange(3 * 4 * 3, dtype=np.uint8).reshape(3, 4, 3)
    s = Series.from_pylist([arr], dtype=DataType.python()).cast(DataType.image("RGB"))
    table = daft.from_pydict({"img": s})

    via_method = table.with_column("h1", col("img").image_hash(method="dhash")).collect()
    via_fn = table.with_column("h2", image_hash(col("img"), method="dhash")).collect()

    assert via_method.to_pydict()["h1"] == via_fn.to_pydict()["h2"]
