"""Independent tests for image_hash — no imagehash library required."""

from __future__ import annotations

import numpy as np
import pytest

import daft
from daft.datatype import DataType
from daft.functions import image_hash
from tests.cookbook.conftest import (
    checkerboard,
    colorful_rgb,
    grad2d,
    gradient_rgb,
    hamming_distance,
    make_image_df,
    solid_rgb,
)

# ─── Output format (dtype / byte length) ─────────────────────────────────────


@pytest.mark.parametrize("method", ["phash", "phash_simple", "dhash", "dhash_vertical", "ahash", "whash"])
def test_image_hash_returns_correct_dtype_and_length(method):
    """Hash output is FixedSizeBinary(8) for default hash_size=8."""
    df = make_image_df([solid_rgb(100, 150, 200)])
    result = df.with_column("h", image_hash(df["img"], method=method)).collect()
    dtype = result.schema()["h"].dtype
    assert dtype == DataType.fixed_size_binary(8), f"Expected Binary[8], got {dtype}"
    hashes = result.to_pydict()["h"]
    assert len(hashes) == 1
    assert len(hashes[0]) == 8


def test_image_hash_hash_size_16_returns_32_bytes():
    """hash_size=16 produces 16*16/8 = 32 bytes per hash."""
    df = make_image_df([solid_rgb(80, 120, 200)])
    result = df.with_column("h", image_hash(df["img"], hash_size=16)).collect()
    dtype = result.schema()["h"].dtype
    assert dtype == DataType.fixed_size_binary(32), f"Expected Binary[32], got {dtype}"
    hashes = result.to_pydict()["h"]
    assert len(hashes[0]) == 32


def test_crop_resistant_returns_correct_dtype():
    """crop_resistant with hash_size=8 produces 9*64/8 = 72 bytes."""
    df = make_image_df([gradient_rgb(h=32, w=32)])
    result = df.with_column("h", image_hash(df["img"], method="crop_resistant")).collect()
    dtype = result.schema()["h"].dtype
    assert dtype == DataType.fixed_size_binary(72), f"Expected Binary[72], got {dtype}"
    hashes = result.to_pydict()["h"]
    assert len(hashes[0]) == 72


def test_colorhash_output_size():
    """Colorhash returns the correct number of bytes: ceil(14 * binbits / 8)."""
    img = colorful_rgb()
    df = make_image_df([img])
    for binbits in [2, 3, 4, 8]:
        expected = (14 * binbits + 7) // 8
        result = df.with_column("h", image_hash(df["img"], method="colorhash", binbits=binbits)).collect()
        h = result.to_pydict()["h"][0]
        assert len(h) == expected, f"binbits={binbits}: expected {expected} bytes, got {len(h)}"


# ─── Null propagation ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "method",
    ["phash", "phash_simple", "dhash", "dhash_vertical", "ahash", "whash", "crop_resistant", "colorhash"],
)
def test_image_hash_null_propagation(method):
    """Null images produce null hashes for every method."""
    df = make_image_df([solid_rgb(10, 20, 30), None, solid_rgb(200, 100, 50)])
    result = df.with_column("h", image_hash(df["img"], method=method)).collect()
    hashes = result.to_pydict()["h"]
    assert hashes[0] is not None
    assert hashes[1] is None
    assert hashes[2] is not None


# ─── Identical images → zero Hamming distance ────────────────────────────────


@pytest.mark.parametrize(
    "method",
    ["phash", "phash_simple", "dhash", "dhash_vertical", "ahash", "whash", "crop_resistant", "colorhash"],
)
def test_identical_images_have_zero_distance(method):
    """Two identical images must produce the same hash."""
    img = gradient_rgb(h=32, w=32)
    df = make_image_df([img, img])
    result = df.with_column("h", image_hash(df["img"], method=method)).collect()
    hashes = result.to_pydict()["h"]
    assert hamming_distance(hashes[0], hashes[1]) == 0


# ─── Discriminability (different images should differ) ───────────────────────


def test_phash_different_images_differ():
    """PHash distinguishes images with different spatial frequency content."""
    gradient = gradient_rgb(h=32, w=32)
    checker = checkerboard(h=32, w=32, cell=2)
    df = make_image_df([gradient, checker])
    result = df.with_column("h", image_hash(df["img"], method="phash")).collect()
    hashes = result.to_pydict()["h"]
    assert hamming_distance(hashes[0], hashes[1]) > 0


def test_dhash_different_gradients_differ():
    """DHash distinguishes images with opposite spatial gradients."""
    left_to_right = gradient_rgb(invert=False)
    right_to_left = gradient_rgb(invert=True)
    df = make_image_df([left_to_right, right_to_left])
    result = df.with_column("h", image_hash(df["img"], method="dhash")).collect()
    hashes = result.to_pydict()["h"]
    assert hamming_distance(hashes[0], hashes[1]) > 0


def test_ahash_different_textures_differ():
    """AHash distinguishes images with complementary pixel distributions."""
    checker = checkerboard(cell=2)
    inverted = 255 - checker
    df = make_image_df([checker, inverted])
    result = df.with_column("h", image_hash(df["img"], method="ahash")).collect()
    hashes = result.to_pydict()["h"]
    assert hamming_distance(hashes[0], hashes[1]) > 0


def test_dhash_vertical_distinguishes_vertical_gradient():
    """dhash_vertical distinguishes top-bright from bottom-bright images."""
    top_bright = gradient_rgb(h=32, w=32, invert=False)
    top_bright_vert = np.rot90(top_bright)
    bottom_bright_vert = np.rot90(top_bright, k=3)
    df = make_image_df([top_bright_vert, bottom_bright_vert])
    result = df.with_column("h", image_hash(df["img"], method="dhash_vertical")).collect()
    hashes = result.to_pydict()["h"]
    assert hamming_distance(hashes[0], hashes[1]) > 0


def test_colorhash_different_colors_differ():
    """Colorhash distinguishes images with clearly different color palettes."""
    red = solid_rgb(200, 50, 50, h=32, w=32)
    blue = solid_rgb(50, 50, 200, h=32, w=32)
    df = make_image_df([red, blue])
    result = df.with_column("h", image_hash(df["img"], method="colorhash")).collect()
    hashes = result.to_pydict()["h"]
    assert hamming_distance(hashes[0], hashes[1]) > 0


# ─── Similarity ordering (similar < dissimilar) ───────────────────────────────


def test_phash_similar_images_closer_than_dissimilar():
    """pHash: noisy copy should have smaller Hamming distance than a very different image."""
    rng = np.random.default_rng(42)
    original = grad2d(h=32, w=32)
    noisy = np.clip(original.astype(np.int32) + rng.integers(-10, 11, original.shape), 0, 255).astype(np.uint8)
    different = checkerboard(h=32, w=32, cell=2)

    df = make_image_df([original, noisy, different])
    result = df.with_column("h", image_hash(df["img"], method="phash")).collect()
    hashes = result.to_pydict()["h"]

    dist_similar = hamming_distance(hashes[0], hashes[1])
    dist_different = hamming_distance(hashes[0], hashes[2])
    assert dist_similar <= dist_different, (
        f"phash: noisy copy (dist={dist_similar}) should be <= checkerboard (dist={dist_different})"
    )


def test_phash_simple_similar_images_closer_than_dissimilar():
    """phash_simple: noisy copy should have smaller Hamming distance than a very different image."""
    rng = np.random.default_rng(42)
    original = grad2d(h=32, w=32)
    noisy = np.clip(original.astype(np.int32) + rng.integers(-10, 11, original.shape), 0, 255).astype(np.uint8)
    different = checkerboard(h=32, w=32, cell=2)

    df = make_image_df([original, noisy, different])
    result = df.with_column("h", image_hash(df["img"], method="phash_simple")).collect()
    hashes = result.to_pydict()["h"]

    dist_similar = hamming_distance(hashes[0], hashes[1])
    dist_different = hamming_distance(hashes[0], hashes[2])
    assert dist_similar <= dist_different, (
        f"phash_simple: noisy copy (dist={dist_similar}) should be <= checkerboard (dist={dist_different})"
    )


def test_dhash_similar_images_closer_than_dissimilar():
    """dHash: a nearly identical gradient image should be closer than its inverse."""
    rng = np.random.default_rng(7)
    original = gradient_rgb(h=32, w=32, invert=False)
    noisy = np.clip(original.astype(np.int32) + rng.integers(-5, 6, original.shape), 0, 255).astype(np.uint8)
    inverted = gradient_rgb(h=32, w=32, invert=True)

    df = make_image_df([original, noisy, inverted])
    result = df.with_column("h", image_hash(df["img"], method="dhash")).collect()
    hashes = result.to_pydict()["h"]

    dist_similar = hamming_distance(hashes[0], hashes[1])
    dist_different = hamming_distance(hashes[0], hashes[2])
    assert dist_similar <= dist_different, (
        f"dhash: noisy gradient (dist={dist_similar}) should be <= inverted (dist={dist_different})"
    )


def test_colorhash_similar_colors_closer_than_dissimilar():
    """colorhash: a slightly tinted copy should be closer than a completely different palette."""
    original = solid_rgb(200, 50, 50, h=32, w=32)
    similar = solid_rgb(210, 60, 55, h=32, w=32)
    different = solid_rgb(50, 50, 200, h=32, w=32)

    df = make_image_df([original, similar, different])
    result = df.with_column("h", image_hash(df["img"], method="colorhash")).collect()
    hashes = result.to_pydict()["h"]

    dist_similar = hamming_distance(hashes[0], hashes[1])
    dist_different = hamming_distance(hashes[0], hashes[2])
    assert dist_similar <= dist_different, (
        f"colorhash: similar (dist={dist_similar}) should be <= different (dist={dist_different})"
    )


# ─── crop_resistant ──────────────────────────────────────────────────────────


def test_crop_resistant_small_crop_low_distance():
    """A slightly cropped image should have low absolute Hamming distance.

    crop_resistant produces a 9-segment hash (576 bits total for hash_size=8).
    A 4-pixel edge crop on a 48×48 image shifts the segments only slightly, so
    far fewer than half the bits should flip.
    """
    img = gradient_rgb(h=48, w=48)
    cropped = img[4:44, 4:44, :]
    df = make_image_df([img, cropped])
    result = df.with_column("h", image_hash(df["img"], method="crop_resistant")).collect()
    hashes = result.to_pydict()["h"]
    dist = hamming_distance(hashes[0], hashes[1])
    total_bits = len(hashes[0]) * 8
    assert dist < total_bits // 4, (
        f"crop_resistant distance {dist} should be < {total_bits // 4} for a mild 4-pixel crop"
    )


# ─── Error handling ──────────────────────────────────────────────────────────


def test_image_hash_invalid_method_raises():
    """Invalid method string raises a ValueError at call time."""
    with pytest.raises(ValueError, match="method must be one of"):
        image_hash(daft.col("img"), method="invalid_method")


def test_image_hash_invalid_hash_size_raises():
    """Non-positive hash_size raises a ValueError at call time."""
    with pytest.raises(ValueError, match="hash_size must be a positive integer"):
        image_hash(daft.col("img"), hash_size=0)


def test_image_hash_multiple_rows():
    """Verify hashing works on a batch of images."""
    images = [gradient_rgb(invert=(i % 2 == 0)) for i in range(5)]
    df = make_image_df(images)
    result = df.with_column("h", image_hash(df["img"])).collect()
    hashes = result.to_pydict()["h"]
    assert len(hashes) == 5
    assert all(h is not None and len(h) == 8 for h in hashes)
