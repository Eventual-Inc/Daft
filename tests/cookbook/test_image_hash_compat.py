"""Bit-exact compatibility tests against the Python imagehash library.

Skipped automatically when `imagehash` is not installed.
"""

from __future__ import annotations

import pytest

from daft.functions import image_hash
from tests.cookbook.conftest import checkerboard, colorful_rgb, gradient_rgb, make_image_df, solid_rgb

imagehash = pytest.importorskip("imagehash", reason="imagehash not installed")
from PIL import Image as PIL_Image

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _daft_hash_bytes(arr, method: str, hash_size: int = 8, binbits: int = 3) -> bytes:
    df = make_image_df([arr])
    result = df.with_column("h", image_hash(df["img"], method=method, hash_size=hash_size, binbits=binbits)).collect()
    return result.to_pydict()["h"][0]


def _imagehash_bits(arr, method: str, hash_size: int = 8, binbits: int = 3) -> bytes:
    """Return hash bytes from the Python imagehash library (MSB-first per byte)."""
    pil = PIL_Image.fromarray(arr)
    if method == "phash":
        h = imagehash.phash(pil, hash_size=hash_size)
    elif method == "phash_simple":
        h = imagehash.phash_simple(pil, hash_size=hash_size)
    elif method == "dhash":
        h = imagehash.dhash(pil, hash_size=hash_size)
    elif method == "dhash_vertical":
        h = imagehash.dhash_vertical(pil, hash_size=hash_size)
    elif method == "ahash":
        h = imagehash.average_hash(pil, hash_size=hash_size)
    elif method == "whash":
        h = imagehash.whash(pil, hash_size=hash_size)
    elif method == "colorhash":
        h = imagehash.colorhash(pil, binbits=binbits)
    else:
        raise ValueError(f"unsupported method: {method}")
    # imagehash stores bits as a flat bool array; pack MSB-first per byte
    bits = h.hash.flatten()
    n_bytes = (len(bits) + 7) // 8
    result = bytearray(n_bytes)
    for i, bit in enumerate(bits):
        if bit:
            result[i // 8] |= 1 << (7 - (i % 8))
    return bytes(result)


# ─── phash / dhash / ahash ────────────────────────────────────────────────────


@pytest.mark.parametrize("method", ["phash", "dhash", "ahash"])
@pytest.mark.parametrize(
    "img_fn",
    [
        lambda: gradient_rgb(h=32, w=32),
        lambda: checkerboard(h=32, w=32, cell=4),
        lambda: solid_rgb(128, 64, 200, h=32, w=32),
    ],
    ids=["gradient", "checkerboard", "solid"],
)
def test_matches_imagehash_library(method, img_fn):
    """Daft perceptual hashes must match the Python imagehash library bit-for-bit."""
    img = img_fn()
    daft_bytes = _daft_hash_bytes(img, method)
    ref_bytes = _imagehash_bits(img, method)
    assert daft_bytes == ref_bytes, f"{method}: Daft hash {daft_bytes.hex()} != imagehash {ref_bytes.hex()}"


# ─── whash ────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "img_fn",
    [
        lambda: gradient_rgb(h=32, w=32),
        lambda: checkerboard(h=32, w=32, cell=4),
    ],
    ids=["gradient", "checkerboard"],
)
def test_whash_matches_imagehash_library(img_fn):
    """Whash must match imagehash.whash bit-for-bit on real-world images.

    Constant-color images are excluded: after DC removal the reconstructed image
    is all-zeros, so the hash depends entirely on sub-epsilon floating-point
    residuals (~1e-17) that differ between pywt's C code and Rust f64 arithmetic.
    That edge case is irrelevant in practice.
    """
    img = img_fn()
    daft_bytes = _daft_hash_bytes(img, "whash")
    ref_bytes = _imagehash_bits(img, "whash")
    assert daft_bytes == ref_bytes, f"whash: Daft hash {daft_bytes.hex()} != imagehash {ref_bytes.hex()}"


# ─── phash_simple ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "img_fn",
    [
        lambda: gradient_rgb(h=32, w=32),
        lambda: checkerboard(h=32, w=32, cell=4),
        lambda: solid_rgb(128, 64, 200, h=32, w=32),
    ],
    ids=["gradient", "checkerboard", "solid"],
)
def test_phash_simple_matches_imagehash_library(img_fn):
    """phash_simple must match imagehash.phash_simple bit-for-bit."""
    img = img_fn()
    daft_bytes = _daft_hash_bytes(img, "phash_simple")
    ref_bytes = _imagehash_bits(img, "phash_simple")
    assert daft_bytes == ref_bytes, f"phash_simple: Daft {daft_bytes.hex()} != imagehash {ref_bytes.hex()}"


# ─── dhash_vertical ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "img_fn",
    [
        lambda: gradient_rgb(h=32, w=32),
        lambda: checkerboard(h=32, w=32, cell=4),
        lambda: solid_rgb(128, 64, 200, h=32, w=32),
    ],
    ids=["gradient", "checkerboard", "solid"],
)
def test_dhash_vertical_matches_imagehash_library(img_fn):
    """dhash_vertical must match imagehash.dhash_vertical bit-for-bit."""
    img = img_fn()
    daft_bytes = _daft_hash_bytes(img, "dhash_vertical")
    ref_bytes = _imagehash_bits(img, "dhash_vertical")
    assert daft_bytes == ref_bytes, f"dhash_vertical: Daft {daft_bytes.hex()} != imagehash {ref_bytes.hex()}"


# ─── colorhash ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("binbits", [2, 3, 4])
@pytest.mark.parametrize(
    "img_fn",
    [
        lambda: gradient_rgb(h=32, w=32),
        lambda: checkerboard(h=32, w=32, cell=4),
        lambda: solid_rgb(128, 64, 200, h=32, w=32),
        lambda: colorful_rgb(h=32, w=32),
    ],
    ids=["gradient", "checkerboard", "solid", "colorful"],
)
def test_colorhash_matches_imagehash_library(binbits, img_fn):
    """Colorhash must match imagehash.colorhash bit-for-bit across binbits and image types."""
    img = img_fn()
    daft_bytes = _daft_hash_bytes(img, "colorhash", binbits=binbits)
    ref_bytes = _imagehash_bits(img, "colorhash", binbits=binbits)
    assert daft_bytes == ref_bytes, (
        f"colorhash (binbits={binbits}): Daft {daft_bytes.hex()} != imagehash {ref_bytes.hex()}"
    )
