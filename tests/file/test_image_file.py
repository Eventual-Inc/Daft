from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
from PIL import Image

import daft
from daft import DataType as dt
from daft.datatype import MediaType
from daft.functions import file, image_file, image_file_metadata, decode_image_file
from tests.conftest import get_tests_daft_runner_name


@pytest.fixture
def sample_png(tmp_path: Path) -> Path:
    img = Image.fromarray(np.zeros((50, 80, 3), dtype=np.uint8))
    p = tmp_path / "sample.png"
    img.save(p)
    return p


@pytest.fixture
def sample_jpeg(tmp_path: Path) -> Path:
    img = Image.fromarray(np.ones((30, 40, 3), dtype=np.uint8) * 128)
    p = tmp_path / "sample.jpg"
    img.save(p)
    return p


# ── standalone ImageFile tests ──


def test_imagefile_standalone(sample_png: Path):
    f = daft.ImageFile(str(sample_png))
    assert f.path == str(sample_png)
    assert f.name == "sample.png"


def test_imagefile_metadata(sample_png: Path):
    f = daft.ImageFile(str(sample_png))
    meta = f.metadata()
    assert meta["width"] == 80
    assert meta["height"] == 50
    assert meta["format"] == "PNG"
    assert meta["mode"] == "RGB"


def test_imagefile_metadata_jpeg(sample_jpeg: Path):
    f = daft.ImageFile(str(sample_jpeg))
    meta = f.metadata()
    assert meta["width"] == 40
    assert meta["height"] == 30
    assert meta["format"] == "JPEG"
    assert meta["mode"] == "RGB"


def test_imagefile_decode(sample_png: Path):
    f = daft.ImageFile(str(sample_png))
    img = f.decode()
    assert isinstance(img, Image.Image)
    assert img.size == (80, 50)
    assert img.mode == "RGB"


def test_imagefile_decode_with_mode(sample_png: Path):
    f = daft.ImageFile(str(sample_png))
    img = f.decode(mode="L")
    assert img.mode == "L"
    assert img.size == (80, 50)


# ── File.is_image / File.as_image ──


def test_file_is_image(sample_png: Path):
    f = daft.File(str(sample_png))
    assert f.is_image()
    assert not f.is_video()
    assert not f.is_audio()


def test_file_as_image(sample_png: Path):
    f = daft.File(str(sample_png))
    img_file = f.as_image()
    assert isinstance(img_file, daft.ImageFile)
    meta = img_file.metadata()
    assert meta["width"] == 80


def test_file_as_image_rejects_non_image(tmp_path: Path):
    txt = tmp_path / "test.txt"
    txt.write_text("hello")
    f = daft.File(str(txt))
    with pytest.raises(ValueError, match="not an image file"):
        f.as_image()


# ── DataType / MediaType ──


def test_media_type_image():
    mt = MediaType.image()
    assert mt._media_type is not None


def test_datatype_file_image():
    dtype = dt.file(MediaType.image())
    assert dtype is not None


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_image_file_dtype(sample_png: Path):
    df = daft.from_pydict({"paths": [str(sample_png)]})
    df = df.select(image_file(df["paths"]))
    field = df.schema()["paths"]
    assert field.dtype == dt.file(MediaType.image())


# ── Expression-level functions ──


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_image_file_from_string(sample_png: Path):
    df = daft.from_pydict({"paths": [str(sample_png)]})
    df = df.select(image_file(df["paths"]))
    result = df.to_pydict()
    assert len(result["paths"]) == 1


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_image_file_verify_ok(sample_png: Path):
    df = daft.from_pydict({"paths": [str(sample_png)]})
    df = df.select(image_file(df["paths"], verify=True))
    result = df.to_pydict()
    assert len(result["paths"]) == 1


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_image_file_verify_rejects_non_image(tmp_path: Path):
    txt = tmp_path / "not_image.txt"
    txt.write_text("hello world")
    df = daft.from_pydict({"paths": [str(txt)]})
    df = df.select(image_file(df["paths"], verify=True))
    with pytest.raises(Exception, match="[Ii]nvalid image file"):
        df.to_pydict()


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_image_file_metadata_expr(sample_png: Path):
    df = daft.from_pydict({"paths": [str(sample_png)]})
    df = df.with_column("img", image_file(df["paths"]))
    df = df.with_column("meta", df["img"].image_file_metadata())
    result = df.to_pydict()
    meta = result["meta"][0]
    assert meta["width"] == 80
    assert meta["height"] == 50
    assert meta["format"] == "PNG"
    assert meta["mode"] == "RGB"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_decode_image_file_expr(sample_png: Path):
    df = daft.from_pydict({"paths": [str(sample_png)]})
    df = df.with_column("img", image_file(df["paths"]))
    df = df.with_column("decoded", df["img"].decode_image_file())
    result = df.to_pydict()
    assert result["decoded"][0] is not None


# ── from File(Unknown) cast ──


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_image_file_from_unknown_file(sample_png: Path):
    df = daft.from_pydict({"paths": [str(sample_png)]})
    df = df.select(file(df["paths"]))
    df = df.select(image_file(df["paths"]))
    field = df.schema()["paths"]
    assert field.dtype == dt.file(MediaType.image())


# ── dependency guard ──


def test_imagefile_init_without_pillow():
    with patch("daft.dependencies.pil_image.module_available", return_value=False):
        with pytest.raises(ImportError, match="pillow"):
            daft.ImageFile("dummy.png")


def test_file_as_image_without_pillow(sample_png: Path):
    f = daft.File(str(sample_png))
    with patch("daft.dependencies.pil_image.module_available", return_value=False):
        with pytest.raises(ImportError, match="pillow"):
            f.as_image()
