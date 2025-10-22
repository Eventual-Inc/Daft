from __future__ import annotations

import io
import random
import struct
from pathlib import Path

import pytest

import daft
from daft import DataType as dt
from daft.functions import file, file_size
from tests.conftest import get_tests_daft_runner_name


def test_bytes_file_is_readable_and_seekable():
    data = b"hello world"
    f = daft.File(data)
    assert f.seekable()
    assert f.readable()
    assert not f.isatty()
    assert not f.writable()


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_path_file_is_readable_and_seekable(tmp_path: Path):
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("hello world")
    f = daft.File(str(temp_file.absolute()))
    assert f.seekable()
    assert f.readable()
    assert not f.isatty()
    assert not f.writable()


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_path_to_file(tmp_path: Path):
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("hello world")
    file = daft.File(str(temp_file.absolute()))
    with file.open() as f:
        data = f.read()
        assert data == b"hello world"
        f.seek(0)
        data = f.read(5)
        assert data == b"hello"
        f.seek(0)
        data = f.read()
        assert data == b"hello world"


def test_bytes_to_file():
    data = b"hello world"
    file = daft.File(data)
    with file.open() as f:
        data = f.read(1)
        assert data == b"h"
        f.seek(0)
        data = f.read()
        assert data == b"hello world"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_can_convert_string_to_file_type():
    df = daft.from_pydict({"paths": ["./some_file.txt"]})
    assert df.schema() == daft.Schema.from_pydict({"paths": dt.string()})

    df = df.select(file(df["paths"]))

    assert df.schema() == daft.Schema.from_pydict({"paths": dt.file()})


def test_can_convert_bytes_to_file_type():
    df = daft.from_pydict({"data": [b"hello world"]})
    assert df.schema() == daft.Schema.from_pydict({"data": dt.binary()})

    df = df.select(file(df["data"]))
    assert df.schema() == daft.Schema.from_pydict({"data": dt.file()})


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_can_open_local_file(tmp_path: Path):
    # Create a file in the temporary directory
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("test content")

    df = daft.from_pydict({"path": [str(temp_file.absolute())]})
    df = df.select(file(df["path"]))

    @daft.func
    def read_text(file: daft.File) -> str:
        with file.open() as f:
            return f.read().decode("utf-8")

    df = df.select(read_text(df["path"]).alias("text"))
    assert df.to_pydict()["text"] == ["test content"]


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_can_open_local_image_with_pil(tmp_path: Path):
    import numpy as np
    from PIL import Image

    # Create a simple red image
    img_array = np.zeros((100, 100, 3), dtype=np.uint8)
    img_array[:, :, 0] = 255  # Red channel
    img = Image.fromarray(img_array)

    # Save to temp file
    temp_file = tmp_path / "test_image.png"
    img.save(temp_file)

    df = daft.from_pydict({"path": [str(temp_file.absolute())]})

    df = df.select(file(df["path"]))

    @daft.func(return_dtype=dt.bool())
    def open_with_pil(file: daft.File):
        from PIL import Image

        with file.open() as f:
            img = Image.open(f)
            return img.getpixel((0, 0))[0] == 255

    df = df.select(open_with_pil(df["path"]).alias("is_red_image"))
    assert df.to_pydict()["is_red_image"] == [True]


def test_can_open_in_memory_image_with_pil():
    import numpy as np
    from PIL import Image

    img_array = np.zeros((100, 100, 3), dtype=np.uint8)
    img_array[:, :, 2] = 255
    img = Image.fromarray(img_array)

    # Save to bytes buffer
    img_buffer = io.BytesIO()
    img.save(img_buffer, format="PNG")
    img_bytes = img_buffer.getvalue()

    df = daft.from_pydict({"data": [img_bytes]})

    df = df.select(file(df["data"]))

    @daft.func(return_dtype=dt.bool())
    def open_with_pil(file: daft.File):
        from PIL import Image

        with file.open() as f:
            img = Image.open(f)
            return img.getpixel((0, 0))[2] == 255

    df = df.select(open_with_pil(df["data"]).alias("is_blue_image"))
    assert df.to_pydict()["is_blue_image"] == [True]


def test_large_binary_file_handling():
    large_data = b"\x00" * 1024 * 1024
    df = daft.from_pydict({"data": [large_data]})
    df = df.select(file(df["data"]))

    @daft.func
    def get_size(file: daft.File) -> int:
        return file.size()

    @daft.func
    def sample(file: daft.File) -> bytes:
        with file.open() as f:
            f.seek(1024 * 512)  # Middle
            return f.read(10)

    df = df.select(get_size(df["data"]).alias("size"), sample(df["data"]).alias("sample"))

    results = df.to_pydict()
    assert results["size"][0] == 1024 * 1024
    assert results["sample"][0] == b"\x00" * 10


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_large_binary_file_handling_with_file(tmp_path: Path):
    large_data = b"\x00" * 1024 * 1024  # 1MB of zeros
    binary_file = tmp_path / "large_binary.dat"
    binary_file.write_bytes(large_data)

    df = daft.from_pydict({"path": [str(binary_file.absolute())]})
    df = df.select(file(df["path"]))

    @daft.func
    def get_size(file: daft.File) -> int:
        return file.size()

    @daft.func
    def sample(file: daft.File) -> bytes:
        with file.open() as f:
            f.seek(1024 * 512)  # Middle
            return f.read(10)

    df = df.select(get_size(df["path"]).alias("size"), sample(df["path"]).alias("sample"))

    results = df.to_pydict()
    assert results["size"][0] == 1024 * 1024
    assert results["sample"][0] == b"\x00" * 10


def test_compatibility_with_json():
    import json

    json_data = '{"name": "Alice", "skills": ["Python", "Rust"]}'

    df = daft.from_pydict({"data": [json_data.encode()]})
    df = df.select(file(df["data"]))

    @daft.func()
    def read_with_json(file: daft.File) -> str:
        with file.open() as f:
            data = json.load(f)
            return data["skills"][0]

    df = df.select(read_with_json(df["data"]).alias("skill"))
    assert df.to_pydict()["skill"] == ["Python"]


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_compatibility_with_json_file(tmp_path: Path):
    import json

    json_data = '{"name": "Alice", "skills": ["Python", "Rust"]}'
    json_file = tmp_path / "data.json"
    json_file.write_text(json_data)

    df = daft.from_pydict({"path": [str(json_file.absolute())]})
    df = df.select(file(df["path"]))

    @daft.func()
    def read_with_json(file: daft.File) -> str:
        with file.open() as f:
            data = json.load(f)
            return data["skills"][0]

    df = df.select(read_with_json(df["path"]).alias("skill"))
    assert df.to_pydict()["skill"] == ["Python"]


def test_to_tempfile(tmp_path: Path):
    temp_file = tmp_path / "test_file.txt"

    temp_file.write_text("test content")

    file = daft.File(str(temp_file.absolute()))

    with file.to_tempfile() as temp_file:
        assert temp_file.read() == b"test content"


def test_to_tempfile_larger_data(tmp_path: Path):
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(data)

    file = daft.File(str(temp_file.absolute()))

    with file.to_tempfile() as f:
        assert f.read() == data


def test_to_tempfile_remote():
    file = daft.File("https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/README.rst")

    with file.to_tempfile() as temp_file:
        text_wrapper = io.TextIOWrapper(temp_file)
        first_line = text_wrapper.readline()
        assert first_line == "|Banner|\n"


def test_to_tempfile_from_bytes():
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    file = daft.File(data)
    with file.to_tempfile() as temp_file:
        assert temp_file.read() == data


def test_file_size(tmp_path: Path):
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(data)
    file = daft.File(str(temp_file.absolute()))
    assert file.size() == 2048


def test_file_size_from_bytes():
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    file = daft.File(data)
    assert file.size() == 2048


def test_filesize_expr(tmp_path: Path):
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(data)

    df = daft.from_pydict({"file": [str(temp_file.absolute())]})

    res = df.select(file_size(file(df["file"]))).to_pydict()["file"][0]
    assert res == 2048


def test_filesize_expr_binary():
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    df = daft.from_pydict({"file": [data]})

    res = df.select(file_size(file(df["file"]))).to_pydict()["file"][0]
    assert res == 2048


@pytest.mark.parametrize(
    "content, expected_mimetype",
    [
        (b"\x89PNG\r\n\x1a\n", "image/png"),
        (b"\xff\xd8\xff", "image/jpeg"),
        (b"GIF89a", "image/gif"),
        (b"RIFF\x00\x00\x00\x00WEBP", "image/webp"),
        (b"%PDF-1.5", "application/pdf"),
        (b"PK\x03\x04", "application/zip"),
        (b"ID3", "audio/mpeg"),
        (b"\xff\xfb\x90", "audio/mpeg"),
        (b"RIFF\x00\x00\x00\x00WAVE", "audio/wav"),
        (b"OggS", "audio/ogg"),
        (b"\x00\x00\x00\x18ftypmp42", "video/mp4"),
        (b"\x00\x00\x01\xba\x21", "video/mpeg"),
        (b"random bytes that won't match", "application/octet-stream"),
    ],
)
def test_file_mimetype_from_bytes(content, expected_mimetype):
    file = daft.File(content)
    mimetype = file.mime_type()

    assert mimetype == expected_mimetype


@pytest.mark.parametrize(
    "file_info",
    [
        # (extension, content_generator, expected_mimetype)
        ("jpg", lambda: b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01", "image/jpeg"),
        ("png", lambda: b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01", "image/png"),
        (
            "gif",
            lambda: b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\xff\xff\xff\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00\x3b",
            "image/gif",
        ),
        ("pdf", lambda: b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n", "application/pdf"),
        ("mp3", lambda: b"ID3\x03\x00\x00\x00\x00\x00\x00" + b"\xff\xfb\x90\x44" + b"\x00" * 32, "audio/mpeg"),
        (
            "wav",
            lambda: b"RIFF"
            + struct.pack("<I", 36)
            + b"WAVE"
            + b"fmt "
            + struct.pack("<I", 16)
            + struct.pack("<HHIIHH", 1, 1, 8000, 8000, 1, 8)
            + b"data"
            + struct.pack("<I", 0),
            "audio/wav",
        ),
        (
            "ogg",
            lambda: b"OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
            "audio/ogg",
        ),
        (
            "mp4",
            lambda: b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom" + b"\x00\x00\x00\x08free\x00\x00\x00\x08mdat",
            "video/mp4",
        ),
        (
            "zip",
            lambda: b"PK\x03\x04\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            "application/zip",
        ),
        ("html", lambda: b"<!DOCTYPE html><html><head></head><body></body></html>", "text/html"),
    ],
)
def test_file_mimetype_with_real_files(tmp_path, file_info):
    extension, content_generator, expected_mimetype = file_info

    temp_file = tmp_path / f"test_file.{extension}"
    temp_file.write_bytes(content_generator())

    file = daft.File(str(temp_file.absolute()))
    mimetype = file.mime_type()
    assert mimetype == expected_mimetype
