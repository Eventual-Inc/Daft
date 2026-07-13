from __future__ import annotations

import importlib
import io
import random
import struct
from pathlib import Path

import pytest

import daft
from daft import DataType as dt
from daft.functions import file, file_exists, file_path, file_size
from tests.conftest import get_tests_daft_runner_name


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


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_can_convert_string_to_file_type():
    df = daft.from_pydict({"paths": ["./some_file.txt"]})
    assert df.schema() == daft.Schema.from_pydict({"paths": dt.string()})

    df = df.select(file(df["paths"]))

    assert df.schema() == daft.Schema.from_pydict({"paths": dt.file()})


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


def test_to_tempfile_larger_data_uses_bounded_copy_buffer(tmp_path: Path, monkeypatch):
    file_module = importlib.import_module("daft.file.file")
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(data)
    copy_lengths: list[int] = []
    original_copyfileobj = file_module.shutil.copyfileobj

    def track_copyfileobj(fsrc, fdst, length=0):
        copy_lengths.append(length)
        return original_copyfileobj(fsrc, fdst, length=length)

    monkeypatch.setattr(file_module.shutil, "copyfileobj", track_copyfileobj)

    file = daft.File(str(temp_file.absolute()))
    with file.to_tempfile() as f:
        assert f.read() == data

    assert copy_lengths == [file_module.BUFFER_COPY]


def test_to_tempfile_remote():
    file = daft.File("https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/README.rst")

    with file.to_tempfile() as temp_file:
        text_wrapper = io.TextIOWrapper(temp_file)
        first_line = text_wrapper.readline()
        assert first_line == "|Banner|\n"


def test_file_size(tmp_path: Path):
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(data)
    file = daft.File(str(temp_file.absolute()))
    assert file.size() == 2048


def test_filesize_expr(tmp_path: Path):
    data = bytes([random.randint(0, 255) for _ in range(2048)])
    temp_file = tmp_path / "test_file.bin"
    temp_file.write_bytes(data)

    df = daft.from_pydict({"file": [str(temp_file.absolute())]})

    res = df.select(file_size(file(df["file"]))).to_pydict()["file"][0]
    assert res == 2048


def test_file_exists(tmp_path: Path):
    existing_file = tmp_path / "exists.bin"
    existing_file.write_bytes(b"data")
    missing_file = tmp_path / "missing.bin"

    assert daft.File(str(existing_file.absolute())).exists() is True
    assert daft.File(str(missing_file.absolute())).exists() is False


def test_open_missing_file_raises(tmp_path: Path):
    missing_file = tmp_path / "missing.bin"
    file = daft.File(str(missing_file.absolute()))

    with pytest.raises(FileNotFoundError, match="does not exist"):
        file.open()


def test_missing_file_mime_type_falls_back_to_extension(tmp_path: Path):
    assert daft.File(str(tmp_path / "missing.mp4")).mime_type() == "video/mp4"
    assert daft.File(str(tmp_path / "missing.hdf5")).mime_type() == "application/vnd.hdfgroup.hdf5"
    assert daft.VideoFile(str(tmp_path / "missing.mp4")).exists() is False


def test_to_tempfile_missing_file_raises_from_open(tmp_path: Path):
    missing_file = tmp_path / "missing.bin"
    file = daft.File(str(missing_file.absolute()))

    with pytest.raises(FileNotFoundError, match="does not exist"):
        file.to_tempfile()


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_to_tempfile_uses_open_existence_check(tmp_path: Path, monkeypatch):
    temp_file = tmp_path / "exists.bin"
    temp_file.write_bytes(b"data")
    file = daft.File(str(temp_file.absolute()))
    original_exists = file.exists
    calls = 0

    def track_exists():
        nonlocal calls
        calls += 1
        return original_exists()

    monkeypatch.setattr(file, "exists", track_exists)

    with file.to_tempfile() as tmp:
        assert tmp.read() == b"data"

    assert calls == 1


def test_file_exists_expr(tmp_path: Path):
    existing_file = tmp_path / "exists.bin"
    existing_file.write_bytes(b"data")
    missing_file = tmp_path / "missing.bin"

    df = daft.from_pydict(
        {
            "file": [
                str(existing_file.absolute()),
                str(missing_file.absolute()),
            ]
        }
    )

    result = df.select(file_exists(file(df["file"]))).to_pydict()["file"]
    assert result == [True, False]


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


def test_file_path_property(tmp_path: Path):
    """Test that the path property returns the full file path."""
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("hello world")
    file_path = str(temp_file.absolute())

    file = daft.File(file_path)
    assert file.path == file_path


def test_file_name_property_local_path(tmp_path: Path):
    """Test that the name property returns just the filename for local paths."""
    temp_file = tmp_path / "test_file.txt"
    temp_file.write_text("hello world")

    file = daft.File(str(temp_file.absolute()))
    assert file.name == "test_file.txt"


def test_file_name_property_with_subdirs(tmp_path: Path):
    """Test that the name property returns just the filename even with subdirectories."""
    subdir = tmp_path / "subdir" / "nested"
    subdir.mkdir(parents=True)
    temp_file = subdir / "data.json"
    temp_file.write_text("{}")

    file = daft.File(str(temp_file.absolute()))
    assert file.name == "data.json"


def test_file_path_and_name_with_url():
    """Test path and name properties with a URL."""
    url = "https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/README.rst"
    file = daft.File(url)

    assert file.path == url
    assert file.name == "README.rst"


@pytest.mark.parametrize(
    "url,expected_name",
    [
        ("s3://bucket/path/to/file.csv", "file.csv"),
        ("s3://bucket/file.parquet", "file.parquet"),
        ("https://example.com/data.json", "data.json"),
        ("file:///path/to/document.pdf", "document.pdf"),
        ("/local/path/image.png", "image.png"),
        ("simple_file.txt", "simple_file.txt"),
    ],
)
def test_file_name_property_various_formats(url, expected_name):
    """Test that the name property correctly extracts filenames from various URL/path formats."""
    file = daft.File(url)
    assert file.name == expected_name
    assert file.path == url


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_path_function():
    """Test file_path() extracts the URL from a File column."""
    paths = ["s3://bucket/file1.csv", "/local/path/file2.txt", "https://example.com/file3.json"]
    df = daft.from_pydict({"path": paths})
    df = df.select(file(df["path"]).alias("file"))
    df = df.with_column("extracted_path", file_path(daft.col("file")))
    result = df.to_pydict()
    assert result["extracted_path"] == paths


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_path_enables_string_operations():
    """Test that file_path() output can be used with string expressions like endswith."""
    paths = ["/data/report.pdf", "/data/image.png", "/data/notes.pdf"]
    df = daft.from_pydict({"path": paths})
    df = df.select(file(df["path"]).alias("file"))
    df = df.where(file_path(daft.col("file")).endswith(".pdf"))
    result = df.to_pydict()
    assert len(result["file"]) == 2


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_path_expression_method():
    """Test that .file_path() works as an expression method."""
    paths = ["/data/report.pdf", "/data/image.png"]
    df = daft.from_pydict({"path": paths})
    df = df.select(file(df["path"]).alias("file"))
    df = df.with_column("extracted_path", daft.col("file").file_path())
    result = df.to_pydict()
    assert result["extracted_path"] == paths


def test_file_position_and_size_properties():
    f = daft.File("s3://bucket/blob", position=100, size=50)
    assert f.position == 100
    assert f._inner.size() == 50


def test_file_position_and_size_default_to_none():
    f = daft.File("s3://bucket/blob")
    assert f.position is None
    assert f._inner.size() is None


def test_file_offset_and_length_backwards_compat():
    # `offset`/`length` are deprecated aliases for `position`/`size`, kept for
    # backwards compatibility. They emit a DeprecationWarning but still work.
    with pytest.warns(DeprecationWarning):
        f = daft.File("s3://bucket/blob", offset=100, length=50)
    with pytest.warns(DeprecationWarning):
        assert f.offset == 100
    with pytest.warns(DeprecationWarning):
        assert f.length == 50

    # New names map to the same underlying values.
    assert f.position == 100
    assert f._inner.size() == 50


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_byte_range_read(tmp_path: Path):
    data = b"0123456789abcdef"
    temp_file = tmp_path / "blob.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()), position=4, size=6)
    assert f.size() == 6
    with f.open() as fh:
        result = fh.read()
    assert result == b"456789"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_byte_range_open_skips_existence_preflight(tmp_path: Path, monkeypatch):
    data = b"0123456789abcdef"
    temp_file = tmp_path / "blob.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()), position=4, size=6)

    def fail_exists():
        raise AssertionError("ranged open should not preflight existence")

    monkeypatch.setattr(f, "exists", fail_exists)

    with f.open() as fh:
        result = fh.read()
    assert result == b"456789"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_byte_range_read_in_udf(tmp_path: Path):
    data = b"AAABBBCCC"
    temp_file = tmp_path / "blob.bin"
    temp_file.write_bytes(data)

    @daft.func
    def read_bytes(file: daft.File) -> bytes:
        with file.open() as f:
            return f.read()

    df = daft.from_pydict({"file": [daft.File(str(temp_file.absolute()), position=3, size=3)]})
    df = df.select(read_bytes(df["file"]).alias("data"))
    assert df.to_pydict()["data"] == [b"BBB"]


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_file_partial_range_raises(tmp_path: Path):
    temp_file = tmp_path / "blob.bin"
    temp_file.write_bytes(b"some data")
    path = str(temp_file.absolute())

    f_position_only = daft.File(path, position=10)
    with pytest.raises(Exception):
        with f_position_only.open() as fh:
            fh.read()

    f_size_only = daft.File(path, size=10)
    with pytest.raises(Exception):
        with f_size_only.open() as fh:
            fh.read()


# ── buffer_size tests ──


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
@pytest.mark.parametrize("buffer_size", [64, 4096, 65536, None])
def test_open_with_buffer_size_reads_correctly(tmp_path: Path, buffer_size):
    data = b"hello world" * 100
    temp_file = tmp_path / "buf_test.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()))
    with f.open(buffer_size=buffer_size) as fh:
        result = fh.read()
    assert result == data


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_mime_type_with_small_buffer(tmp_path: Path):
    content = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100
    temp_file = tmp_path / "img.png"
    temp_file.write_bytes(content)

    f = daft.File(str(temp_file.absolute()))
    assert f.mime_type() == "image/png"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_size_with_small_buffer(tmp_path: Path):
    data = bytes(range(256)) * 4
    temp_file = tmp_path / "sized.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()))
    assert f.size() == 1024


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_read_seek_roundtrip_after_gil_fix(tmp_path: Path):
    """Basic read/seek still works after py.detach() changes."""
    data = b"abcdefghij"
    temp_file = tmp_path / "roundtrip.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()))
    with f.open() as fh:
        assert fh.read(3) == b"abc"
        assert fh.tell() == 3
        fh.seek(0)
        assert fh.tell() == 0
        assert fh.read() == data
        fh.seek(5)
        assert fh.read(3) == b"fgh"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_seek_end_works(tmp_path: Path):
    """SeekFrom::End calls block_within_async_context — verify it still works."""
    data = b"0123456789"
    temp_file = tmp_path / "seek_end.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()))
    with f.open() as fh:
        fh.seek(0, 2)  # seek to end
        pos = fh.tell()
        assert pos == len(data)


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_multiple_files_concurrent_read(tmp_path: Path):
    """Multiple files opened and read from threads — exercises GIL release under concurrency."""
    import threading

    files = []
    for i in range(8):
        p = tmp_path / f"file_{i}.bin"
        p.write_bytes(bytes(range(256)) * 4)
        files.append(str(p.absolute()))

    results = {}
    errors = []

    def read_file(path, idx):
        try:
            f = daft.File(path)
            with f.open() as fh:
                data = fh.read()
                results[idx] = len(data)
        except Exception as e:
            errors.append((idx, e))

    threads = [threading.Thread(target=read_file, args=(p, i)) for i, p in enumerate(files)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, f"Errors during concurrent read: {errors}"
    assert len(results) == 8
    assert all(v == 1024 for v in results.values())


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_read_after_eof_returns_empty(tmp_path: Path):
    """Reading at EOF should return empty bytes, not error — cursor preserved."""
    data = b"short"
    temp_file = tmp_path / "eof.bin"
    temp_file.write_bytes(data)

    f = daft.File(str(temp_file.absolute()))
    with f.open() as fh:
        fh.read()  # read all
        again = fh.read()  # read at EOF
        assert again == b""
        # file should still be usable
        fh.seek(0)
        assert fh.read() == data
