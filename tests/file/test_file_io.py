from __future__ import annotations

from pathlib import Path

import pytest

from daft.file.file_io import open_file


@pytest.fixture
def temp_text_file(tmp_path: Path) -> str:
    p = tmp_path / "test.txt"
    p.write_text("hello world\nthis is a test!")
    return str(p)


@pytest.fixture
def temp_binary_file(tmp_path: Path) -> tuple[str, bytes]:
    data = b"\x00\x01\x02testbinarydata\xff"
    p = tmp_path / "test.bin"
    p.write_bytes(data)
    return str(p), data


def test_open_file_text_mode_default(temp_text_file):
    with open_file(temp_text_file) as f:
        contents = f.read()
        assert isinstance(contents, str)
        assert "hello world" in contents
        assert "this is a test!" in contents


def test_open_file_text_mode_explicit_rt(temp_text_file):
    with open_file(temp_text_file, mode="rt") as f:
        f.readline()  # should not raise
        f.seek(0)
        content = f.read()
        assert content.startswith("hello world")


def test_open_file_binary_mode(temp_binary_file):
    fname, data = temp_binary_file
    with open_file(fname, mode="rb") as f:
        chunk = f.read()
        assert chunk == data


def test_open_file_buffering_modes(temp_text_file):
    with open_file(temp_text_file, mode="rt", buffering=1) as f:
        assert f.readline().startswith("hello")
    with open_file(temp_text_file, mode="rt", buffering=8) as f:
        assert f.read(5) == "hello"


def test_open_file_text_mode_zero_buffer_raises(temp_text_file):
    with pytest.raises(ValueError):
        open_file(temp_text_file, mode="rt", buffering=0)


def test_open_file_invalid_mode(temp_text_file):
    with pytest.raises(ValueError):
        open_file(temp_text_file, mode="w")


def test_open_file_encoding_utf8(temp_text_file):
    with open(temp_text_file, "w", encoding="utf-8") as f:
        f.write("café\n")
    with open_file(temp_text_file, mode="rt", encoding="utf-8") as f:
        line = f.readline()
        assert "café" in line
