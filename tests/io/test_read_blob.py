from __future__ import annotations

import os

import pyarrow as pa
import pytest

import daft
from daft import Schema


def test_read_blob_with_empty_path():
    with pytest.raises(ValueError, match="Cannot read DataFrame from empty list of blob filepaths"):
        daft.read_blob([])


def test_read_blob_single_file(tmp_path):
    path = tmp_path / "file.bin"
    data = b"\x00\x01\x02hello blob"
    path.write_bytes(data)

    df = daft.read_blob(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(
        pa.schema([("path", pa.string()), ("size", pa.int64()), ("content", pa.binary())])
    )
    result = df.to_pydict()
    assert len(result["path"]) == 1
    assert result["path"][0].endswith("file.bin")
    assert result["size"] == [len(data)]
    assert result["content"] == [data]


def test_read_blob_glob(tmp_path):
    contents = {}
    for i in range(3):
        data = f"content-{i}".encode()
        (tmp_path / f"{i}.bin").write_bytes(data)
        contents[f"{i}.bin"] = data

    df = daft.read_blob(f"{tmp_path}/*.bin").sort("path")
    result = df.to_pydict()
    assert len(result["path"]) == 3
    for path, size, content in zip(result["path"], result["size"], result["content"]):
        name = os.path.basename(path)
        assert content == contents[name]
        assert size == len(contents[name])


def test_read_blob_multiple_paths(tmp_path):
    file_a = tmp_path / "a.bin"
    file_b = tmp_path / "b.bin"
    file_a.write_bytes(b"aaa")
    file_b.write_bytes(b"bbbb")

    df = daft.read_blob([str(file_a), str(file_b)]).sort("path")
    result = df.to_pydict()
    assert [os.path.basename(p) for p in result["path"]] == ["a.bin", "b.bin"]
    assert result["size"] == [3, 4]
    assert result["content"] == [b"aaa", b"bbbb"]


def test_read_blob_empty_file(tmp_path):
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")

    df = daft.read_blob(str(path))
    result = df.to_pydict()
    assert result["size"] == [0]
    assert result["content"] == [b""]
