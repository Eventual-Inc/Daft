from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import Schema


def test_read_with_empty_path():
    with pytest.raises(ValueError, match="Cannot read DataFrame from empty list of text filepaths"):
        daft.read_text([])


def test_read_with_empty_file(tmp_path):
    path = tmp_path / "empty.txt"
    path.write_text("", encoding="utf-8")

    df = daft.read_text(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("text", pa.string())]))
    assert df.to_pydict()["text"] == []


def test_read_with_basic_lines(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("hello\nworld\n", encoding="utf-8")

    df = daft.read_text(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("text", pa.string())]))
    assert df.to_pydict()["text"] == ["hello", "world"]


def test_read_with_empty_and_whitespace_lines(tmp_path):
    path = tmp_path / "lines.txt"
    path.write_text("line1\n\nline2\n   \n\t\n", encoding="utf-8")

    df = daft.read_text(str(path), skip_blank_lines=False)
    assert df.to_pydict()["text"] == ["line1", "", "line2", "   ", "\t"]

    df = daft.read_text(str(path), skip_blank_lines=True)
    assert df.to_pydict()["text"] == ["line1", "line2"]


def test_read_without_trailing_newline(tmp_path):
    path = tmp_path / "no_trailing_newline.txt"
    path.write_text("hello\nworld", encoding="utf-8")

    df = daft.read_text(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("text", pa.string())]))
    assert df.to_pydict()["text"] == ["hello", "world"]


def test_read_include_path_column(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("a1\na2\n", encoding="utf-8")
    file_b.write_text("b1\n", encoding="utf-8")

    df = daft.read_text([str(file_a), str(file_b)], file_path_column="path")
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("text", pa.string()), ("path", pa.string())]))

    data = df.to_pydict()
    assert len(data["text"]) == len(data["path"])

    rows = {(t, p) for t, p in zip(data["text"], data["path"])}
    assert rows == {("a1", f"{tmp_path}/a.txt"), ("a2", f"{tmp_path}/a.txt"), ("b1", f"{tmp_path}/b.txt")}


def test_read_with_glob_patterns(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("a1\na2\n", encoding="utf-8")
    file_b.write_text("b1\n", encoding="utf-8")

    df = daft.read_text(str(tmp_path / "*.txt"), file_path_column="path")
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("text", pa.string()), ("path", pa.string())]))

    data = df.to_pydict()
    assert len(data["text"]) == len(data["path"])

    rows = {(t, p) for t, p in zip(data["text"], data["path"])}
    assert rows == {("a1", f"{tmp_path}/a.txt"), ("a2", f"{tmp_path}/a.txt"), ("b1", f"{tmp_path}/b.txt")}


def test_read_with_hive_partitioning(tmp_path):
    dir_a = tmp_path / "key=a"
    dir_b = tmp_path / "key=b"
    dir_a.mkdir()
    dir_b.mkdir()

    (dir_a / "a.txt").write_text("a1\na2\n", encoding="utf-8")
    (dir_b / "b.txt").write_text("b1\n", encoding="utf-8")

    df = daft.read_text(str(tmp_path / "key=*/*.txt"), hive_partitioning=True, file_path_column="path")
    assert df.schema() == Schema.from_pyarrow_schema(
        pa.schema([("text", pa.string()), ("key", pa.string()), ("path", pa.string())])
    )

    data = df.to_pydict()
    rows = {(t, k, p) for t, k, p in zip(data["text"], data["key"], data["path"])}
    assert rows == {
        ("a1", "a", f"{tmp_path}/key=a/a.txt"),
        ("a2", "a", f"{tmp_path}/key=a/a.txt"),
        ("b1", "b", f"{tmp_path}/key=b/b.txt"),
    }


def test_read_with_gzip_files(tmp_path):
    def _write_gzip(path: Path, content: bytes) -> None:
        with gzip.open(path, "wb") as f:
            f.write(content)

    path = tmp_path / "compressed.txt.gz"
    _write_gzip(path, b"l1\nl2\n")

    df = daft.read_text(str(path))
    assert df.to_pydict()["text"] == ["l1", "l2"]


def test_read_with_buffer_and_chunk_size(tmp_path):
    path = tmp_path / "many_lines.txt"
    path.write_text("\n".join([f"line{i}" for i in range(1024)]) + "\n", encoding="utf-8")

    df = daft.read_text(str(path), _buffer_size=16, _chunk_size=1)

    assert df.to_pydict()["text"] == [f"line{i}" for i in range(1024)]


def test_read_with_limit_pushdown(tmp_path):
    path = tmp_path / "many_lines_1.txt"
    path.write_text("\n".join([f"line1_{i}" for i in range(1024)]) + "\n", encoding="utf-8")
    path = tmp_path / "many_lines_2.txt"
    path.write_text("\n".join([f"line2_{i}" for i in range(1024)]) + "\n", encoding="utf-8")

    df = daft.read_text(str(tmp_path)).limit(128)
    assert 128 == len(df.collect())


def test_read_with_encoding_setting(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("hello\n", encoding="utf-8")

    assert daft.read_text(str(path), encoding="UTF8").to_pydict()["text"] == ["hello"]

    with pytest.raises(ValueError, match=r"Unsupported text encoding: latin-1"):
        daft.read_text(str(path), encoding="latin-1").to_pydict()

    path = tmp_path / "bad_utf8.txt"
    path.write_bytes(b"\xff\n")

    with pytest.raises(Exception, match=r"(?i)utf-?8"):
        daft.read_text(str(path)).to_pydict()
