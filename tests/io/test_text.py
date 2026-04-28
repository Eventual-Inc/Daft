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
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string())]))
    assert df.to_pydict()["content"] == []


def test_read_with_basic_lines(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("hello\nworld\n", encoding="utf-8")

    df = daft.read_text(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string())]))
    assert df.to_pydict()["content"] == ["hello", "world"]


def test_read_with_empty_and_whitespace_lines(tmp_path):
    path = tmp_path / "lines.txt"
    path.write_text("line1\n\nline2\n   \n\t\n", encoding="utf-8")

    df = daft.read_text(str(path), skip_blank_lines=False)
    assert df.to_pydict()["content"] == ["line1", "", "line2", "   ", "\t"]

    df = daft.read_text(str(path), skip_blank_lines=True)
    assert df.to_pydict()["content"] == ["line1", "line2"]


def test_read_without_trailing_newline(tmp_path):
    path = tmp_path / "no_trailing_newline.txt"
    path.write_text("hello\nworld", encoding="utf-8")

    df = daft.read_text(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string())]))
    assert df.to_pydict()["content"] == ["hello", "world"]


def test_read_include_path_column(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("a1\na2\n", encoding="utf-8")
    file_b.write_text("b1\n", encoding="utf-8")

    df = daft.read_text([str(file_a), str(file_b)], file_path_column="path")
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string()), ("path", pa.string())]))

    data = df.to_pydict()
    assert len(data["content"]) == len(data["path"])

    rows = {(t, p) for t, p in zip(data["content"], data["path"])}
    assert rows == {("a1", f"{tmp_path}/a.txt"), ("a2", f"{tmp_path}/a.txt"), ("b1", f"{tmp_path}/b.txt")}


def test_read_with_glob_patterns(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("a1\na2\n", encoding="utf-8")
    file_b.write_text("b1\n", encoding="utf-8")

    df = daft.read_text(str(tmp_path / "*.txt"), file_path_column="path")
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string()), ("path", pa.string())]))

    data = df.to_pydict()
    assert len(data["content"]) == len(data["path"])

    rows = {(t, p) for t, p in zip(data["content"], data["path"])}
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
        pa.schema([("content", pa.string()), ("key", pa.string()), ("path", pa.string())])
    )

    data = df.to_pydict()
    rows = {(t, k, p) for t, k, p in zip(data["content"], data["key"], data["path"])}
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
    assert df.to_pydict()["content"] == ["l1", "l2"]


def test_read_with_buffer_and_chunk_size(tmp_path):
    path = tmp_path / "many_lines.txt"
    path.write_text("\n".join([f"line{i}" for i in range(1024)]) + "\n", encoding="utf-8")

    df = daft.read_text(str(path), _buffer_size=16, _chunk_size=1)

    assert df.to_pydict()["content"] == [f"line{i}" for i in range(1024)]


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

    assert daft.read_text(str(path), encoding="UTF8").to_pydict()["content"] == ["hello"]

    with pytest.raises(ValueError, match=r"Unsupported text encoding: latin-1"):
        daft.read_text(str(path), encoding="latin-1").to_pydict()

    path = tmp_path / "bad_utf8.txt"
    path.write_bytes(b"\xff\n")

    with pytest.raises(Exception, match=r"(?i)utf-?8"):
        daft.read_text(str(path)).to_pydict()


def test_read_whole_text_from_single_file(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("hello\nworld\nfoo", encoding="utf-8")

    df = daft.read_text(str(path), whole_text=True)
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string())]))
    result = df.to_pydict()
    assert result["content"] == ["hello\nworld\nfoo"]


def test_read_whole_text_from_multiple_files(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("content of file a\nwith multiple lines", encoding="utf-8")
    file_b.write_text("content of file b", encoding="utf-8")

    df = daft.read_text([str(file_a), str(file_b)], whole_text=True)
    result = df.to_pydict()
    assert len(result["content"]) == 2
    assert "content of file a\nwith multiple lines" in result["content"]
    assert "content of file b" in result["content"]


def test_read_whole_text_with_path_column(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("content a", encoding="utf-8")
    file_b.write_text("content b", encoding="utf-8")

    df = daft.read_text([str(file_a), str(file_b)], whole_text=True, file_path_column="path")
    assert df.schema() == Schema.from_pyarrow_schema(pa.schema([("content", pa.string()), ("path", pa.string())]))

    data = df.to_pydict()
    assert len(data["content"]) == 2
    assert len(data["path"]) == 2

    rows = {(t, p) for t, p in zip(data["content"], data["path"])}
    assert rows == {
        ("content a", f"{tmp_path}/a.txt"),
        ("content b", f"{tmp_path}/b.txt"),
    }


def test_read_whole_text_from_empty_file(tmp_path):
    path = tmp_path / "empty.txt"
    path.write_text("", encoding="utf-8")

    df = daft.read_text(str(path), whole_text=True, skip_blank_lines=False)
    result = df.to_pydict()
    assert result["content"] == [""]

    df = daft.read_text(str(path), whole_text=True, skip_blank_lines=True)
    result = df.to_pydict()
    assert result["content"] == []


def test_read_whole_text_with_glob_patterns(tmp_path):
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_c = tmp_path / "c.txt"
    file_d = tmp_path / "d.txt"
    file_a.write_text("content a1", encoding="utf-8")
    file_b.write_text("content b1\ncontent b2\t", encoding="utf-8")
    file_c.write_text("content c1\ncontent c2\ncontent c3\n\t", encoding="utf-8")
    file_d.write_text("", encoding="utf-8")

    df = daft.read_text(
        str(tmp_path / "*.txt"),
        skip_blank_lines=True,
        whole_text=True,
        file_path_column="path",
    )
    data = df.to_pydict()
    assert len(data["content"]) == 3
    assert len(data["path"]) == 3

    file_to_content = {p: t for p, t in zip(data["path"], data["content"])}
    assert file_to_content[str(file_a)] == "content a1"
    assert file_to_content[str(file_b)] == "content b1\ncontent b2\t"
    assert file_to_content[str(file_c)] == "content c1\ncontent c2\ncontent c3\n\t"


def test_read_whole_text_with_gzip(tmp_path):
    def _write_gzip(path: Path, content: bytes) -> None:
        with gzip.open(path, "wb") as f:
            f.write(content)

    path = tmp_path / "compressed.txt.gz"
    _write_gzip(path, b"line1\nline2\nline3")

    df = daft.read_text(str(path), whole_text=True)
    result = df.to_pydict()
    assert result["content"] == ["line1\nline2\nline3"]
