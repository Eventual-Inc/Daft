from __future__ import annotations

import datetime
import gzip
from pathlib import Path

import pyarrow as pa
import pytest

import daft
from daft import Schema

EXPECTED_SCHEMA = pa.schema(
    [
        ("content", pa.large_binary()),
        ("size", pa.int64()),
        ("last_modified", pa.timestamp("us", tz="Etc/UTC")),
    ]
)


def test_read_with_empty_path():
    with pytest.raises(ValueError, match="Cannot read DataFrame from empty list of blob filepaths"):
        daft.read_blob([])


def test_read_with_empty_file(tmp_path: Path):
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")

    df = daft.read_blob(str(path))
    assert df.schema() == Schema.from_pyarrow_schema(EXPECTED_SCHEMA)
    data = df.to_pydict()
    assert data["content"] == [b""]
    assert data["size"] == [0]
    assert "last_modified" in data


def test_read_basic_bytes(tmp_path: Path):
    content = b"hello\x00world"
    path = tmp_path / "sample.bin"
    path.write_bytes(content)

    df = daft.read_blob(str(path))
    data = df.to_pydict()
    assert data["content"] == [content]
    assert data["size"] == [len(content)]


def test_read_non_utf8_bytes(tmp_path: Path):
    """read_blob must round-trip arbitrary bytes without applying any encoding."""
    content = b"\xff\xfe\x00\x01\x80\x81"
    path = tmp_path / "binary.bin"
    path.write_bytes(content)

    df = daft.read_blob(str(path))
    assert df.to_pydict()["content"] == [content]


def test_read_no_auto_decompress_gz(tmp_path: Path):
    """Unlike read_text, read_blob must return raw .gz bytes (no transparent decompression)."""
    payload = b"decompressed payload"
    path = tmp_path / "file.gz"
    with gzip.open(path, "wb") as f:
        f.write(payload)
    raw_bytes = path.read_bytes()

    df = daft.read_blob(str(path))
    result = df.to_pydict()["content"][0]
    assert result == raw_bytes
    assert result[:2] == b"\x1f\x8b"
    assert result != payload


def test_read_include_path_column(tmp_path: Path):
    (tmp_path / "a.bin").write_bytes(b"A")
    (tmp_path / "b.bin").write_bytes(b"B")

    df = daft.read_blob(str(tmp_path / "*.bin"), file_path_column="path")
    data = df.to_pydict()
    assert set(data.keys()) == {"content", "size", "last_modified", "path"}
    assert len(data["content"]) == 2
    by_content = {c: p for c, p in zip(data["content"], data["path"])}
    assert by_content[b"A"].endswith("a.bin")
    assert by_content[b"B"].endswith("b.bin")


def test_read_with_glob_patterns(tmp_path: Path):
    (tmp_path / "a.bin").write_bytes(b"1")
    (tmp_path / "b.bin").write_bytes(b"22")
    (tmp_path / "c.txt").write_bytes(b"ignored")

    df = daft.read_blob(str(tmp_path / "*.bin"))
    sizes = sorted(df.to_pydict()["size"])
    assert sizes == [1, 2]


def test_read_with_hive_partitioning(tmp_path: Path):
    (tmp_path / "key=a").mkdir()
    (tmp_path / "key=b").mkdir()
    (tmp_path / "key=a" / "f.bin").write_bytes(b"AAA")
    (tmp_path / "key=b" / "f.bin").write_bytes(b"BB")

    df = daft.read_blob(str(tmp_path / "**" / "*.bin"), hive_partitioning=True)
    data = df.to_pydict()
    assert set(data.keys()) >= {"content", "size", "last_modified", "key"}
    by_key = {k: (c, s) for k, c, s in zip(data["key"], data["content"], data["size"])}
    assert by_key["a"] == (b"AAA", 3)
    assert by_key["b"] == (b"BB", 2)


def test_size_matches_listing(tmp_path: Path):
    """Selecting only `size` should return correct byte lengths from listing metadata."""
    (tmp_path / "x.bin").write_bytes(b"1234")
    (tmp_path / "y.bin").write_bytes(b"1234567")

    df = daft.read_blob(str(tmp_path / "*.bin")).select("size")
    assert sorted(df.to_pydict()["size"]) == [4, 7]


def test_last_modified_column_present(tmp_path: Path):
    path = tmp_path / "f.bin"
    path.write_bytes(b"hi")

    df = daft.read_blob(str(path))
    data = df.to_pydict()
    assert "last_modified" in data
    ts = data["last_modified"][0]
    if ts is not None:
        assert isinstance(ts, datetime.datetime)
        assert ts.tzinfo is not None


def test_read_with_buffer_size(tmp_path: Path):
    content = b"a" * (1 << 20)  # 1 MiB
    path = tmp_path / "big.bin"
    path.write_bytes(content)

    df = daft.read_blob(str(path), _buffer_size=16)
    assert df.to_pydict()["content"] == [content]


def test_read_limit(tmp_path: Path):
    for i in range(5):
        (tmp_path / f"f{i}.bin").write_bytes(bytes([i]))

    df = daft.read_blob(str(tmp_path / "*.bin")).limit(1)
    assert len(df.collect()) == 1


def test_read_large_file(tmp_path: Path):
    import os

    content = os.urandom(1 << 20)
    path = tmp_path / "rand.bin"
    path.write_bytes(content)

    df = daft.read_blob(str(path))
    data = df.to_pydict()
    assert data["content"] == [content]
    assert data["size"] == [len(content)]
