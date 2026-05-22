"""Tests for inline-bytes-backed `daft.File` rows.

An inline-bytes File carries its payload in the physical struct's `data`
field instead of a URI. Reads come straight from memory; no I/O.
"""

from __future__ import annotations

import pytest

import daft
from daft.file import File
from tests.conftest import get_tests_daft_runner_name

# --- Construction paths ---


def test_from_bytes_classmethod():
    f = File.from_bytes(b"hello world")
    with f.open() as g:
        assert g.read() == b"hello world"


def test_init_dispatch_bytes():
    f = File(b"raw bytes")
    with f.open() as g:
        assert g.read() == b"raw bytes"


def test_init_dispatch_bytearray():
    f = File(bytearray(b"raw bytearray"))
    with f.open() as g:
        assert g.read() == b"raw bytearray"


def test_init_dispatch_uri_str():
    f = File("s3://bucket/x.txt")
    assert f.path == "s3://bucket/x.txt"


def test_init_rejects_unsupported_type():
    with pytest.raises(TypeError):
        File(123)  # type: ignore[arg-type]


# --- I/O behavior ---


def test_seek_and_tell_inline():
    f = File(b"hello world")
    with f.open() as g:
        g.seek(6)
        assert g.tell() == 6
        assert g.read() == b"world"


def test_size_inline_is_byte_count():
    f = File.from_bytes(b"abcdef")
    with f.open() as g:
        assert g.size() == 6


def test_byte_size_property_inline():
    """The `byte_size` property exposes the wire-format size field."""
    f = File.from_bytes(b"abcdef")
    assert f.byte_size == 6


# --- path / name semantics ---


def test_path_is_none_for_inline():
    f = File(b"x")
    assert f.path is None


def test_name_is_none_for_inline():
    f = File(b"x")
    assert f.name is None


# --- Column-level ---


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_column_of_inline_files():
    df = daft.from_pydict(
        {
            "f": [
                File(b"row0"),
                File(b"row1"),
                File(b"row2"),
            ]
        }
    )
    rows = df.to_pydict()["f"]
    contents = []
    for f in rows:
        with f.open() as g:
            contents.append(g.read())
    assert contents == [b"row0", b"row1", b"row2"]


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_mixed_uri_and_inline_in_column():
    """URI-backed and inline-backed rows can coexist in the same column."""
    df = daft.from_pydict(
        {
            "f": [
                File("s3://bucket/example.txt"),
                File(b"local"),
            ]
        }
    )
    rows = df.to_pydict()["f"]
    assert rows[0].path == "s3://bucket/example.txt"
    assert rows[1].path is None
    with rows[1].open() as g:
        assert g.read() == b"local"


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_uri_only_column_unchanged():
    """URI-backed columns continue to work."""
    df = daft.from_pydict({"f": [File("s3://bucket/a.txt"), File("s3://bucket/b.txt")]})
    paths = [f.path for f in df.to_pydict()["f"]]
    assert paths == ["s3://bucket/a.txt", "s3://bucket/b.txt"]


@pytest.mark.skipif(get_tests_daft_runner_name() == "ray", reason="local only test")
def test_udf_over_inline_column():
    @daft.func(return_dtype=daft.DataType.binary())
    def read_first(f: File) -> bytes:
        with f.open() as g:
            return g.read()

    df = daft.from_pydict(
        {
            "f": [
                File(b"hello"),
                File(b"world"),
            ]
        }
    )
    out = df.select(read_first(daft.col("f")).alias("b")).to_pydict()
    assert out["b"] == [b"hello", b"world"]


def test_inline_column_survives_repartition():
    """Inline-bytes Files survive shuffle/repartition (Ray and native)."""
    df = daft.from_pydict(
        {
            "f": [
                File(b"hello"),
                File(b"world"),
            ]
        }
    )
    df = df.repartition(2)
    rows = df.to_pydict()["f"]
    contents = []
    for f in rows:
        with f.open() as g:
            contents.append(g.read())
    assert set(contents) == {b"hello", b"world"}
