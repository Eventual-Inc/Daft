from __future__ import annotations

import pytest

import daft
from daft.functions import try_to_binary


def test_try_to_binary_hex():
    df = daft.from_pydict({"v": ["616263", "6D", "", "61626", "zz", None]})
    result = df.select(try_to_binary(df["v"], "hex")).to_pydict()
    assert result["v"] == [b"abc", b"m", b"", None, None, None]


def test_try_to_binary_default_format_is_hex():
    df = daft.from_pydict({"v": ["68656c6c6f"]})
    result = df.select(try_to_binary(df["v"])).to_pydict()
    assert result["v"] == [b"hello"]


def test_try_to_binary_utf8():
    df = daft.from_pydict({"v": ["abc", "", "café 🔥", None]})
    result = df.select(try_to_binary(df["v"], "utf-8")).to_pydict()
    assert result["v"] == [b"abc", b"", "café 🔥".encode(), None]


@pytest.mark.parametrize("fmt", ["utf-8", "utf8", "UTF-8", "UTF8", "HEX", "Base64"])
def test_try_to_binary_format_case_insensitive(fmt):
    df = daft.from_pydict({"v": ["61", "aGk=", "hi"]})
    df.select(try_to_binary(df["v"], fmt)).to_pydict()


def test_try_to_binary_base64():
    df = daft.from_pydict({"v": ["aGVsbG8=", "", "!!!", None]})
    result = df.select(try_to_binary(df["v"], "base64")).to_pydict()
    assert result["v"] == [b"hello", b"", None, None]


def test_try_to_binary_output_dtype():
    df = daft.from_pydict({"v": ["616263"]})
    for fmt in ("hex", "utf-8", "base64"):
        assert df.select(try_to_binary(df["v"], fmt)).schema()["v"].dtype == daft.DataType.binary()


def test_try_to_binary_unsupported_format():
    df = daft.from_pydict({"v": ["616263"]})
    with pytest.raises(ValueError, match="Unsupported try_to_binary format"):
        try_to_binary(df["v"], "gzip")
