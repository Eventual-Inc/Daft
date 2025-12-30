"""Tests for guess_mime_type scalar expression."""

from __future__ import annotations

import struct

import pytest

import daft
from daft.functions import guess_mime_type


@pytest.mark.parametrize(
    "bytes_data,expected_mime",
    [
        # PNG
        (b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR", "image/png"),
        # JPEG
        (b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01", "image/jpeg"),
        # GIF
        (b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff", "image/gif"),
        # PDF
        (b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n", "application/pdf"),
        # ZIP
        (b"PK\x03\x04\x0a\x00\x00\x00\x00\x00\x00\x00", "application/zip"),
        # MP3 with ID3 tag
        (b"ID3\x03\x00\x00\x00\x00\x00\x00\xff\xfb\x90\x44", "audio/mpeg"),
        # MP3 without ID3 tag
        (b"\xff\xfb\x90\x44" + b"\x00" * 12, "audio/mpeg"),
        # OGG
        (b"OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00", "audio/ogg"),
        # HTML with DOCTYPE
        (b"<!DOCTYPE html><html><head></head>", "text/html"),
        # HTML lowercase
        (b"<html><head></head><body></body>", "text/html"),
        # HTML uppercase
        (b"<HTML><HEAD></HEAD><BODY></BODY>", "text/html"),
    ],
)
def test_guess_mime_type_basic(bytes_data, expected_mime):
    """Test basic MIME type detection for various formats."""
    df = daft.from_pydict({"data": [bytes_data]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == [expected_mime]


def test_guess_mime_type_wav():
    """Test WAV file detection."""
    # Create a minimal WAV file header
    wav_data = (
        b"RIFF"
        + struct.pack("<I", 36)
        + b"WAVE"
        + b"fmt "
        + struct.pack("<I", 16)
        + struct.pack("<HHIIHH", 1, 1, 8000, 8000, 1, 8)
        + b"data"
        + struct.pack("<I", 0)
    )
    df = daft.from_pydict({"data": [wav_data]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == ["audio/wav"]


def test_guess_mime_type_webp():
    """Test WEBP file detection."""
    # WEBP has RIFF header followed by WEBP at offset 8
    webp_data = b"RIFF\x00\x00\x00\x00WEBP" + b"\x00" * 6
    df = daft.from_pydict({"data": [webp_data]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == ["image/webp"]


def test_guess_mime_type_mp4():
    """Test MP4 file detection."""
    # MP4 has ftyp at offset 4
    mp4_data = b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"
    df = daft.from_pydict({"data": [mp4_data]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == ["video/mp4"]


def test_guess_mime_type_mpeg():
    """Test MPEG file detection."""
    mpeg_data = b"\x00\x00\x01\xBA" + b"\x00" * 12
    df = daft.from_pydict({"data": [mpeg_data]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == ["video/mpeg"]


def test_guess_mime_type_null():
    """Test that None is returned for unknown formats."""
    df = daft.from_pydict({"data": [b"unknown format"]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == [None]


def test_guess_mime_type_empty():
    """Test empty byte array."""
    df = daft.from_pydict({"data": [b""]})
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == [None]


def test_guess_mime_type_multiple_rows():
    """Test with multiple rows of different types."""
    df = daft.from_pydict(
        {
            "data": [
                b"\x89PNG\r\n\x1a\n",
                b"\xff\xd8\xff\xe0",
                b"%PDF-1.7",
                b"unknown",
                None,
            ]
        }
    )
    result = df.select(guess_mime_type(df["data"])).collect()
    assert result.to_pydict()["data"] == [
        "image/png",
        "image/jpeg",
        "application/pdf",
        None,
        None,
    ]


def test_guess_mime_type_with_fill_null():
    """Test combining with fill_null for default values."""
    df = daft.from_pydict({"data": [b"\x89PNG\r\n\x1a\n", b"unknown"]})
    result = df.select(
        guess_mime_type(df["data"]).fill_null("application/octet-stream").alias("mime")
    ).collect()
    assert result.to_pydict()["mime"] == ["image/png", "application/octet-stream"]


def test_guess_mime_type_column_name():
    """Test that column name is preserved."""
    df = daft.from_pydict({"bytes_col": [b"\x89PNG\r\n\x1a\n"]})
    result = df.select(guess_mime_type(df["bytes_col"])).collect()
    assert "bytes_col" in result.column_names


def test_guess_mime_type_aliasing():
    """Test aliasing the result."""
    df = daft.from_pydict({"data": [b"\x89PNG\r\n\x1a\n"]})
    result = df.select(guess_mime_type(df["data"]).alias("mime_type")).collect()
    assert "mime_type" in result.column_names
    assert result.to_pydict()["mime_type"] == ["image/png"]
