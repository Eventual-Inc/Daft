"""Tests for ChatTemplateFormatter (message handling)."""

from __future__ import annotations

import io
import os
import tempfile

import pytest

from daft.ai.transformers.protocols.prompter.messages import ChatTemplateFormatter
from daft.file import File

# Check if required dependencies are available
np = pytest.importorskip("numpy")
PIL = pytest.importorskip("PIL")
from PIL import Image as PILImage


@pytest.fixture
def formatter() -> ChatTemplateFormatter:
    """Create a ChatTemplateFormatter instance."""
    return ChatTemplateFormatter()


def test_format_simple_text(formatter: ChatTemplateFormatter):
    """Test formatting a simple text message."""
    messages, images = formatter.format(("Hello, world!",))

    assert len(messages) == 1
    assert messages[0]["role"] == "user"
    assert messages[0]["content"] == [{"type": "text", "text": "Hello, world!"}]
    assert images == []


def test_format_with_system_message(formatter: ChatTemplateFormatter):
    """Test formatting with system message."""
    messages, images = formatter.format(("User prompt",), system_message="System prompt")

    assert len(messages) == 2
    assert messages[0]["role"] == "system"
    assert messages[0]["content"] == "System prompt"
    assert messages[1]["role"] == "user"


def test_format_multiple_text_parts(formatter: ChatTemplateFormatter):
    """Test formatting multiple text parts."""
    messages, images = formatter.format(("Part 1", "Part 2", "Part 3"))

    assert len(messages) == 1
    content = messages[0]["content"]
    assert len(content) == 3
    assert all(part["type"] == "text" for part in content)


def test_format_with_image_extracts_image(formatter: ChatTemplateFormatter):
    """Test formatting with image extracts it correctly."""
    img_array = np.zeros((10, 10, 3), dtype=np.uint8)

    messages, images = formatter.format(("Describe this:", img_array))

    assert len(messages) == 1
    content = messages[0]["content"]
    # Text part
    assert content[0] == {"type": "text", "text": "Describe this:"}
    # Image placeholder
    assert content[1] == {"type": "image"}
    # Actual image extracted
    assert len(images) == 1
    assert np.array_equal(images[0], img_array)


def test_format_with_image_url_keeps_inline(formatter: ChatTemplateFormatter):
    """Test formatting with image URL keeps it inline and does not extract."""
    url = "https://example.com/image.png"
    messages, images = formatter.format(("Describe this:", url))

    assert len(messages) == 1
    content = messages[0]["content"]
    assert content[0] == {"type": "text", "text": "Describe this:"}
    assert content[1] == {"type": "image", "url": url}
    assert images == []


def test_format_with_image_data_url_keeps_inline(formatter: ChatTemplateFormatter):
    """Test formatting with image data URL keeps it inline and does not extract."""
    data_url = "data:image/png;base64,AAAA"
    messages, images = formatter.format((data_url,))

    assert len(messages) == 1
    content = messages[0]["content"]
    assert content == [{"type": "image", "url": data_url}]
    assert images == []


def test_process_string_message(formatter: ChatTemplateFormatter):
    """Test basic string message processing."""
    result = formatter._process_content_part("Hello, world!")
    assert result == {"type": "text", "text": "Hello, world!"}


def test_process_empty_string(formatter: ChatTemplateFormatter):
    """Test empty string handling."""
    result = formatter._process_content_part("")
    assert result == {"type": "text", "text": ""}


def test_process_multiline_string(formatter: ChatTemplateFormatter):
    """Test multiline string handling."""
    text = "Line 1\nLine 2\nLine 3"
    result = formatter._process_content_part(text)
    assert result == {"type": "text", "text": text}


def test_process_text_bytes(formatter: ChatTemplateFormatter):
    """Test plain text bytes handling."""
    text_bytes = b"Hello, world!"
    result = formatter._process_content_part(text_bytes)

    assert result["type"] == "text"
    assert result["text"] == "Hello, world!"


def test_process_utf8_bytes(formatter: ChatTemplateFormatter):
    """Test UTF-8 encoded bytes."""
    text_bytes = "日本語テスト".encode()
    result = formatter._process_content_part(text_bytes)

    assert result["type"] == "text"
    assert result["text"] == "日本語テスト"


def test_process_image_bytes_png(formatter: ChatTemplateFormatter):
    """Test PNG image bytes handling."""
    img = PILImage.new("RGB", (10, 10), color="red")
    bio = io.BytesIO()
    img.save(bio, format="PNG")
    png_bytes = bio.getvalue()

    result = formatter._process_content_part(png_bytes)

    assert result["type"] == "image"
    assert isinstance(result["image"], PILImage.Image)


def test_process_image_bytes_jpeg(formatter: ChatTemplateFormatter):
    """Test JPEG image bytes handling."""
    img = PILImage.new("RGB", (10, 10), color="blue")
    bio = io.BytesIO()
    img.save(bio, format="JPEG")
    jpeg_bytes = bio.getvalue()

    result = formatter._process_content_part(jpeg_bytes)

    assert result["type"] == "image"
    assert isinstance(result["image"], PILImage.Image)


def test_process_numpy_image(formatter: ChatTemplateFormatter):
    """Test numpy array image handling."""
    img_array = np.zeros((10, 10, 3), dtype=np.uint8)
    img_array[:, :, 0] = 255  # Red channel

    result = formatter._process_content_part(img_array)

    assert result["type"] == "image"
    # For numpy arrays, the image is passed through directly
    assert np.array_equal(result["image"], img_array)


def test_process_text_file(formatter: ChatTemplateFormatter):
    """Test text file handling."""
    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write("File content here")
        temp_path = tmp.name

    try:
        result = formatter._process_content_part(File(temp_path))

        assert result["type"] == "text"
        assert "File content here" in result["text"]
        # Should be wrapped in file tags
        assert "<file_text_plain>" in result["text"]
        assert "</file_text_plain>" in result["text"]
    finally:
        os.unlink(temp_path)


def test_process_image_file(formatter: ChatTemplateFormatter):
    """Test image file handling."""
    img = PILImage.new("RGB", (10, 10), color="green")
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        img.save(tmp, format="PNG")
        temp_path = tmp.name

    try:
        result = formatter._process_content_part(File(temp_path))

        assert result["type"] == "image"
        assert isinstance(result["image"], PILImage.Image)
    finally:
        os.unlink(temp_path)


def test_process_html_file_as_text(formatter: ChatTemplateFormatter):
    """Test HTML file is processed as text."""
    with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w", encoding="utf-8") as tmp:
        tmp.write("<html><body>Hello</body></html>")
        temp_path = tmp.name

    try:
        result = formatter._process_content_part(File(temp_path))

        assert result["type"] == "text"
        assert "<html><body>Hello</body></html>" in result["text"]
    finally:
        os.unlink(temp_path)


def test_unsupported_type_raises_error(formatter: ChatTemplateFormatter):
    """Test that unsupported types raise ValueError."""
    with pytest.raises(ValueError, match="Unsupported content type"):
        formatter._process_content_part(12345)  # int is not supported


def test_unsupported_object_raises_error(formatter: ChatTemplateFormatter):
    """Test that arbitrary objects raise ValueError."""
    with pytest.raises(ValueError, match="Unsupported content type"):
        formatter._process_content_part({"key": "value"})  # dict is not supported


def test_is_text_mime_type_true(formatter: ChatTemplateFormatter):
    """Test text MIME type detection."""
    assert formatter._is_text_mime_type("text/plain") is True
    assert formatter._is_text_mime_type("text/html") is True
    assert formatter._is_text_mime_type("text/html; charset=utf-8") is True


def test_is_text_mime_type_false(formatter: ChatTemplateFormatter):
    """Test non-text MIME type detection."""
    assert formatter._is_text_mime_type("image/png") is False
    assert formatter._is_text_mime_type("application/json") is False
    assert formatter._is_text_mime_type("audio/mp3") is False


def test_build_text_from_bytes(formatter: ChatTemplateFormatter):
    """Test text building from bytes."""
    result = formatter._build_text_from_bytes(b"Hello")
    assert result == {"type": "text", "text": "Hello"}


def test_build_text_from_bytes_invalid_utf8(formatter: ChatTemplateFormatter):
    """Test handling of invalid UTF-8 bytes."""
    invalid_bytes = b"\xff\xfe"
    result = formatter._build_text_from_bytes(invalid_bytes)

    # Should use replacement characters
    assert result["type"] == "text"
    assert "�" in result["text"] or len(result["text"]) > 0
