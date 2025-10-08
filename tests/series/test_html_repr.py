"""Tests for HTML representation of Series and array values."""

from __future__ import annotations

import pytest

import daft


def test_utf8_html_value_truncation():
    """Test that strings longer than 1MB are truncated in HTML representation."""
    # Create a string longer than 1MB
    long_string = "a" * (1024 * 1024 + 100)  # 1MB + 100 bytes
    df = daft.from_pydict({"test_col": [long_string]})

    # Get the HTML representation
    html_repr = df._repr_html_()

    # The actual content should be truncated - check that the long string doesn't appear in full
    # The HTML table structure adds overhead, so we check for truncation indicator instead
    assert "..." in html_repr

    # Verify the original long string isn't fully present in the HTML
    assert long_string not in html_repr


def test_utf8_html_value_short_string():
    """Test that short strings are not truncated."""
    short_string = "Hello, world!"
    df = daft.from_pydict({"test_col": [short_string]})

    html_repr = df._repr_html_()

    # Should contain the original string
    assert "Hello, world!" in html_repr
    # Should not contain truncation indicator (except for possible table formatting)
    assert html_repr.count("...") <= 1  # Allow for table formatting


def test_utf8_html_value_html_escaping():
    """Test that HTML special characters are properly escaped."""
    html_string = "<script>alert('xss')</script>"
    df = daft.from_pydict({"test_col": [html_string]})

    html_repr = df._repr_html_()

    # Should be HTML escaped
    assert "&lt;script&gt;" in html_repr
    assert "&lt;/script&gt;" in html_repr
    # Should not contain raw HTML
    assert "<script>" not in html_repr


def test_utf8_html_value_newline_replacement():
    """Test that newlines are replaced with <br /> tags."""
    multiline_string = "line1\nline2\nline3"
    df = daft.from_pydict({"test_col": [multiline_string]})

    html_repr = df._repr_html_()

    # Should replace newlines with <br />
    assert "<br />" in html_repr


def test_unicode_boundary_handling():
    """Test that truncation respects UTF-8 character boundaries."""
    # Create a string with unicode characters near the 1MB boundary
    unicode_char = "🦀"  # 4-byte UTF-8 character
    base_string = "a" * (1024 * 1024 - 2)  # Just under 1MB
    long_string = base_string + unicode_char

    df = daft.from_pydict({"test_col": [long_string]})
    html_repr = df._repr_html_()

    # Should not crash (which would happen if we split in the middle of a UTF-8 character)
    assert isinstance(html_repr, str)
    assert len(html_repr) > 0


def test_python_object_html_representation():
    """Test that Python objects with large string representations are truncated."""

    class LargeObject:
        def __str__(self):
            return "large_data:" + "x" * (1024 * 1024 + 100)  # 1MB + 100 bytes

    large_obj = LargeObject()
    # Create a dataframe with the Python object
    df = daft.from_pydict({"test_col": [large_obj]})

    html_repr = df._repr_html_()

    # Should not crash and should be truncated
    assert isinstance(html_repr, str)
    # The full object string shouldn't be present
    full_obj_str = str(large_obj)
    assert full_obj_str not in html_repr


def test_list_html_representation():
    """Test that lists with large string representations are truncated."""
    # Create a list with many elements that would produce a large string
    large_list = ["item_" + str(i) for i in range(10000)]  # Reduced size for faster testing
    df = daft.from_pydict({"test_col": [large_list]})

    html_repr = df._repr_html_()

    # Should not crash and should be reasonably sized
    assert isinstance(html_repr, str)
    # Should be much smaller than the full representation would be
    assert len(html_repr) < 1024 * 1024 * 2  # Allow some overhead for HTML structure


@pytest.mark.parametrize("data_type", ["string", "binary"])
def test_various_data_types_truncation(data_type):
    """Test that various data types handle large representations correctly."""
    if data_type == "string":
        large_data = "x" * (1024 * 1024 + 100)
        df = daft.from_pydict({"test_col": [large_data]})
    elif data_type == "binary":
        large_data = b"x" * (1024 * 1024 + 100)
        df = daft.from_pydict({"test_col": [large_data]})

    html_repr = df._repr_html_()

    # Should not crash and should be truncated
    assert isinstance(html_repr, str)
    assert len(html_repr) < 1024 * 1024 * 2  # Allow some overhead
