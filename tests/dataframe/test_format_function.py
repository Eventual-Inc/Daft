from __future__ import annotations

import pytest

from daft.expressions import col
from daft.functions import format


def test_format_basic(make_df) -> None:
    """Test basic format functionality."""
    data = {"first": ["Alice", "Bob"], "last": ["Smith", "Jones"]}
    df = make_df(data).with_column("greeting", format("Hello {} {}", "first", "last"))

    expected = ["Hello Alice Smith", "Hello Bob Jones"]
    assert df.to_pydict()["greeting"] == expected


def test_format_string_vs_expression_args(make_df) -> None:
    """Test that string column names and col() expressions work the same."""
    data = {"name": ["Alice", "Bob"]}
    df = make_df(data).with_column("greeting", format("Hello {}", "name"))

    expected = ["Hello Alice", "Hello Bob"]
    assert df.to_pydict()["greeting"] == expected


def test_format_no_placeholders(make_df) -> None:
    """Test format with no placeholders returns just the literal string."""
    data = {"name": ["Alice", "Bob"]}
    df = make_df(data).with_column("static", format("Hello World"))

    expected = ["Hello World", "Hello World"]
    assert df.to_pydict()["static"] == expected


def test_format_only_placeholders(make_df) -> None:
    """Test format with only placeholders (no static text)."""
    data = {"first": ["Alice", "Bob"], "last": ["Smith", "Jones"]}
    df = make_df(data).with_column("full", format("{}{}", "first", "last"))

    expected = ["AliceSmith", "BobJones"]
    assert df.to_pydict()["full"] == expected


def test_format_ends_with_placeholder(make_df) -> None:
    """Test that parts[-1] is handled correctly when format string ends with {}."""
    data = {"name": ["Alice", "Bob"]}
    df = make_df(data).with_column("greeting", format("Hello {}", "name"))

    expected = ["Hello Alice", "Hello Bob"]
    assert df.to_pydict()["greeting"] == expected


def test_format_starts_with_placeholder(make_df) -> None:
    """Test format string that starts with a placeholder."""
    data = {"name": ["Alice", "Bob"]}
    df = make_df(data).with_column("greeting", format("{}, welcome!", "name"))

    expected = ["Alice, welcome!", "Bob, welcome!"]
    assert df.to_pydict()["greeting"] == expected


def test_format_multiple_static_parts(make_df) -> None:
    """Test format with multiple static text parts between placeholders."""
    data = {"first": ["Alice", "Bob"], "last": ["Smith", "Jones"]}
    df = make_df(data).with_column("message", format("Dear {} from {}, welcome!", "first", "last"))

    expected = ["Dear Alice from Smith, welcome!", "Dear Bob from Jones, welcome!"]
    assert df.to_pydict()["message"] == expected


def test_format_with_expressions(make_df) -> None:
    """Test format with computed expressions as arguments."""
    data = {"a": [1, 2], "b": [3, 4]}
    df = make_df(data).with_column("result", format("Sum: {}", col("a") + col("b")))

    expected = ["Sum: 4", "Sum: 6"]
    assert df.to_pydict()["result"] == expected


def test_format_placeholder_count_mismatch_too_few_args() -> None:
    """Test error when number of placeholders does not match number of arguments."""
    with pytest.raises(ValueError, match="Format string .* has 2 placeholders but 1 arguments were provided"):
        format("Hello {} {}", "name")


def test_format_empty_string_no_args(make_df) -> None:
    """Test format with empty string and no arguments returns empty string literal."""
    data = {"name": ["Alice", "Bob"]}
    df = make_df(data).with_column("empty", format(""))

    expected = ["", ""]
    assert df.to_pydict()["empty"] == expected
