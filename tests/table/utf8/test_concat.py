from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "input_a,input_b,expected_result",
    [
        # Basic ASCII concatenation
        (
            ["Hello", "Test", "", "End"],
            [" World", "ing", "Empty", "!"],
            ["Hello World", "Testing", "Empty", "End!"],
        ),
        # Special UTF-8 sequences
        (
            [
                "☃",  # Snowman
                "😉",  # Winking face
                "🌈",  # Rainbow
                "Hello☃",  # String with UTF-8
                "Hello\u0000",  # String with null
            ],
            [
                "😉",  # Winking face
                "☃",  # Snowman
                "☃",  # Snowman
                "World",  # ASCII
                "\u0000World",  # Null and string
            ],
            [
                "☃😉",  # Snowman + Winking face
                "😉☃",  # Winking face + Snowman
                "🌈☃",  # Rainbow + Snowman
                "Hello☃World",  # String with UTF-8 + ASCII
                "Hello\u0000\u0000World",  # String with multiple nulls
            ],
        ),
        # Nulls and empty strings
        (
            ["Hello", None, "", "Test", None, "End", ""],
            [" World", "!", None, None, "ing", "", "Empty"],
            ["Hello World", None, None, None, None, "End", "Empty"],
        ),
        # Mixed length concatenation
        (
            ["a", "ab", "abc", "abcd"],
            ["1", "12", "123", "1234"],
            ["a1", "ab12", "abc123", "abcd1234"],
        ),
        # Empty string combinations
        (
            ["", "", "Hello", "World", ""],
            ["", "Test", "", "", "!"],
            ["", "Test", "Hello", "World", "!"],
        ),
        # Complex UTF-8 sequences
        (
            [
                "☃",  # Snowman
                "😉",  # Winking face
                "🌈",  # Rainbow
                "☃😉",  # Snowman + Winking face
            ],
            [
                "😉",  # Winking face
                "☃",  # Snowman
                "☃",  # Snowman
                "🌈",  # Rainbow
            ],
            [
                "☃😉",  # Snowman + Winking face
                "😉☃",  # Winking face + Snowman
                "🌈☃",  # Rainbow + Snowman
                "☃😉🌈",  # Snowman + Winking face + Rainbow
            ],
        ),
        # Null characters in different positions
        (
            [
                "\u0000abc",  # Leading null
                "abc\u0000",  # Trailing null
                "ab\u0000c",  # Middle null
                "\u0000ab\u0000c\u0000",  # Multiple nulls
            ],
            [
                "def\u0000",  # Trailing null
                "\u0000def",  # Leading null
                "d\u0000ef",  # Middle null
                "\u0000de\u0000f\u0000",  # Multiple nulls
            ],
            [
                "\u0000abcdef\u0000",  # Nulls at ends
                "abc\u0000\u0000def",  # Adjacent nulls
                "ab\u0000cd\u0000ef",  # Separated nulls
                "\u0000ab\u0000c\u0000\u0000de\u0000f\u0000",  # Many nulls
            ],
        ),
    ],
)
def test_utf8_concat(input_a: list[str | None], input_b: list[str | None], expected_result: list[str | None]) -> None:
    table = MicroPartition.from_pydict({"a": input_a, "b": input_b})
    result = table.eval_expression_list([col("a").str.concat(col("b"))])
    assert result.to_pydict() == {"a": expected_result}


def test_utf8_concat_large() -> None:
    # Test concatenating large strings
    large_string = "x" * 1000
    table = MicroPartition.from_pydict(
        {"a": [large_string, "small", large_string], "b": [large_string, large_string, "small"]}
    )
    result = table.eval_expression_list([col("a").str.concat(col("b"))])
    assert result.to_pydict() == {
        "a": [
            "x" * 2000,  # Two large strings concatenated
            "small" + ("x" * 1000),  # Small + large
            ("x" * 1000) + "small",  # Large + small
        ]
    }


def test_utf8_concat_very_large() -> None:
    # Test with very large string sequences
    data_a = [
        "x" * 1_000_000,  # 1MB of 'x'
        "\u0000" * 1_000_000,  # 1MB of null characters
        "Hello\u0000World" * 100_000,  # Repeated pattern
        "☃" * 333_333,  # Many snowmen
    ]
    data_b = [
        "y" * 1_000_000,  # 1MB of 'y'
        "z" * 1_000_000,  # 1MB of 'z'
        "Test\u0000Case" * 100_000,  # Different pattern
        "😉" * 250_000,  # Many winking faces
    ]
    expected = [
        ("x" * 1_000_000) + ("y" * 1_000_000),
        ("\u0000" * 1_000_000) + ("z" * 1_000_000),
        ("Hello\u0000World" * 100_000) + ("Test\u0000Case" * 100_000),
        ("☃" * 333_333) + ("😉" * 250_000),
    ]

    table = MicroPartition.from_pydict({"a": data_a, "b": data_b})
    result = table.eval_expression_list([col("a").str.concat(col("b"))])
    assert result.to_pydict() == {"a": expected}


@pytest.mark.parametrize(
    "input_data,literal,expected_result",
    [
        # Basic broadcasting
        (
            ["Hello", "Goodbye", "Test"],
            " World!",
            ["Hello World!", "Goodbye World!", "Test World!"],
        ),
        # Broadcasting with nulls
        (
            ["Hello", None, "Test"],
            " World!",
            ["Hello World!", None, "Test World!"],
        ),
        # Broadcasting with UTF-8 sequences
        (
            ["Hello", "Test", "Goodbye"],
            "☃",  # Snowman
            ["Hello☃", "Test☃", "Goodbye☃"],
        ),
        # Broadcasting with null characters
        (
            ["Hello", "Test\u0000", "\u0000World"],
            "\u0000",
            ["Hello\u0000", "Test\u0000\u0000", "\u0000World\u0000"],
        ),
        # Broadcasting with empty strings
        (
            ["", "Test", ""],
            "☃",
            ["☃", "Test☃", "☃"],
        ),
        # Broadcasting with complex UTF-8
        (
            ["Hello", "Test", "Goodbye"],
            "☃😉🌈",  # Snowman + Winking face + Rainbow
            ["Hello☃😉🌈", "Test☃😉🌈", "Goodbye☃😉🌈"],
        ),
    ],
)
def test_utf8_concat_broadcast(input_data: list[str | None], literal: str, expected_result: list[str | None]) -> None:
    # Test right-side broadcasting
    table = MicroPartition.from_pydict({"a": input_data})
    result = table.eval_expression_list([col("a").str.concat(literal)])
    assert result.to_pydict() == {"a": expected_result}

    # Test left-side broadcasting
    table = MicroPartition.from_pydict({"b": input_data})
    result = table.eval_expression_list([lit(literal).str.concat(col("b"))])
    assert result.to_pydict() == {
        "literal": [
            lit + data if data is not None else None for lit, data in zip([literal] * len(input_data), input_data)
        ]
    }


def test_utf8_concat_edge_cases() -> None:
    # Test various edge cases
    table = MicroPartition.from_pydict(
        {
            "a": [
                "",  # Empty string
                "\u0000",  # Single null character
                "Hello",  # Normal string
                None,  # Null value
                "☃",  # UTF-8 sequence
                "😉",  # Another UTF-8 sequence
            ],
            "b": [
                "",  # Empty + Empty
                "\u0000",  # Null + Null
                "",  # Normal + Empty
                None,  # Null + Null
                "😉",  # UTF-8 + UTF-8
                "☃",  # UTF-8 + UTF-8
            ],
        }
    )
    result = table.eval_expression_list([col("a").str.concat(col("b"))])
    assert result.to_pydict() == {
        "a": [
            "",  # Empty + Empty = Empty
            "\u0000\u0000",  # Null + Null = Two nulls
            "Hello",  # Normal + Empty = Normal
            None,  # Null + Null = Null
            "☃😉",  # Snowman + Winking face
            "😉☃",  # Winking face + Snowman
        ]
    }
