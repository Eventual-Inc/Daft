from __future__ import annotations

import pytest

from daft.expressions import col
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "input_data,expected_lengths",
    [
        # Basic ASCII strings
        (
            ["Hello", "World!", "", "Test"],
            [5, 6, 0, 4],
        ),
        # Special UTF-8 sequences
        (
            [
                "☃",  # UTF-8 encoded snowman
                "😉",  # UTF-8 encoded winking face
                "🌈",  # UTF-8 encoded rainbow
                "Hello☃World",  # Mixed ASCII and UTF-8
                "☃😉🌈",  # Multiple UTF-8 characters
                "Hello\u0000World",  # String with null character
            ],
            [1, 1, 1, 11, 3, 11],
        ),
        # Nulls and empty strings
        (
            ["Hello", None, "", "\u0000", None, "Test", ""],
            [5, None, 0, 1, None, 4, 0],
        ),
        # Very large strings
        (
            ["x" * 1000, "y" * 10000, "z" * 100000],
            [1000, 10000, 100000],
        ),
        # Mixed strings with different sizes
        (
            [
                "a",  # Single character
                "ab",  # Two characters
                "abc",  # Three characters
                "☃",  # Single UTF-8 character
                "☃☃",  # Two UTF-8 characters
                "☃☃☃",  # Three UTF-8 characters
            ],
            [1, 2, 3, 1, 2, 3],
        ),
        # Strings with repeated patterns
        (
            [
                "\u0000" * 5,  # Repeated null characters
                "ab" * 5,  # Repeated ASCII pattern
                "☃" * 5,  # Repeated UTF-8 snowman
                "😉" * 5,  # Repeated UTF-8 winking face
            ],
            [5, 10, 5, 5],
        ),
        # Edge cases with single characters
        (
            [
                "\u0000",  # Null character
                "\u0001",  # Start of heading
                "\u001f",  # Unit separator
                " ",  # Space
                "\u007f",  # Delete
                "☃",  # Snowman
                "😉",  # Winking face
            ],
            [1, 1, 1, 1, 1, 1, 1],
        ),
        # Complex UTF-8 sequences
        (
            [
                "☃",  # Snowman
                "😉",  # Winking face
                "☃😉",  # Snowman + Winking face
                "🌈",  # Rainbow
                "🌈☃",  # Rainbow + Snowman
                "☃🌈😉",  # Snowman + Rainbow + Winking face
            ],
            [1, 1, 2, 1, 2, 3],
        ),
        # Mixed content lengths
        (
            [
                "Hello☃World",  # ASCII + UTF-8 + ASCII
                "\u0000Hello\u0000World\u0000",  # Null-separated
                "☃Hello☃World☃",  # UTF-8-separated
                "Hello😉World",  # ASCII + UTF-8 + ASCII
            ],
            [11, 13, 13, 11],
        ),
    ],
)
def test_utf8_length(input_data: list[str | None], expected_lengths: list[int | None]) -> None:
    table = MicroPartition.from_pydict({"col": input_data})
    result = table.eval_expression_list([col("col").str.length()])
    assert result.to_pydict() == {"col": expected_lengths}


def test_utf8_length_large_sequences() -> None:
    # Test with very large string sequences
    large_data = [
        "x" * 1_000_000,  # 1MB of 'x'
        "\u0000" * 1_000_000,  # 1MB of null characters
        "Hello\u0000World" * 100_000,  # Repeated pattern with null
        "☃" * 333_333,  # Many snowmen
        None,  # Null value
        "",  # Empty string
    ]
    expected_lengths = [
        1_000_000,
        1_000_000,
        1_100_000,  # 11 chars * 100_000
        333_333,  # 1 char * 333_333
        None,
        0,
    ]

    table = MicroPartition.from_pydict({"col": large_data})
    result = table.eval_expression_list([col("col").str.length()])
    assert result.to_pydict() == {"col": expected_lengths}
