from __future__ import annotations

import pytest

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_binary_length() -> None:
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"foo",  # Basic ASCII
                None,  # Null value
                b"",  # Empty string
                b"Hello\xe2\x98\x83World",  # UTF-8 character in middle
                b"\xf0\x9f\x98\x89test",  # UTF-8 bytes at start
                b"test\xf0\x9f\x8c\x88",  # UTF-8 bytes at end
                b"\xe2\x98\x83\xf0\x9f\x98\x89\xf0\x9f\x8c\x88",  # Multiple UTF-8 sequences
                b"Hello\x00World",  # Null character
                b"\xff\xfe\xfd",  # High bytes
                b"\x00\x01\x02",  # Control characters
                b"a" * 1000,  # Long ASCII string
                b"\xff" * 1000,  # Long binary string
            ]
        }
    )
    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {
        "col": [
            3,  # "foo"
            None,  # None
            0,  # ""
            13,  # "Hello☃World" (5 + 3 + 5 = 13 bytes)
            8,  # "😉test" (4 + 4 = 8 bytes)
            8,  # "test🌈" (4 + 4 = 8 bytes)
            11,  # "☃😉🌈" (3 + 4 + 4 = 11 bytes)
            11,  # "Hello\x00World"
            3,  # "\xff\xfe\xfd"
            3,  # "\x00\x01\x02"
            1000,  # Long ASCII string
            1000,  # Long binary string
        ]
    }


@pytest.mark.parametrize(
    "input_data,expected_lengths",
    [
        # Basic ASCII strings
        ([b"hello", b"world", b"test"], [5, 5, 4]),
        # Empty strings and nulls
        ([b"", None, b"", None], [0, None, 0, None]),
        # Special binary sequences
        (
            [
                b"\x00\x01\x02",  # Control characters
                b"\xff\xfe\xfd",  # High bytes
                b"Hello\x00World",  # String with null
                b"\xe2\x98\x83",  # UTF-8 snowman
                b"\xf0\x9f\x98\x89",  # UTF-8 winking face
            ],
            [3, 3, 11, 3, 4],
        ),
        # Mixed content
        (
            [
                b"Hello\xe2\x98\x83World",  # String with UTF-8
                b"\xf0\x9f\x98\x89test",  # UTF-8 at start
                b"test\xf0\x9f\x8c\x88",  # UTF-8 at end
                b"\xe2\x98\x83\xf0\x9f\x98\x89\xf0\x9f\x8c\x88",  # Multiple UTF-8
            ],
            [13, 8, 8, 11],  # Fixed lengths for UTF-8 sequences
        ),
        # Large strings
        ([b"a" * 1000, b"\xff" * 1000, b"x" * 500], [1000, 1000, 500]),
    ],
)
def test_binary_length_parameterized(input_data: list[bytes | None], expected_lengths: list[int | None]) -> None:
    table = MicroPartition.from_pydict({"col": input_data})
    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": expected_lengths}


def test_binary_length_errors() -> None:
    # Test length with wrong number of arguments
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"], "b": [b"foo", b"bar"]})
    with pytest.raises(
        Exception, match="(?:ExpressionBinaryNamespace.)?length\\(\\) takes 1 positional argument but 2 were given"
    ):
        table.eval_expression_list([col("a").binary.length(col("b"))])
