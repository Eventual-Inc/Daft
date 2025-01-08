from __future__ import annotations

import pytest

from daft.expressions import col
from daft.table import MicroPartition


@pytest.mark.parametrize(
    "input_a,input_b,expected_result",
    [
        # Basic ASCII concatenation
        (
            [b"Hello", b"Test", b"", b"End"],
            [b" World", b"ing", b"Empty", b"!"],
            [b"Hello World", b"Testing", b"Empty", b"End!"],
        ),
        # Special binary sequences
        (
            [
                b"\x00\x01",  # Null and control chars
                b"\xff\xfe",  # High-value bytes
                b"Hello\x00",  # String with null
                b"\xe2\x98",  # Partial UTF-8
                b"\xf0\x9f\x98",  # Another partial UTF-8
            ],
            [
                b"\x02\x03",  # More control chars
                b"\xfd\xfc",  # More high-value bytes
                b"\x00World",  # Null and string
                b"\x83",  # Complete the UTF-8 snowman
                b"\x89",  # Complete the UTF-8 winking face
            ],
            [
                b"\x00\x01\x02\x03",  # Concatenated control chars
                b"\xff\xfe\xfd\xfc",  # Concatenated high-value bytes
                b"Hello\x00\x00World",  # String with multiple nulls
                b"\xe2\x98\x83",  # Complete UTF-8 snowman (☃)
                b"\xf0\x9f\x98\x89",  # Complete UTF-8 winking face (😉)
            ],
        ),
        # Nulls and empty strings
        (
            [b"Hello", None, b"", b"Test", None, b"End", b""],
            [b" World", b"!", None, None, b"ing", b"", b"Empty"],
            [b"Hello World", None, None, None, None, b"End", b"Empty"],
        ),
    ],
)
def test_binary_concat(
    input_a: list[bytes | None], input_b: list[bytes | None], expected_result: list[bytes | None]
) -> None:
    table = MicroPartition.from_pydict({"a": input_a, "b": input_b})
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {"a": expected_result}


def test_binary_concat_large() -> None:
    # Test concatenating large binary strings
    large_binary = b"x" * 1000
    table = MicroPartition.from_pydict(
        {"a": [large_binary, b"small", large_binary], "b": [large_binary, large_binary, b"small"]}
    )
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {
        "a": [
            b"x" * 2000,  # Two large binaries concatenated
            b"small" + (b"x" * 1000),  # Small + large
            (b"x" * 1000) + b"small",  # Large + small
        ]
    }


@pytest.mark.parametrize(
    "input_data,literal,expected_result",
    [
        # Basic broadcasting
        (
            [b"Hello", b"Goodbye", b"Test"],
            b" World!",
            [b"Hello World!", b"Goodbye World!", b"Test World!"],
        ),
        # Broadcasting with nulls
        (
            [b"Hello", None, b"Test"],
            b" World!",
            [b"Hello World!", None, b"Test World!"],
        ),
        # Broadcasting with special sequences
        (
            [b"\x00\x01", b"\xff\xfe", b"Hello\x00"],
            b"\x02\x03",
            [b"\x00\x01\x02\x03", b"\xff\xfe\x02\x03", b"Hello\x00\x02\x03"],
        ),
    ],
)
def test_binary_concat_broadcast(
    input_data: list[bytes | None], literal: bytes, expected_result: list[bytes | None]
) -> None:
    table = MicroPartition.from_pydict({"a": input_data})
    result = table.eval_expression_list([col("a").binary.concat(literal)])
    assert result.to_pydict() == {"a": expected_result}
