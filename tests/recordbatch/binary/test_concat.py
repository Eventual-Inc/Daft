from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


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
        # Mixed length concatenation
        (
            [b"a", b"ab", b"abc", b"abcd"],
            [b"1", b"12", b"123", b"1234"],
            [b"a1", b"ab12", b"abc123", b"abcd1234"],
        ),
        # Empty string combinations
        (
            [b"", b"", b"Hello", b"World", b""],
            [b"", b"Test", b"", b"", b"!"],
            [b"", b"Test", b"Hello", b"World", b"!"],
        ),
        # Complex UTF-8 sequences
        (
            [
                b"\xe2\x98\x83",  # Snowman
                b"\xf0\x9f\x98\x89",  # Winking face
                b"\xf0\x9f\x8c\x88",  # Rainbow
                b"\xe2\x98\x83\xf0\x9f\x98\x89",  # Snowman + Winking face
            ],
            [
                b"\xf0\x9f\x98\x89",  # Winking face
                b"\xe2\x98\x83",  # Snowman
                b"\xe2\x98\x83",  # Snowman
                b"\xf0\x9f\x8c\x88",  # Rainbow
            ],
            [
                b"\xe2\x98\x83\xf0\x9f\x98\x89",  # Snowman + Winking face
                b"\xf0\x9f\x98\x89\xe2\x98\x83",  # Winking face + Snowman
                b"\xf0\x9f\x8c\x88\xe2\x98\x83",  # Rainbow + Snowman
                b"\xe2\x98\x83\xf0\x9f\x98\x89\xf0\x9f\x8c\x88",  # Snowman + Winking face + Rainbow
            ],
        ),
        # Zero bytes in different positions
        (
            [
                b"\x00abc",  # Leading zero
                b"abc\x00",  # Trailing zero
                b"ab\x00c",  # Middle zero
                b"\x00ab\x00c\x00",  # Multiple zeros
            ],
            [
                b"def\x00",  # Trailing zero
                b"\x00def",  # Leading zero
                b"d\x00ef",  # Middle zero
                b"\x00de\x00f\x00",  # Multiple zeros
            ],
            [
                b"\x00abcdef\x00",  # Zeros at ends
                b"abc\x00\x00def",  # Adjacent zeros
                b"ab\x00cd\x00ef",  # Separated zeros
                b"\x00ab\x00c\x00\x00de\x00f\x00",  # Many zeros
            ],
        ),
    ],
)
def test_binary_concat(
    input_a: list[bytes | None], input_b: list[bytes | None], expected_result: list[bytes | None]
) -> None:
    table = MicroPartition.from_pydict({"a": input_a, "b": input_b})
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {"a": expected_result}


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
        # Broadcasting with empty strings
        (
            [b"", b"Test", b""],
            b"\xff\xfe",
            [b"\xff\xfe", b"Test\xff\xfe", b"\xff\xfe"],
        ),
        # Broadcasting with UTF-8
        (
            [b"Hello", b"Test", b"Goodbye"],
            b"\xe2\x98\x83",  # Snowman
            [b"Hello\xe2\x98\x83", b"Test\xe2\x98\x83", b"Goodbye\xe2\x98\x83"],
        ),
        # Broadcasting with zero bytes
        (
            [b"Hello", b"Test\x00", b"\x00World"],
            b"\x00",
            [b"Hello\x00", b"Test\x00\x00", b"\x00World\x00"],
        ),
        # Broadcasting with literal None
        (
            [b"Hello", None, b"Test", b""],
            None,
            [None, None, None, None],  # Any concat with None should result in None
        ),
    ],
)
def test_binary_concat_broadcast(
    input_data: list[bytes | None], literal: bytes | None, expected_result: list[bytes | None]
) -> None:
    # Test right-side broadcasting
    table = MicroPartition.from_pydict({"a": input_data})
    result = table.eval_expression_list([col("a").binary.concat(literal)])
    assert result.to_pydict() == {"a": expected_result}

    # Test left-side broadcasting
    table = MicroPartition.from_pydict({"b": input_data})
    result = table.eval_expression_list([lit(literal).binary.concat(col("b"))])
    if literal is None:
        # When literal is None, all results should be None
        assert result.to_pydict() == {"literal": [None] * len(input_data)}
    else:
        assert result.to_pydict() == {
            "literal": [
                lit + data if data is not None else None for lit, data in zip([literal] * len(input_data), input_data)
            ]
        }


def test_binary_concat_edge_cases() -> None:
    # Test various edge cases
    table = MicroPartition.from_pydict(
        {
            "a": [
                b"",  # Empty string
                b"\x00",  # Single null byte
                b"\xff",  # Single high byte
                b"Hello",  # Normal string
                None,  # Null value
                b"\xe2\x98\x83",  # UTF-8 sequence
                b"\xf0\x9f\x98\x89",  # Another UTF-8 sequence
                b"\x80\x81\x82",  # Binary sequence
                b"\xff\xff\xff",  # High bytes
            ],
            "b": [
                b"",  # Empty + Empty
                b"\x00",  # Null + Null
                b"\x00",  # High + Null
                b"",  # Normal + Empty
                None,  # Null + Null
                b"\xf0\x9f\x98\x89",  # UTF-8 + UTF-8
                b"\xe2\x98\x83",  # UTF-8 + UTF-8
                b"\x83\x84\x85",  # Binary + Binary
                b"\xfe\xfe\xfe",  # High bytes + High bytes
            ],
        }
    )
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {
        "a": [
            b"",  # Empty + Empty = Empty
            b"\x00\x00",  # Null + Null = Two nulls
            b"\xff\x00",  # High + Null = High then null
            b"Hello",  # Normal + Empty = Normal
            None,  # Null + Null = Null
            b"\xe2\x98\x83\xf0\x9f\x98\x89",  # Snowman + Winking face
            b"\xf0\x9f\x98\x89\xe2\x98\x83",  # Winking face + Snowman
            b"\x80\x81\x82\x83\x84\x85",  # Binary sequence concatenation
            b"\xff\xff\xff\xfe\xfe\xfe",  # High bytes concatenation
        ]
    }


def test_binary_concat_errors() -> None:
    # Test concat with incompatible type (string)
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"], "b": ["foo", "bar"]})
    with pytest.raises(Exception, match="Expects inputs to concat to be binary, but received a#Binary and b#Utf8"):
        table.eval_expression_list([col("a").binary.concat(col("b"))])

    # Test concat with incompatible type (integer)
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"], "b": [1, 2]})
    with pytest.raises(Exception, match="Expects inputs to concat to be binary, but received a#Binary and b#Int64"):
        table.eval_expression_list([col("a").binary.concat(col("b"))])

    # Test concat with incompatible type (float)
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"], "b": [1.0, 2.0]})
    with pytest.raises(Exception, match="Expects inputs to concat to be binary, but received a#Binary and b#Float64"):
        table.eval_expression_list([col("a").binary.concat(col("b"))])

    # Test concat with incompatible type (boolean)
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"], "b": [True, False]})
    with pytest.raises(Exception, match="Expects inputs to concat to be binary, but received a#Binary and b#Boolean"):
        table.eval_expression_list([col("a").binary.concat(col("b"))])

    # Test concat with wrong number of arguments
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"], "b": [b"foo", b"bar"], "c": [b"test", b"data"]})
    with pytest.raises(
        Exception, match="(?:ExpressionBinaryNamespace.)?concat\\(\\) takes 2 positional arguments but 3 were given"
    ):
        table.eval_expression_list([col("a").binary.concat(col("b"), col("c"))])

    # Test concat with no arguments
    table = MicroPartition.from_pydict({"a": [b"hello", b"world"]})
    with pytest.raises(
        Exception, match="(?:ExpressionBinaryNamespace.)?concat\\(\\) missing 1 required positional argument: 'other'"
    ):
        table.eval_expression_list([col("a").binary.concat()])
