from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_binary_length():
    # Test basic ASCII strings
    table = MicroPartition.from_pydict({"col": [b"Hello", b"World!", b"", b"Test"]})
    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": [5, 6, 0, 4]}

    # Test with special binary sequences
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"\x00\x01\x02",  # Null and control characters
                b"\xff\xfe\xfd",  # High-value bytes
                b"\x7f\x80\x81",  # Around ASCII boundary
                b"Hello\x00World",  # Embedded null
                b"\xe2\x98\x83",  # UTF-8 encoded snowman (☃)
                b"\xf0\x9f\x98\x89",  # UTF-8 encoded winking face (😉)
            ]
        }
    )
    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": [3, 3, 3, 11, 3, 4]}

    # Test with nulls and empty strings
    table = MicroPartition.from_pydict({"col": [b"Hello", None, b"", b"\x00", None, b"Test", b""]})
    result = table.eval_expression_list([col("col").binary.length()])
    assert result.to_pydict() == {"col": [5, None, 0, 1, None, 4, 0]}


def test_binary_concat():
    # Test basic ASCII concatenation
    table = MicroPartition.from_pydict(
        {"a": [b"Hello", b"Test", b"", b"End"], "b": [b" World", b"ing", b"Empty", b"!"]}
    )
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {"a": [b"Hello World", b"Testing", b"Empty", b"End!"]}

    # Test with special binary sequences
    table = MicroPartition.from_pydict(
        {
            "a": [
                b"\x00\x01",  # Null and control chars
                b"\xff\xfe",  # High-value bytes
                b"Hello\x00",  # String with null
                b"\xe2\x98",  # Partial UTF-8
                b"\xf0\x9f\x98",  # Another partial UTF-8
            ],
            "b": [
                b"\x02\x03",  # More control chars
                b"\xfd\xfc",  # More high-value bytes
                b"\x00World",  # Null and string
                b"\x83",  # Complete the UTF-8 snowman
                b"\x89",  # Complete the UTF-8 winking face
            ],
        }
    )
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {
        "a": [
            b"\x00\x01\x02\x03",  # Concatenated control chars
            b"\xff\xfe\xfd\xfc",  # Concatenated high-value bytes
            b"Hello\x00\x00World",  # String with multiple nulls
            b"\xe2\x98\x83",  # Complete UTF-8 snowman (☃)
            b"\xf0\x9f\x98\x89",  # Complete UTF-8 winking face (😉)
        ]
    }

    # Test with nulls and empty strings
    table = MicroPartition.from_pydict(
        {
            "a": [b"Hello", None, b"", b"Test", None, b"End", b""],
            "b": [b" World", b"!", None, None, b"ing", b"", b"Empty"],
        }
    )
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {"a": [b"Hello World", None, None, None, None, b"End", b"Empty"]}

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


def test_binary_concat_broadcast():
    # Test broadcasting with literal on right
    table = MicroPartition.from_pydict({"a": [b"Hello", b"Goodbye", b"Test"]})
    result = table.eval_expression_list([col("a").binary.concat(b" World!")])
    assert result.to_pydict() == {"a": [b"Hello World!", b"Goodbye World!", b"Test World!"]}

    # Test broadcasting with nulls
    table = MicroPartition.from_pydict({"a": [b"Hello", None, b"Test"]})
    result = table.eval_expression_list([col("a").binary.concat(b" World!")])
    assert result.to_pydict() == {"a": [b"Hello World!", None, b"Test World!"]}

    # Test broadcasting special binary sequences
    table = MicroPartition.from_pydict({"a": [b"\x00\x01", b"\xff\xfe", b"Hello\x00"]})
    result = table.eval_expression_list([col("a").binary.concat(b"\x02\x03")])
    assert result.to_pydict() == {"a": [b"\x00\x01\x02\x03", b"\xff\xfe\x02\x03", b"Hello\x00\x02\x03"]}


def test_binary_substr():
    # Test basic substring
    table = MicroPartition.from_pydict({"col": [b"Hello World", b"\xff\xfe\x00", b"empty", None]})
    result = table.eval_expression_list([col("col").binary.substr(1, 3)])
    assert result.to_pydict()["col"] == [b"ell", b"\xfe\x00", b"mpt", None]

    # Test with length that would exceed string length (should return truncated substring)
    table = MicroPartition.from_pydict({"col": [b"Hi", b"\xff", b"a"]})
    result = table.eval_expression_list([col("col").binary.substr(1, 3)])
    assert result.to_pydict()["col"] == [b"i", b"", b""]

    # Test with start at string length (should return empty)
    table = MicroPartition.from_pydict({"col": [b"Hi", b"\xff", b"a"]})
    result = table.eval_expression_list([col("col").binary.substr(2, 1)])
    assert result.to_pydict()["col"] == [b"", b"", b""]

    # Test with start beyond string length (should return empty)
    table = MicroPartition.from_pydict({"col": [b"Hi", b"\xff", b"a"]})
    result = table.eval_expression_list([col("col").binary.substr(3, 1)])
    assert result.to_pydict()["col"] == [b"", b"", b""]

    # Test with empty input
    table = MicroPartition.from_pydict({"col": [b""]})
    result = table.eval_expression_list([col("col").binary.substr(0, 1)])
    assert result.to_pydict()["col"] == [b""]

    # Test with zero length (should return empty)
    table = MicroPartition.from_pydict({"col": [b"Hello", b"\xff\xfe", b"test"]})
    result = table.eval_expression_list([col("col").binary.substr(1, 0)])
    assert result.to_pydict()["col"] == [b"", b"", b""]

    # Test with None length (should return rest of string)
    table = MicroPartition.from_pydict({"col": [b"Hello", b"\xff\xfe", b"test"]})
    result = table.eval_expression_list([col("col").binary.substr(1, None)])
    assert result.to_pydict()["col"] == [b"ello", b"\xfe", b"est"]

    # Test with mixed null and non-null inputs
    table = MicroPartition.from_pydict({"col": [None, b"Hi", None]})
    result = table.eval_expression_list([col("col").binary.substr(0, 1)])
    assert result.to_pydict()["col"] == [None, b"H", None]

    # # Test with computed indices
    # table = MicroPartition.from_pydict({"col": [b"Hi", b"Hello", b"Hey"]})
    # result = table.eval_expression_list([col("col").binary.substr(col("col").binary.length() - 1, 1)])
    # assert result.to_pydict()["col"] == [b"i", b"o", b"y"]
