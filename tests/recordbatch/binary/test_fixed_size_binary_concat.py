from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataType
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    "input_data1,input_data2,expected_result,size1,size2",
    [
        # Basic concatenation
        ([b"abc", b"def", b"ghi"], [b"xyz", b"uvw", b"rst"], [b"abcxyz", b"defuvw", b"ghirst"], 3, 3),
        # With nulls
        ([b"ab", None, b"cd"], [b"xy", b"uv", None], [b"abxy", None, None], 2, 2),
        # Special sequences
        (
            [b"\x00\x01", b"\xff\xfe", b"\xe2\x98"],
            [b"\x99\x00", b"\x01\xff", b"\xfe\xe2"],
            [b"\x00\x01\x99\x00", b"\xff\xfe\x01\xff", b"\xe2\x98\xfe\xe2"],
            2,
            2,
        ),
        # Complex UTF-8 sequences
        (
            [b"\xe2\x98", b"\xf0\x9f", b"\xf0\x9f"],  # Partial UTF-8 sequences
            [b"\x83\x00", b"\x98\x89", b"\x8c\x88"],  # Complete the sequences
            [b"\xe2\x98\x83\x00", b"\xf0\x9f\x98\x89", b"\xf0\x9f\x8c\x88"],  # Complete UTF-8 characters
            2,
            2,
        ),
        # Mixed length concatenation
        (
            [b"a", b"b", b"c", b"d"],  # Single bytes
            [b"12", b"34", b"56", b"78"],  # Two bytes
            [b"a12", b"b34", b"c56", b"d78"],  # Three bytes result
            1,
            2,
        ),
        # Zero bytes in different positions
        (
            [b"\x00a", b"a\x00", b"\x00\x00"],  # Zeros in different positions
            [b"b\x00", b"\x00b", b"c\x00"],  # More zeros
            [b"\x00ab\x00", b"a\x00\x00b", b"\x00\x00c\x00"],  # Combined zeros
            2,
            2,
        ),
    ],
)
def test_fixed_size_binary_concat(
    input_data1: list[bytes | None],
    input_data2: list[bytes | None],
    expected_result: list[bytes | None],
    size1: int,
    size2: int,
) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array(input_data1, type=pa.binary(size1)),
            "b": pa.array(input_data2, type=pa.binary(size2)),
        }
    )
    # Verify inputs are FixedSizeBinary before concatenating
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size1)
    assert table.schema()["b"].dtype == DataType.fixed_size_binary(size2)
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {"a": expected_result}
    # Result should be FixedSizeBinary with combined size when both inputs are FixedSizeBinary
    assert result.schema()["a"].dtype == DataType.fixed_size_binary(size1 + size2)


def test_fixed_size_binary_concat_large() -> None:
    # Test concatenating large fixed size binary strings
    size1, size2 = 100, 50
    large_binary1 = b"x" * size1
    large_binary2 = b"y" * size2

    table = MicroPartition.from_pydict(
        {
            "a": pa.array([large_binary1, b"a" * size1, large_binary1], type=pa.binary(size1)),
            "b": pa.array([large_binary2, large_binary2, b"b" * size2], type=pa.binary(size2)),
        }
    )

    # Verify inputs are FixedSizeBinary
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size1)
    assert table.schema()["b"].dtype == DataType.fixed_size_binary(size2)

    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {
        "a": [
            b"x" * size1 + b"y" * size2,  # Large + Large
            b"a" * size1 + b"y" * size2,  # Small repeated + Large
            b"x" * size1 + b"b" * size2,  # Large + Small repeated
        ]
    }
    # Result should be FixedSizeBinary with combined size
    assert result.schema()["a"].dtype == DataType.fixed_size_binary(size1 + size2)


@pytest.mark.parametrize(
    "input_data,literal,expected_result,size",
    [
        # Basic broadcasting with fixed size
        ([b"abc", b"def", b"ghi"], b"xyz", [b"abcxyz", b"defxyz", b"ghixyz"], 3),
        # Broadcasting with nulls
        ([b"ab", None, b"cd"], b"xy", [b"abxy", None, b"cdxy"], 2),
        # Broadcasting with special sequences
        (
            [b"\x00\x01", b"\xff\xfe", b"\xe2\x98"],
            b"\x99\x00",
            [b"\x00\x01\x99\x00", b"\xff\xfe\x99\x00", b"\xe2\x98\x99\x00"],
            2,
        ),
        # Broadcasting with UTF-8
        (
            [b"\xe2\x98", b"\xf0\x9f", b"\xf0\x9f"],
            b"\x83\x00",
            [b"\xe2\x98\x83\x00", b"\xf0\x9f\x83\x00", b"\xf0\x9f\x83\x00"],
            2,
        ),
    ],
)
def test_fixed_size_binary_concat_broadcast(
    input_data: list[bytes | None],
    literal: bytes | None,
    expected_result: list[bytes | None],
    size: int,
) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array(input_data, type=pa.binary(size)),
        }
    )
    # Verify input is FixedSizeBinary
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)

    # Test right-side broadcasting
    result = table.eval_expression_list([col("a").binary.concat(lit(literal))])
    assert result.to_pydict() == {"a": expected_result}
    # Result should be Binary when using literals
    assert result.schema()["a"].dtype == DataType.binary()

    # Test left-side broadcasting
    result = table.eval_expression_list([lit(literal).binary.concat(col("a"))])
    assert result.to_pydict() == {"literal": [literal + data if data is not None else None for data in input_data]}
    # Result should be Binary when using literals
    assert result.schema()["literal"].dtype == DataType.binary()


def test_fixed_size_binary_concat_edge_cases() -> None:
    # Test various edge cases with different fixed sizes
    cases = [
        # Single byte values
        (1, [b"\x00", b"\xff", b"a", None], 1, [b"\x01", b"\x02", b"b", None], [b"\x00\x01", b"\xff\x02", b"ab", None]),
        # Two byte values
        (
            2,
            [b"\x00\x00", b"\xff\xff", b"ab", None],
            2,
            [b"\x01\x01", b"\x02\x02", b"cd", None],
            [b"\x00\x00\x01\x01", b"\xff\xff\x02\x02", b"abcd", None],
        ),
        # Four byte values (common for integers, floats)
        (
            4,
            [b"\x00" * 4, b"\xff" * 4, b"abcd", None],
            2,
            [b"\x01\x01", b"\x02\x02", b"ef", None],
            [b"\x00" * 4 + b"\x01\x01", b"\xff" * 4 + b"\x02\x02", b"abcdef", None],
        ),
    ]

    for size1, input_data1, size2, input_data2, expected_result in cases:
        table = MicroPartition.from_pydict(
            {
                "a": pa.array(input_data1, type=pa.binary(size1)),
                "b": pa.array(input_data2, type=pa.binary(size2)),
            }
        )
        # Verify inputs are FixedSizeBinary
        assert table.schema()["a"].dtype == DataType.fixed_size_binary(size1)
        assert table.schema()["b"].dtype == DataType.fixed_size_binary(size2)
        result = table.eval_expression_list([col("a").binary.concat(col("b"))])
        assert result.to_pydict() == {"a": expected_result}
        # Result should be FixedSizeBinary with combined size
        assert result.schema()["a"].dtype == DataType.fixed_size_binary(size1 + size2)


def test_fixed_size_binary_concat_with_binary() -> None:
    # Test concatenating FixedSizeBinary with regular Binary
    table = MicroPartition.from_pydict(
        {
            "a": pa.array([b"abc", b"def", None], type=pa.binary(3)),
            "b": pa.array([b"x", b"yz", b"uvw"]),  # Regular Binary
        }
    )
    # Verify first input is FixedSizeBinary
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(3)
    assert table.schema()["b"].dtype == DataType.binary()

    # Test FixedSizeBinary + Binary
    result = table.eval_expression_list([col("a").binary.concat(col("b"))])
    assert result.to_pydict() == {"a": [b"abcx", b"defyz", None]}
    # Result should be Binary when mixing types
    assert result.schema()["a"].dtype == DataType.binary()

    # Test Binary + FixedSizeBinary
    result = table.eval_expression_list([col("b").binary.concat(col("a"))])
    assert result.to_pydict() == {"b": [b"xabc", b"yzdef", None]}
    # Result should be Binary when mixing types
    assert result.schema()["b"].dtype == DataType.binary()


def test_fixed_size_binary_concat_with_literals() -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array([b"abc", b"def", None], type=pa.binary(3)),
        }
    )
    # Verify input is FixedSizeBinary
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(3)

    # Test with literal
    result = table.eval_expression_list([col("a").binary.concat(lit(b"xyz"))])
    assert result.to_pydict() == {"a": [b"abcxyz", b"defxyz", None]}
    # Result should be Binary when using literals
    assert result.schema()["a"].dtype == DataType.binary()

    # Test with null literal
    result = table.eval_expression_list([col("a").binary.concat(lit(None))])
    assert result.to_pydict() == {"a": [None, None, None]}
    # Result should be Binary when using literals
    assert result.schema()["a"].dtype == DataType.binary()


def test_fixed_size_binary_concat_errors() -> None:
    # Test error cases
    table = MicroPartition.from_pydict(
        {
            "a": pa.array([b"abc", b"def"], type=pa.binary(3)),
            "b": [1, 2],  # Wrong type
        }
    )

    # Test concat with wrong type
    with pytest.raises(Exception, match="Expects inputs to concat to be binary, but received a#Binary and b#Int64"):
        table.eval_expression_list([col("a").binary.concat(col("b"))])
