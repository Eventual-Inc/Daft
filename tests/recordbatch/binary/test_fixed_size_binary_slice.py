from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataType
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    "input_data,start,length,expected_result,size",
    [
        # Basic slicing
        ([b"abc", b"def", b"ghi"], [0, 1, 2], [2, 1, 1], [b"ab", b"e", b"i"], 3),
        # With nulls
        ([b"ab", None, b"cd"], [0, 1, 0], [1, 1, 2], [b"a", None, b"cd"], 2),
        # Special sequences
        ([b"\x00\x01", b"\xff\xfe", b"\xe2\x98"], [1, 0, 1], [1, 1, 1], [b"\x01", b"\xff", b"\x98"], 2),
        # Edge cases
        (
            [b"abc", b"def", b"ghi"],
            [3, 2, 1],  # Start at or beyond length
            [1, 2, 3],
            [b"", b"f", b"hi"],
            3,
        ),
        # UTF-8 sequences
        (
            [b"\xe2\x98\x83", b"\xf0\x9f\x98", b"\xf0\x9f\x8c"],  # UTF-8 characters and partials
            [0, 1, 2],
            [2, 2, 1],
            [b"\xe2\x98", b"\x9f\x98", b"\x8c"],
            3,
        ),
        # Zero bytes in different positions
        ([b"\x00ab", b"a\x00b", b"ab\x00"], [0, 1, 2], [2, 1, 1], [b"\x00a", b"\x00", b"\x00"], 3),
        # High value bytes
        ([b"\xff\xff\xff", b"\xfe\xfe\xfe", b"\xfd\xfd\xfd"], [0, 1, 2], [2, 1, 1], [b"\xff\xff", b"\xfe", b"\xfd"], 3),
        # Mixed content
        (
            [b"a\xff\x00", b"\x00\xff\x83", b"\xe2\x98\x83"],
            [1, 0, 0],
            [2, 2, 2],
            [b"\xff\x00", b"\x00\xff", b"\xe2\x98"],
            3,
        ),
    ],
)
def test_fixed_size_binary_slice(
    input_data: list[bytes | None],
    start: list[int],
    length: list[int],
    expected_result: list[bytes | None],
    size: int,
) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array(input_data, type=pa.binary(size)),
            "start": start,
            "length": length,
        }
    )
    # Verify input is FixedSizeBinary before slicing
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)
    result = table.eval_expression_list([col("a").binary.slice(col("start"), col("length"))])
    assert result.to_pydict() == {"a": expected_result}
    # Result should be regular Binary since slice might be smaller
    assert result.schema()["a"].dtype == DataType.binary()


@pytest.mark.parametrize(
    "input_data,start,expected_result,size",
    [
        # Without length parameter
        ([b"abc", b"def", b"ghi"], [1, 0, 2], [b"bc", b"def", b"i"], 3),
        # With nulls
        ([b"ab", None, b"cd"], [1, 0, 1], [b"b", None, b"d"], 2),
        # UTF-8 sequences
        ([b"\xe2\x98\x83", b"\xf0\x9f\x98", b"\xf0\x9f\x8c"], [1, 0, 2], [b"\x98\x83", b"\xf0\x9f\x98", b"\x8c"], 3),
        # Special bytes
        ([b"\x00\xff\x7f", b"\xff\x00\xff", b"\x7f\xff\x00"], [1, 2, 0], [b"\xff\x7f", b"\xff", b"\x7f\xff\x00"], 3),
    ],
)
def test_fixed_size_binary_slice_no_length(
    input_data: list[bytes | None],
    start: list[int],
    expected_result: list[bytes | None],
    size: int,
) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array(input_data, type=pa.binary(size)),
            "start": start,
        }
    )
    # Verify input is FixedSizeBinary before slicing
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)
    result = table.eval_expression_list([col("a").binary.slice(col("start"))])
    assert result.to_pydict() == {"a": expected_result}
    # Result should be regular Binary since slice might be smaller
    assert result.schema()["a"].dtype == DataType.binary()


def test_fixed_size_binary_slice_computed() -> None:
    # Test with computed start index (length - 2)
    size = 4
    table = MicroPartition.from_pydict(
        {
            "a": pa.array(
                [
                    b"abcd",  # Start at 2, take 2
                    b"\xff\xfe\xfd\xfc",  # Start at 2, take 2
                    b"\x00\x01\x02\x03",  # Start at 2, take 2
                    b"\xe2\x98\x83\x00",  # Start at 2, take 2
                ],
                type=pa.binary(size),
            ),
        }
    )
    # Verify input is FixedSizeBinary
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)

    # Test with computed start (size - 2) and fixed length
    result = table.eval_expression_list(
        [
            col("a").binary.slice(
                (lit(size) - 2).cast(DataType.int32()),  # Start 2 chars from end
                2,  # Take 2 chars
            )
        ]
    )
    assert result.to_pydict() == {"a": [b"cd", b"\xfd\xfc", b"\x02\x03", b"\x83\x00"]}
    assert result.schema()["a"].dtype == DataType.binary()

    # Test with fixed start and computed length (size - start)
    result = table.eval_expression_list(
        [
            col("a").binary.slice(
                1,  # Start at second char
                (lit(size) - 1).cast(DataType.int32()),  # Take remaining chars
            )
        ]
    )
    assert result.to_pydict() == {"a": [b"bcd", b"\xfe\xfd\xfc", b"\x01\x02\x03", b"\x98\x83\x00"]}
    assert result.schema()["a"].dtype == DataType.binary()


def test_fixed_size_binary_slice_edge_cases() -> None:
    # Test various edge cases with different fixed sizes
    cases = [
        # Single byte values
        (
            1,  # size
            [b"\x00", b"\xff", b"a", None],  # input
            [0, 0, 0, 0],  # start
            [1, 1, 1, 1],  # length
            [b"\x00", b"\xff", b"a", None],  # expected
        ),
        # Two byte values with boundary cases
        (
            2,  # size
            [b"\x00\x01", b"\xff\xfe", b"ab", None],  # input
            [1, 2, 0, 1],  # start
            [1, 0, 2, 1],  # length
            [b"\x01", b"", b"ab", None],  # expected
        ),
        # Four byte values with various slices
        (
            4,  # size
            [b"\x00\x01\x02\x03", b"abcd", b"\xff\xfe\xfd\xfc", None],  # input
            [0, 1, 2, 0],  # start
            [2, 2, 2, 4],  # length
            [b"\x00\x01", b"bc", b"\xfd\xfc", None],  # expected
        ),
    ]

    for size, input_data, start, length, expected in cases:
        table = MicroPartition.from_pydict(
            {
                "a": pa.array(input_data, type=pa.binary(size)),
                "start": start,
                "length": length,
            }
        )
        # Verify input is FixedSizeBinary
        assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)
        result = table.eval_expression_list([col("a").binary.slice(col("start"), col("length"))])
        assert result.to_pydict() == {"a": expected}
        assert result.schema()["a"].dtype == DataType.binary()


def test_fixed_size_binary_slice_with_literals() -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array([b"abc", b"def", None], type=pa.binary(3)),
        }
    )
    # Verify input is FixedSizeBinary before slicing
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(3)

    # Test with literal start and length
    result = table.eval_expression_list([col("a").binary.slice(lit(1), lit(1))])
    assert result.to_pydict() == {"a": [b"b", b"e", None]}
    assert result.schema()["a"].dtype == DataType.binary()

    # Test with only start
    result = table.eval_expression_list([col("a").binary.slice(lit(0))])
    assert result.to_pydict() == {"a": [b"abc", b"def", None]}
    assert result.schema()["a"].dtype == DataType.binary()

    # Test with start beyond length
    result = table.eval_expression_list([col("a").binary.slice(lit(3), lit(1))])
    assert result.to_pydict() == {"a": [b"", b"", None]}
    assert result.schema()["a"].dtype == DataType.binary()

    # Test with zero length
    result = table.eval_expression_list([col("a").binary.slice(lit(0), lit(0))])
    assert result.to_pydict() == {"a": [b"", b"", None]}
    assert result.schema()["a"].dtype == DataType.binary()


def test_fixed_size_binary_slice_errors() -> None:
    # Test error cases
    table = MicroPartition.from_pydict(
        {
            "a": pa.array([b"abc", b"def"], type=pa.binary(3)),
            "b": [1, 2],  # Wrong type
            "start": [-1, 0],  # Negative start
            "length": [0, -1],  # Negative length
        }
    )

    # Test slice on wrong type
    with pytest.raises(
        Exception, match="Expects inputs to binary_slice to be binary, integer and integer or null but received Int64"
    ):
        table.eval_expression_list([col("b").binary.slice(lit(0))])

    # Test negative start
    with pytest.raises(Exception, match="DaftError::ComputeError Failed to cast numeric value to target type"):
        table.eval_expression_list([col("a").binary.slice(col("start"))])

    # Test negative length
    with pytest.raises(Exception, match="DaftError::ComputeError Failed to cast numeric value to target type"):
        table.eval_expression_list([col("a").binary.slice(lit(0), col("length"))])

    # Test with wrong number of arguments (too many)
    with pytest.raises(
        Exception,
        match="(?:ExpressionBinaryNamespace.)?slice\\(\\) takes from 2 to 3 positional arguments but 4 were given",
    ):
        table.eval_expression_list([col("a").binary.slice(lit(0), lit(1), lit(2))])

    # Test with wrong number of arguments (too few)
    with pytest.raises(
        Exception, match="(?:ExpressionBinaryNamespace.)?slice\\(\\) missing 1 required positional argument: 'start'"
    ):
        table.eval_expression_list([col("a").binary.slice()])
