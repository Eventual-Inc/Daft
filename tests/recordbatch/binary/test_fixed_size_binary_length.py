from __future__ import annotations

import pyarrow as pa
import pytest

from daft import DataType
from daft.expressions import col
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    "input_data,expected_result,size",
    [
        # Basic binary data
        ([b"abc", b"def", b"ghi"], [3, 3, 3], 3),
        # With nulls
        ([b"ab", None, b"cd"], [2, None, 2], 2),
        # Special sequences
        ([b"\x00\x01", b"\xff\xfe", b"\xe2\x98"], [2, 2, 2], 2),
        # Complex UTF-8 sequences
        (
            [b"\xe2\x98\x83", b"\xf0\x9f\x98", b"\xf0\x9f\x8c"],  # Snowman, partial faces
            [3, 3, 3],
            3,
        ),
        # Zero bytes in different positions
        (
            [b"\x00ab", b"a\x00b", b"ab\x00"],  # Leading, middle, trailing zeros
            [3, 3, 3],
            3,
        ),
        # High value bytes
        ([b"\xff\xff\xff", b"\xfe\xfe\xfe", b"\xfd\xfd\xfd"], [3, 3, 3], 3),
        # Mixed binary content
        (
            [b"a\xff\x00", b"\x00\xff\x83", b"\xe2\x98\x83"],  # Mix of ASCII, high bytes, nulls, and UTF-8
            [3, 3, 3],
            3,
        ),
    ],
)
def test_fixed_size_binary_length(
    input_data: list[bytes | None],
    expected_result: list[int | None],
    size: int,
) -> None:
    table = MicroPartition.from_pydict(
        {
            "a": pa.array(input_data, type=pa.binary(size)),
        }
    )
    # Verify input is FixedSizeBinary before getting length
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)
    result = table.eval_expression_list([col("a").binary.length()])
    assert result.to_pydict() == {"a": expected_result}
    # Result should be UInt64 since length can't be negative
    assert result.schema()["a"].dtype == DataType.uint64()


def test_fixed_size_binary_length_large() -> None:
    # Test with larger fixed size binary values
    size = 100
    input_data = [
        b"x" * size,  # Repeated ASCII
        b"\x00" * size,  # All nulls
        b"\xff" * size,  # All high bytes
        None,  # Null value
        (b"Hello\x00World!" * 9)[:size],  # Pattern with null byte (12 bytes * 9 = 108 bytes, truncated to 100)
        (b"\xe2\x98\x83" * 34)[:size],  # Repeated UTF-8 sequence (3 bytes * 34 = 102 bytes, truncated to 100)
    ]
    expected_result = [size, size, size, None, size, size]

    table = MicroPartition.from_pydict(
        {
            "a": pa.array(input_data, type=pa.binary(size)),
        }
    )
    # Verify input is FixedSizeBinary
    assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)
    result = table.eval_expression_list([col("a").binary.length()])
    assert result.to_pydict() == {"a": expected_result}
    assert result.schema()["a"].dtype == DataType.uint64()


def test_fixed_size_binary_length_edge_cases() -> None:
    # Test various edge cases with different sizes
    cases = [
        # Single byte values
        (1, [b"\x00", b"\xff", b"a", None], [1, 1, 1, None]),
        # Two byte values
        (2, [b"\x00\x00", b"\xff\xff", b"ab", None], [2, 2, 2, None]),
        # Four byte values (common for integers, floats)
        (4, [b"\x00\x00\x00\x00", b"\xff\xff\xff\xff", b"abcd", None], [4, 4, 4, None]),
        # Eight byte values (common for timestamps, large integers)
        (8, [b"\x00" * 8, b"\xff" * 8, b"abcdefgh", None], [8, 8, 8, None]),
    ]

    for size, input_data, expected_result in cases:
        table = MicroPartition.from_pydict(
            {
                "a": pa.array(input_data, type=pa.binary(size)),
            }
        )
        # Verify input is FixedSizeBinary
        assert table.schema()["a"].dtype == DataType.fixed_size_binary(size)
        result = table.eval_expression_list([col("a").binary.length()])
        assert result.to_pydict() == {"a": expected_result}
        assert result.schema()["a"].dtype == DataType.uint64()


def test_fixed_size_binary_length_errors() -> None:
    # Test error cases
    table = MicroPartition.from_pydict(
        {
            "a": pa.array([1, 2], type=pa.int64()),  # Wrong type
        }
    )

    # Test length on wrong type
    with pytest.raises(Exception, match="Expects input to length to be binary, but received a#Int64"):
        table.eval_expression_list([col("a").binary.length()])
