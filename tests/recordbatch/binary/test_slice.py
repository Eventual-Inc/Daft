from __future__ import annotations

import pytest

from daft import DataType
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


def test_binary_slice() -> None:
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"foo",
                None,
                b"barbarbar",
                b"quux",
                b"1",
                b"",
                b"Hello\xe2\x98\x83World",  # UTF-8 character in middle
                b"\xf0\x9f\x98\x89test",  # UTF-8 bytes at start
                b"test\xf0\x9f\x8c\x88",  # UTF-8 bytes at end
                b"\xe2\x98\x83\xf0\x9f\x98\x89\xf0\x9f\x8c\x88",  # Multiple UTF-8 sequences
                b"Hello\x00World",  # Null character
                b"\xff\xfe\xfd",  # High bytes
                b"\x00\x01\x02",  # Control characters
            ]
        }
    )
    result = table.eval_expression_list([col("col").binary.slice(0, 5)])
    assert result.to_pydict() == {
        "col": [
            b"foo",
            None,
            b"barba",
            b"quux",
            b"1",
            b"",
            b"Hello",  # Should handle UTF-8 correctly
            b"\xf0\x9f\x98\x89t",  # Should include full UTF-8 sequence
            b"test\xf0",  # Should split UTF-8 sequence
            b"\xe2\x98\x83\xf0\x9f",  # Should split between sequences
            b"Hello",  # Should handle null character
            b"\xff\xfe\xfd",  # Should handle high bytes
            b"\x00\x01\x02",  # Should handle control characters
        ]
    }


@pytest.mark.parametrize(
    "input_data,start_data,length_data,expected_result",
    [
        # Test with column for start position
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            [1, 0, 2, 5, 1, 4, 1],
            3,
            [b"ell", b"wor", b"st", b"\xe2\x98\x83", b"\x9f\x98\x89", b"\xf0\x9f\x8c", b"\xfe\xfd"],
        ),
        # Test with column for length
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            1,
            [2, 3, 4, 5, 2, 1, 2],
            [b"el", b"orl", b"est", b"ello\xe2", b"\x9f\x98", b"e", b"\xfe\xfd"],
        ),
        # Test with both start and length as columns
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            [1, 0, 2, 5, 1, 4, 0],
            [2, 3, 1, 2, 3, 1, 3],
            [b"el", b"wor", b"s", b"\xe2\x98", b"\x9f\x98\x89", b"\xf0", b"\xff\xfe\xfd"],
        ),
        # Test with nulls in start column
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            [1, None, 2, None, 1, None, 1],
            3,
            [b"ell", None, b"st", None, b"\x9f\x98\x89", None, b"\xfe\xfd"],
        ),
        # Test with nulls in length column
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            1,
            [2, None, 4, None, 2, None, None],
            [b"el", None, b"est", None, b"\x9f\x98", None, None],
        ),
        # Test with nulls in both columns
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            [1, None, 2, 5, None, 4, None],
            [2, 3, None, None, 2, None, 2],
            [b"el", None, None, None, None, None, None],
        ),
        # Test with all nulls in start column
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            [1, None, None, None, None, None, None],
            [2, 3, 1, 2, 3, 1, 3],
            [b"el", None, None, None, None, None, None],
        ),
        # Test with all nulls in length column
        (
            [
                b"hello",
                b"world",
                b"test",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd",
            ],
            [1, 0, 2, 5, 1, 4, 0],
            [2, None, None, None, None, None, None],
            [b"el", None, None, None, None, None, None],
        ),
    ],
)
def test_binary_slice_with_columns(
    input_data: list[bytes | None],
    start_data: list[int | None] | int,
    length_data: list[int | None] | int,
    expected_result: list[bytes | None],
) -> None:
    table_data = {"col": input_data}
    if isinstance(start_data, list):
        table_data["start"] = start_data
        start = col("start")
    else:
        start = start_data

    if isinstance(length_data, list):
        table_data["length"] = length_data
        length = col("length")
    else:
        length = length_data

    table = MicroPartition.from_pydict(table_data)
    result = table.eval_expression_list([col("col").binary.slice(start, length)])
    assert result.to_pydict() == {"col": expected_result}


@pytest.mark.parametrize(
    "input_data,start,length,expected_result",
    [
        # Test start beyond string length
        (
            [
                b"hello",
                b"world",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd\xfc",
            ],
            [10, 20, 15, 10, 10, 5],
            2,
            [b"", b"", b"", b"", b"", b""],
        ),
        # Test start way beyond string length
        (
            [
                b"hello",  # len 5
                b"world",  # len 5
                b"test",  # len 4
                b"\xff\xfe\xfd",  # len 3
            ],
            [100, 1000, 50, 25],
            5,
            [b"", b"", b"", b""],
        ),
        # Test start beyond length with None length
        (
            [
                b"hello",
                b"world",
                b"test",
                b"\xff\xfe\xfd",
            ],
            [10, 20, 15, 8],
            None,
            [b"", b"", b"", b""],
        ),
        # Test zero length
        (
            [
                b"hello",
                b"world",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd\xfc",
            ],
            [1, 0, 5, 0, 4, 2],
            0,
            [b"", b"", b"", b"", b"", b""],
        ),
        # Test very large length
        (
            [
                b"hello",
                b"world",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd\xfc",
            ],
            [0, 1, 5, 0, 4, 1],
            100,
            [b"hello", b"orld", b"\xe2\x98\x83World", b"\xf0\x9f\x98\x89test", b"\xf0\x9f\x8c\x88", b"\xfe\xfd\xfc"],
        ),
        # Test empty strings
        (
            [b"", b"", b"", b""],
            [0, 1, 2, 3],
            3,
            [b"", b"", b"", b""],
        ),
        # Test start + length overflow
        (
            [
                b"hello",
                b"world",
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd\xfc",
            ],
            [2, 3, 5, 0, 4, 2],
            9999999999,
            [b"llo", b"ld", b"\xe2\x98\x83World", b"\xf0\x9f\x98\x89test", b"\xf0\x9f\x8c\x88", b"\xfd\xfc"],
        ),
        # Test UTF-8 and binary sequence boundaries
        (
            [
                b"Hello\xe2\x98\x83World",
                b"\xf0\x9f\x98\x89test",
                b"test\xf0\x9f\x8c\x88",
                b"\xff\xfe\xfd\xfc",
                b"\x00\x01\x02\x03",
            ],
            [4, 0, 3, 1, 2],
            2,
            [b"o\xe2", b"\xf0\x9f", b"t\xf0", b"\xfe\xfd", b"\x02\x03"],
        ),
    ],
)
def test_binary_slice_edge_cases(
    input_data: list[bytes],
    start: list[int],
    length: int,
    expected_result: list[bytes | None],
) -> None:
    table = MicroPartition.from_pydict({"col": input_data, "start": start})
    result = table.eval_expression_list([col("col").binary.slice(col("start"), length)])
    assert result.to_pydict() == {"col": expected_result}


def test_binary_slice_errors() -> None:
    # Test negative start
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world", b"Hello\xe2\x98\x83World", b"\xff\xfe\xfd"], "start": [-1, -2, -3, -1]}
    )
    with pytest.raises(Exception, match="DaftError::ComputeError Failed to cast numeric value to target type"):
        table.eval_expression_list([col("col").binary.slice(col("start"), 2)])

    # Test negative length
    table = MicroPartition.from_pydict({"col": [b"hello", b"world", b"Hello\xe2\x98\x83World", b"\xff\xfe\xfd"]})
    with pytest.raises(Exception, match="DaftError::ComputeError Failed to cast numeric value to target type"):
        table.eval_expression_list([col("col").binary.slice(0, -3)])

    # Test both negative
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world", b"Hello\xe2\x98\x83World", b"\xff\xfe\xfd"], "start": [-2, -1, -3, -2]}
    )
    with pytest.raises(Exception, match="DaftError::ComputeError Failed to cast numeric value to target type"):
        table.eval_expression_list([col("col").binary.slice(col("start"), -2)])

    # Test negative length in column
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world", b"Hello\xe2\x98\x83World", b"\xff\xfe\xfd"], "length": [-2, -3, -4, -2]}
    )
    with pytest.raises(Exception, match="DaftError::ComputeError Failed to cast numeric value to target type"):
        table.eval_expression_list([col("col").binary.slice(0, col("length"))])

    # Test slice with wrong number of arguments (too many)
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world"], "start": [1, 2], "length": [2, 3], "extra": [4, 5]}
    )
    with pytest.raises(
        Exception,
        match="(?:ExpressionBinaryNamespace.)?slice\\(\\) takes from 2 to 3 positional arguments but 4 were given",
    ):
        table.eval_expression_list([col("col").binary.slice(col("start"), col("length"), col("extra"))])

    # Test slice with wrong number of arguments (too few)
    table = MicroPartition.from_pydict({"col": [b"hello", b"world"], "start": [1, 2]})
    with pytest.raises(
        Exception, match="(?:ExpressionBinaryNamespace.)?slice\\(\\) missing 1 required positional argument: 'start'"
    ):
        table.eval_expression_list([col("col").binary.slice()])


def test_binary_slice_computed() -> None:
    # Test with computed start index (length - 5)
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"hello world",  # len=11, start=6, expect "world"
                b"python programming",  # len=17, start=12, expect "mming"
                b"data science",  # len=12, start=7, expect "ience"
                b"artificial",  # len=10, start=5, expect "icial"
                b"intelligence",  # len=12, start=7, expect "gence"
                b"Hello\xe2\x98\x83World",  # len=12, start=7, expect "World"
                b"test\xf0\x9f\x98\x89test",  # len=12, start=7, expect "test"
                b"test\xf0\x9f\x8c\x88test",  # len=12, start=7, expect "test"
                b"\xff\xfe\xfd\xfc\xfb",  # len=5, start=0, expect "\xff\xfe\xfd"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").binary.slice(
                (col("col").binary.length() - 5).cast(DataType.int32()),  # start 5 chars from end
                3,  # take 3 chars
            )
        ]
    )
    assert result.to_pydict() == {
        "col": [b"wor", b"mmi", b"ien", b"ici", b"gen", b"Wor", b"\x89te", b"\x88te", b"\xff\xfe\xfd"]
    }

    # Test with computed length (half of string length)
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"hello world",  # len=11, len/2=5, expect "hello"
                b"python programming",  # len=17, len/2=8, expect "python pr"
                b"data science",  # len=12, len/2=6, expect "data s"
                b"artificial",  # len=10, len/2=5, expect "artif"
                b"intelligence",  # len=12, len/2=6, expect "intell"
                b"Hello\xe2\x98\x83World",  # len=12, len/2=6, expect "Hello\xe2"
                b"test\xf0\x9f\x98\x89test",  # len=12, len/2=6, expect "test\xf0\x9f"
                b"test\xf0\x9f\x8c\x88test",  # len=12, len/2=6, expect "test\xf0\x9f"
                b"\xff\xfe\xfd\xfc\xfb",  # len=5, len/2=2, expect "\xff\xfe"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").binary.slice(
                0,  # start from beginning
                (col("col").binary.length() / 2).cast(DataType.int32()),  # take half of string
            )
        ]
    )
    assert result.to_pydict() == {
        "col": [
            b"hello",
            b"python pr",
            b"data s",
            b"artif",
            b"intell",
            b"Hello\xe2",
            b"test\xf0\x9f",
            b"test\xf0\x9f",
            b"\xff\xfe",
        ]
    }

    # Test with both computed start and length
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"hello world",  # len=11, start=2, len=3, expect "llo"
                b"python programming",  # len=17, start=3, len=5, expect "hon pr"
                b"data science",  # len=12, start=2, len=4, expect "ta s"
                b"artificial",  # len=10, start=2, len=3, expect "tif"
                b"intelligence",  # len=12, start=2, len=4, expect "tell"
                b"Hello\xe2\x98\x83World",  # len=12, start=2, len=4, expect "llo\xe2"
                b"test\xf0\x9f\x98\x89test",  # len=12, start=2, len=4, expect "st\xf0\x9f"
                b"test\xf0\x9f\x8c\x88test",  # len=12, start=2, len=4, expect "st\xf0\x9f"
                b"\xff\xfe\xfd\xfc\xfb",  # len=5, start=1, len=2, expect "\xfe\xfd"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").binary.slice(
                (col("col").binary.length() / 5).cast(DataType.int32()),  # start at 1/5 of string
                (col("col").binary.length() / 3).cast(DataType.int32()),  # take 1/3 of string
            )
        ]
    )
    assert result.to_pydict() == {
        "col": [b"llo", b"hon pr", b"ta s", b"tif", b"tell", b"llo\xe2", b"st\xf0\x9f", b"st\xf0\x9f", b"\xfe"]
    }


def test_binary_slice_type_errors() -> None:
    # Test slice with string start type
    table = MicroPartition.from_pydict({"col": [b"hello", b"world"], "start": ["1", "2"]})
    with pytest.raises(
        Exception,
        match="Expects inputs to binary_slice to be binary, integer and integer or null but received Binary, Utf8 and Int32",
    ):
        table.eval_expression_list([col("col").binary.slice(col("start"), 2)])

    # Test slice with float start type
    table = MicroPartition.from_pydict({"col": [b"hello", b"world"], "start": [1.5, 2.5]})
    with pytest.raises(
        Exception,
        match="Expects inputs to binary_slice to be binary, integer and integer or null but received Binary, Float64 and Int32",
    ):
        table.eval_expression_list([col("col").binary.slice(col("start"), 2)])

    # Test slice with boolean start type
    table = MicroPartition.from_pydict({"col": [b"hello", b"world"], "start": [True, False]})
    with pytest.raises(
        Exception,
        match="Expects inputs to binary_slice to be binary, integer and integer or null but received Binary, Boolean and Int32",
    ):
        table.eval_expression_list([col("col").binary.slice(col("start"), 2)])

    # Test slice with binary start type
    table = MicroPartition.from_pydict({"col": [b"hello", b"world"], "start": [b"1", b"2"]})
    with pytest.raises(
        Exception,
        match="Expects inputs to binary_slice to be binary, integer and integer or null but received Binary, Binary and Int32",
    ):
        table.eval_expression_list([col("col").binary.slice(col("start"), 2)])

    # Test slice with null start type
    table = MicroPartition.from_pydict({"col": [b"hello", b"world"], "start": [None, None]})
    with pytest.raises(
        Exception,
        match="Expects inputs to binary_slice to be binary, integer and integer or null but received Binary, Null and Int32",
    ):
        table.eval_expression_list([col("col").binary.slice(col("start"), 2)])


def test_binary_slice_multiple_slices() -> None:
    # Test taking multiple different slices from the same binary data
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"hello",  # Simple ASCII
                b"Hello\xe2\x98\x83World",  # With UTF-8 character
                b"\xf0\x9f\x98\x89test\xf0\x9f\x8c\x88",  # Multiple UTF-8 sequences
                b"\xff\xfe\xfd\xfc\xfb",  # Raw bytes
            ]
        }
    )

    # Get multiple slices
    result = table.eval_expression_list(
        [
            col("col").binary.slice(1, 3).alias("slice1"),  # Middle slice
            col("col").binary.slice(0, 1).alias("slice2"),  # First byte
            col("col").binary.slice(2, 2).alias("slice3"),  # Another middle slice
            col("col")
            .binary.slice((col("col").binary.length().cast(DataType.int64()) - 1), 1)
            .alias("slice4"),  # Last byte
        ]
    )

    assert result.to_pydict() == {
        "slice1": [b"ell", b"ell", b"\x9f\x98\x89", b"\xfe\xfd\xfc"],
        "slice2": [b"h", b"H", b"\xf0", b"\xff"],
        "slice3": [b"ll", b"ll", b"\x98\x89", b"\xfd\xfc"],
        "slice4": [b"o", b"d", b"\x88", b"\xfb"],
    }

    # Test with computed indices
    result = table.eval_expression_list(
        [
            # First half
            col("col").binary.slice(0, (col("col").binary.length() / 2).cast(DataType.int32())).alias("first_half"),
            # Second half
            col("col")
            .binary.slice(
                (col("col").binary.length() / 2).cast(DataType.int32()),
                (col("col").binary.length() / 2).cast(DataType.int32()),
            )
            .alias("second_half"),
            # Middle third
            col("col")
            .binary.slice(
                (col("col").binary.length() / 3).cast(DataType.int32()),
                (col("col").binary.length() / 3).cast(DataType.int32()),
            )
            .alias("middle_third"),
        ]
    )

    assert result.to_pydict() == {
        "first_half": [b"he", b"Hello\xe2", b"\xf0\x9f\x98\x89te", b"\xff\xfe"],
        "second_half": [b"ll", b"\x98\x83Worl", b"st\xf0\x9f\x8c\x88", b"\xfd\xfc"],
        "middle_third": [b"e", b"o\xe2\x98\x83", b"test", b"\xfe"],
    }


@pytest.mark.parametrize(
    "input_data,start,length,expected_result",
    [
        # Test single binary value with multiple start positions
        (
            b"hello",  # Single binary value
            [0, 1, 2, 3, 4, 5],  # Multiple start positions
            2,  # Fixed length
            [b"he", b"el", b"ll", b"lo", b"o", b""],  # Expected slices
        ),
        # Test single binary value with multiple lengths
        (
            b"hello",
            1,  # Fixed start
            [1, 2, 3, 4],  # Multiple lengths
            [b"e", b"el", b"ell", b"ello"],  # Expected slices
        ),
        # Test single binary value with both start and length as arrays
        (
            b"hello",
            [0, 1, 2, 3],  # Multiple starts
            [1, 2, 3, 2],  # Multiple lengths
            [b"h", b"el", b"llo", b"lo"],  # Expected slices
        ),
        # Test with UTF-8 sequences
        (
            # Single UTF-8 string with snowman character
            b"Hello\xe2\x98\x83World",
            [0, 5, 8],  # Multiple starts
            [5, 3, 5],  # Multiple lengths
            # Expected: "Hello", snowman char, "World"
            [b"Hello", b"\xe2\x98\x83", b"World"],
        ),
        # Test with binary data
        (
            b"\xff\xfe\xfd\xfc\xfb",  # Single binary sequence
            [0, 2, 4],  # Multiple starts
            [2, 2, 1],  # Multiple lengths
            [b"\xff\xfe", b"\xfd\xfc", b"\xfb"],  # Expected slices
        ),
        # Test edge cases with single binary value
        (
            b"test",
            [0, 4, 2],  # Start at beginning, end, and middle
            [4, 0, 5],  # Full length, zero length, overflow length
            [b"test", b"", b"st"],  # Expected results
        ),
        # Test with nulls in start/length
        (
            b"hello",
            [0, None, 2, None],  # Mix of valid starts and nulls
            [2, 3, None, None],  # Mix of valid lengths and nulls
            [b"he", None, None, None],  # Expected results with nulls
        ),
    ],
)
def test_binary_slice_broadcasting(
    input_data: bytes,
    start: list[int] | int,
    length: list[int] | int,
    expected_result: list[bytes | None],
) -> None:
    # Create table with slice parameters
    table_data = {}

    # Add start/length columns if they are arrays
    if isinstance(start, list):
        table_data["start"] = start
        start_expr = col("start")
    else:
        start_expr = start

    if isinstance(length, list):
        table_data["length"] = length
        length_expr = col("length")
    else:
        length_expr = length

    # Create table with just the slice parameters
    table = MicroPartition.from_pydict(table_data)

    # Perform slice operation on the raw binary value
    result = table.eval_expression_list([lit(input_data).binary.slice(start_expr, length_expr)])
    assert result.to_pydict() == {"literal": expected_result}
