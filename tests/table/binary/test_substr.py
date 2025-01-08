from __future__ import annotations

import pytest

from daft import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_binary_substr() -> None:
    table = MicroPartition.from_pydict(
        {"col": [b"foo", None, b"barbarbar", b"quux", b"1", b"", b"\xff\xfe\x00\x01", b"\x00\xff\xfe", b"\xff\xff\xff"]}
    )
    result = table.eval_expression_list([col("col").binary.substr(0, 5)])
    assert result.to_pydict() == {
        "col": [b"foo", None, b"barba", b"quux", b"1", None, b"\xff\xfe\x00\x01", b"\x00\xff\xfe", b"\xff\xff\xff"]
    }


@pytest.mark.parametrize(
    "input_data,start_data,length_data,expected_result",
    [
        # Test with column for start position
        (
            [b"hello", b"world", b"test", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            [1, 0, 2, 1, 0],
            3,
            [b"ell", b"wor", b"st", b"\xfe\x00", b"\x00\xff\xfe"],
        ),
        # Test with column for length
        (
            [b"hello", b"world", b"test", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            1,
            [2, 3, 4, 1, 2],
            [b"el", b"orl", b"est", b"\xfe", b"\xff\xfe"],
        ),
        # Test with both start and length as columns
        (
            [b"hello", b"world", b"test", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            [1, 0, 2, 0, 1],
            [2, 3, 1, 2, 1],
            [b"el", b"wor", b"s", b"\xff\xfe", b"\xff"],
        ),
        # Test with nulls in start column
        (
            [b"hello", b"world", b"test", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            [1, None, 2, None, 1],
            3,
            [b"ell", None, b"st", None, b"\xff\xfe"],
        ),
        # Test with nulls in length column
        (
            [b"hello", b"world", b"test", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            1,
            [2, None, 4, None, 2],
            [b"el", b"orld", b"est", b"\xfe\x00", b"\xff\xfe"],
        ),
        # Test with nulls in both columns
        (
            [b"hello", b"world", b"test", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            [1, None, 2, 1, None],
            [2, 3, None, None, 2],
            [b"el", None, b"st", b"\xfe\x00", None],
        ),
    ],
)
def test_binary_substr_with_columns(
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
    result = table.eval_expression_list([col("col").binary.substr(start, length)])
    assert result.to_pydict() == {"col": expected_result}


@pytest.mark.parametrize(
    "input_data,start,length,expected_result",
    [
        # Test start beyond string length
        ([b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"], [10, 20, 5, 4], 2, [None, None, None, None]),
        # Test zero length
        ([b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"], [1, 0, 1, 0], 0, [None, None, None, None]),
        # Test very large length
        (
            [b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            [0, 1, 0, 1],
            100,
            [b"hello", b"orld", b"\xff\xfe\x00", b"\xff\xfe"],
        ),
        # Test empty strings
        ([b"", b"", b"", b""], [0, 1, 0, 1], 3, [None, None, None, None]),
        # Test start + length overflow
        (
            [b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"],
            [2, 3, 1, 2],
            9999999999,
            [b"llo", b"ld", b"\xfe\x00", b"\xfe"],
        ),
    ],
)
def test_binary_substr_edge_cases(
    input_data: list[bytes],
    start: list[int],
    length: int,
    expected_result: list[bytes | None],
) -> None:
    table = MicroPartition.from_pydict({"col": input_data, "start": start})
    result = table.eval_expression_list([col("col").binary.substr(col("start"), length)])
    assert result.to_pydict() == {"col": expected_result}


def test_binary_substr_errors() -> None:
    # Test negative start
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"], "start": [-1, -2, -1, -2]}
    )
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize"):
        table.eval_expression_list([col("col").binary.substr(col("start"), 2)])

    # Test negative length
    table = MicroPartition.from_pydict({"col": [b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"]})
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize"):
        table.eval_expression_list([col("col").binary.substr(0, -3)])

    # Test both negative
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"], "start": [-2, -1, -2, -1]}
    )
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize"):
        table.eval_expression_list([col("col").binary.substr(col("start"), -2)])

    # Test negative length in column
    table = MicroPartition.from_pydict(
        {"col": [b"hello", b"world", b"\xff\xfe\x00", b"\x00\xff\xfe"], "length": [-2, -3, -2, -3]}
    )
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize"):
        table.eval_expression_list([col("col").binary.substr(0, col("length"))])


def test_binary_substr_computed() -> None:
    # Test with computed start index (length - 5)
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"hello world",  # len=11, start=6, expect "world"
                b"python programming",  # len=17, start=12, expect "mming"
                b"data science",  # len=12, start=7, expect "ience"
                b"artificial",  # len=10, start=5, expect "icial"
                b"intelligence",  # len=12, start=7, expect "gence"
                b"\xff\xfe\x00\xff\xfe",  # len=5, start=0, expect "\xff\xfe\x00"
                b"\x00\xff\xfe\xff\x00",  # len=5, start=0, expect "\x00\xff\xfe"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").binary.substr(
                (col("col").binary.length() - 5).cast(DataType.int32()),  # start 5 chars from end
                3,  # take 3 chars
            )
        ]
    )
    assert result.to_pydict() == {"col": [b"wor", b"mmi", b"ien", b"ici", b"gen", b"\xff\xfe\x00", b"\x00\xff\xfe"]}

    # Test with computed length (half of string length)
    table = MicroPartition.from_pydict(
        {
            "col": [
                b"hello world",  # len=11, len/2=5, expect "hello"
                b"python programming",  # len=17, len/2=8, expect "python pr"
                b"data science",  # len=12, len/2=6, expect "data s"
                b"artificial",  # len=10, len/2=5, expect "artif"
                b"intelligence",  # len=12, len/2=6, expect "intell"
                b"\xff\xfe\x00\xff\xfe",  # len=5, len/2=2, expect "\xff\xfe"
                b"\x00\xff\xfe\xff\x00",  # len=5, len/2=2, expect "\x00\xff"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").binary.substr(
                0,  # start from beginning
                (col("col").binary.length() / 2).cast(DataType.int32()),  # take half of string
            )
        ]
    )
    assert result.to_pydict() == {
        "col": [b"hello", b"python pr", b"data s", b"artif", b"intell", b"\xff\xfe", b"\x00\xff"]
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
                b"\xff\xfe\x00\xff\xfe",  # len=5, start=1, len=2, expect "\xfe\x00"
                b"\x00\xff\xfe\xff\x00",  # len=5, start=1, len=2, expect "\xff\xfe"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").binary.substr(
                (col("col").binary.length() / 5).cast(DataType.int32()),  # start at 1/5 of string
                (col("col").binary.length() / 3).cast(DataType.int32()),  # take 1/3 of string
            )
        ]
    )
    assert result.to_pydict() == {"col": [b"llo", b"hon pr", b"ta s", b"tif", b"tell", b"\xfe", b"\xff"]}
