from __future__ import annotations

import pytest

from daft import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_substr() -> None:
    table = MicroPartition.from_pydict({"col": ["foo", None, "barbarbar", "quux", "1", ""]})
    result = table.eval_expression_list([col("col").str.substr(0, 5)])
    assert result.to_pydict() == {"col": ["foo", None, "barba", "quux", "1", None]}


@pytest.mark.parametrize(
    "input_data,start_data,length_data,expected_result",
    [
        # Test with column for start position
        (
            ["hello", "world", "test"],
            [1, 0, 2],
            3,
            ["ell", "wor", "st"],
        ),
        # Test with column for length
        (
            ["hello", "world", "test"],
            1,
            [2, 3, 4],
            ["el", "orl", "est"],
        ),
        # Test with both start and length as columns
        (
            ["hello", "world", "test"],
            [1, 0, 2],
            [2, 3, 1],
            ["el", "wor", "s"],
        ),
        # Test with nulls in start column
        (
            ["hello", "world", "test"],
            [1, None, 2],
            3,
            ["ell", None, "st"],
        ),
        # Test with nulls in length column
        (
            ["hello", "world", "test"],
            1,
            [2, None, 4],
            ["el", "orld", "est"],
        ),
        # Test with nulls in both columns
        (
            ["hello", "world", "test"],
            [1, None, 2],
            [2, 3, None],
            ["el", None, "st"],
        ),
    ],
)
def test_utf8_substr_with_columns(
    input_data: list[str | None],
    start_data: list[int | None] | int,
    length_data: list[int | None] | int,
    expected_result: list[str | None],
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
    result = table.eval_expression_list([col("col").str.substr(start, length)])
    assert result.to_pydict() == {"col": expected_result}


@pytest.mark.parametrize(
    "input_data,start,length,expected_result",
    [
        # Test start beyond string length
        (["hello", "world"], [10, 20], 2, [None, None]),
        # Test zero length
        (["hello", "world"], [1, 0], 0, [None, None]),
        # Test very large length
        (["hello", "world"], [0, 1], 100, ["hello", "orld"]),
        # Test empty strings
        (["", ""], [0, 1], 3, [None, None]),
        # Test start + length overflow
        (["hello", "world"], [2, 3], 9999999999, ["llo", "ld"]),
    ],
)
def test_utf8_substr_edge_cases(
    input_data: list[str],
    start: list[int],
    length: int,
    expected_result: list[str | None],
) -> None:
    table = MicroPartition.from_pydict({"col": input_data, "start": start})
    result = table.eval_expression_list([col("col").str.substr(col("start"), length)])
    assert result.to_pydict() == {"col": expected_result}


def test_utf8_substr_errors() -> None:
    # Test negative start
    table = MicroPartition.from_pydict({"col": ["hello", "world"], "start": [-1, -2]})
    with pytest.raises(Exception, match="Error in repeat: failed to cast length as usize"):
        table.eval_expression_list([col("col").str.substr(col("start"), 2)])

    # Test negative length
    table = MicroPartition.from_pydict({"col": ["hello", "world"]})
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize -3"):
        table.eval_expression_list([col("col").str.substr(0, -3)])

    # Test both negative
    table = MicroPartition.from_pydict({"col": ["hello", "world"], "start": [-2, -1]})
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize -2"):
        table.eval_expression_list([col("col").str.substr(col("start"), -2)])

    # Test negative length in column
    table = MicroPartition.from_pydict({"col": ["hello", "world"], "length": [-2, -3]})
    with pytest.raises(Exception, match="Error in repeat: failed to cast length as usize"):
        table.eval_expression_list([col("col").str.substr(0, col("length"))])


def test_utf8_substr_computed() -> None:
    # Test with computed start index (length - 5)
    table = MicroPartition.from_pydict(
        {
            "col": [
                "hello world",  # len=11, start=6, expect "world"
                "python programming",  # len=17, start=12, expect "mming"
                "data science",  # len=12, start=7, expect "ience"
                "artificial",  # len=10, start=5, expect "icial"
                "intelligence",  # len=12, start=7, expect "gence"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").str.substr(
                (col("col").str.length() - 5).cast(DataType.int32()),  # start 5 chars from end
                3,  # take 3 chars
            )
        ]
    )
    assert result.to_pydict() == {"col": ["wor", "mmi", "ien", "ici", "gen"]}

    # Test with computed length (half of string length)
    table = MicroPartition.from_pydict(
        {
            "col": [
                "hello world",  # len=11, len/2=5, expect "hello"
                "python programming",  # len=17, len/2=8, expect "python pr"
                "data science",  # len=12, len/2=6, expect "data s"
                "artificial",  # len=10, len/2=5, expect "artif"
                "intelligence",  # len=12, len/2=6, expect "intell"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").str.substr(
                0,  # start from beginning
                (col("col").str.length() / 2).cast(DataType.int32()),  # take half of string
            )
        ]
    )
    assert result.to_pydict() == {"col": ["hello", "python pr", "data s", "artif", "intell"]}

    # Test with both computed start and length
    table = MicroPartition.from_pydict(
        {
            "col": [
                "hello world",  # len=11, start=2, len=3, expect "llo"
                "python programming",  # len=17, start=3, len=5, expect "hon pr"
                "data science",  # len=12, start=2, len=4, expect "ta s"
                "artificial",  # len=10, start=2, len=3, expect "tif"
                "intelligence",  # len=12, start=2, len=4, expect "tell"
            ]
        }
    )
    result = table.eval_expression_list(
        [
            col("col").str.substr(
                (col("col").str.length() / 5).cast(DataType.int32()),  # start at 1/5 of string
                (col("col").str.length() / 3).cast(DataType.int32()),  # take 1/3 of string
            )
        ]
    )
    assert result.to_pydict() == {"col": ["llo", "hon pr", "ta s", "tif", "tell"]}
