from __future__ import annotations

import pytest

from daft import DataType
from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_substr() -> None:
    table = MicroPartition.from_pydict(
        {
            "col": [
                "foo",
                None,
                "barbarbar",
                "quux",
                "1",
                "",
                "Hello☃World",  # UTF-8 character in middle
                "😉test",  # UTF-8 character at start
                "test🌈",  # UTF-8 character at end
                "☃😉🌈",  # Multiple UTF-8 characters
                "Hello\u0000World",  # Null character
            ]
        }
    )
    result = table.eval_expression_list([col("col").str.substr(0, 5)])
    assert result.to_pydict() == {
        "col": [
            "foo",
            None,
            "barba",
            "quux",
            "1",
            None,
            "Hello",  # Should handle UTF-8 correctly
            "😉test",  # Should include full emoji
            "test🌈",  # Should include full emoji
            "☃😉🌈",  # Should include all characters
            "Hello",  # Should handle null character
        ]
    }


@pytest.mark.parametrize(
    "input_data,start_data,length_data,expected_result",
    [
        # Test with column for start position
        (
            ["hello", "world", "test", "Hello☃World", "😉test", "test🌈"],
            [1, 0, 2, 5, 1, 4],
            3,
            ["ell", "wor", "st", "☃Wo", "tes", "🌈"],
        ),
        # Test with column for length
        (
            ["hello", "world", "test", "Hello☃World", "😉best", "test🌈"],
            1,
            [2, 3, 4, 5, 2, 1],
            ["el", "orl", "est", "ello☃", "be", "e"],
        ),
        # Test with both start and length as columns
        (
            ["hello", "world", "test", "Hello☃World", "😉test", "test🌈"],
            [1, 0, 2, 5, 1, 4],
            [2, 3, 1, 2, 3, 1],
            ["el", "wor", "s", "☃W", "tes", "🌈"],
        ),
        # Test with nulls in start column
        (
            ["hello", "world", "test", "Hello☃World", "😉test", "test🌈"],
            [1, None, 2, None, 1, None],
            3,
            ["ell", None, "st", None, "tes", None],
        ),
        # Test with nulls in length column
        (
            ["hello", "world", "test", "Hello☃World", "😉best", "test🌈"],
            1,
            [2, None, 4, None, 2, None],
            ["el", "orld", "est", "ello☃World", "be", "est🌈"],
        ),
        # Test with nulls in both columns
        (
            ["hello", "world", "test", "Hello☃World", "😉test", "test🌈"],
            [1, None, 2, 5, None, 4],
            [2, 3, None, None, 2, None],
            ["el", None, "st", "☃World", None, "🌈"],
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
        (
            ["hello", "world", "Hello☃World", "😉test", "test🌈"],
            [10, 20, 15, 10, 10],
            2,
            [None, None, None, None, None],
        ),
        # Test start way beyond string length
        (
            [
                "hello",  # len 5
                "world",  # len 5
                "test",  # len 4
                "☃😉🌈",  # len 3
            ],
            [100, 1000, 50, 25],
            5,
            [None, None, None, None],
        ),
        # Test start beyond length with None length
        (
            [
                "hello",
                "world",
                "test",
                "☃😉🌈",
            ],
            [10, 20, 15, 8],
            None,
            [None, None, None, None],
        ),
        # Test zero length
        (
            ["hello", "world", "Hello☃World", "😉test", "test🌈"],
            [1, 0, 5, 0, 4],
            0,
            [None, None, None, None, None],
        ),
        # Test very large length
        (
            ["hello", "world", "Hello☃World", "😉test", "test🌈"],
            [0, 1, 5, 0, 4],
            100,
            ["hello", "orld", "☃World", "😉test", "🌈"],
        ),
        # Test empty strings
        (
            ["", "", ""],
            [0, 1, 2],
            3,
            [None, None, None],
        ),
        # Test start + length overflow
        (
            ["hello", "world", "Hello☃World", "😉test", "test🌈"],
            [2, 3, 5, 0, 4],
            9999999999,
            ["llo", "ld", "☃World", "😉test", "🌈"],
        ),
        # Test UTF-8 character boundaries
        (
            ["Hello☃World", "😉test", "test🌈", "☃😉🌈"],
            [4, 0, 3, 1],
            2,
            ["o☃", "😉t", "t🌈", "😉🌈"],
        ),
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
    table = MicroPartition.from_pydict({"col": ["hello", "world", "Hello☃World"], "start": [-1, -2, -3]})
    with pytest.raises(Exception, match="Error in repeat: failed to cast length as usize"):
        table.eval_expression_list([col("col").str.substr(col("start"), 2)])

    # Test negative length
    table = MicroPartition.from_pydict({"col": ["hello", "world", "Hello☃World"]})
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize -3"):
        table.eval_expression_list([col("col").str.substr(0, -3)])

    # Test both negative
    table = MicroPartition.from_pydict({"col": ["hello", "world", "Hello☃World"], "start": [-2, -1, -3]})
    with pytest.raises(Exception, match="Error in substr: failed to cast length as usize -2"):
        table.eval_expression_list([col("col").str.substr(col("start"), -2)])

    # Test negative length in column
    table = MicroPartition.from_pydict({"col": ["hello", "world", "Hello☃World"], "length": [-2, -3, -4]})
    with pytest.raises(Exception, match="Error in repeat: failed to cast length as usize"):
        table.eval_expression_list([col("col").str.substr(0, col("length"))])


def test_utf8_substr_computed() -> None:
    # Test with computed start index (length - 5)
    table = MicroPartition.from_pydict(
        {
            "col": [
                "hello world",  # len=11, start=6, expect "wor"
                "python programming",  # len=17, start=12, expect "mmi"
                "data science",  # len=12, start=7, expect "ien"
                "artificial",  # len=10, start=5, expect "ici"
                "intelligence",  # len=12, start=7, expect "gen"
                "Hello☃World",  # len=11, start=6, expect "Wor"
                "test😉best",  # len=9, start=4, expect "😉be"
                "test🌈best",  # len=9, start=4, expect "🌈be"
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
    assert result.to_pydict() == {"col": ["wor", "mmi", "ien", "ici", "gen", "Wor", "😉be", "🌈be"]}

    # Test with computed length (half of string length)
    table = MicroPartition.from_pydict(
        {
            "col": [
                "hello world",  # len=11, len/2=5, expect "hello"
                "python programming",  # len=17, len/2=8, expect "python pr"
                "data science",  # len=12, len/2=6, expect "data s"
                "artificial",  # len=10, len/2=5, expect "artif"
                "intelligence",  # len=12, len/2=6, expect "intell"
                "Hello☃World",  # len=11, len/2=5, expect "Hello"
                "test😉test",  # len=9, len/2=4, expect "test"
                "test🌈test",  # len=9, len/2=4, expect "test"
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
    assert result.to_pydict() == {"col": ["hello", "python pr", "data s", "artif", "intell", "Hello", "test", "test"]}

    # Test with both computed start and length
    table = MicroPartition.from_pydict(
        {
            "col": [
                "hello world",  # len=11, start=2, len=3, expect "llo"
                "python programming",  # len=17, start=3, len=5, expect "hon pr"
                "data science",  # len=12, start=2, len=4, expect "ta s"
                "artificial",  # len=10, start=2, len=3, expect "tif"
                "intelligence",  # len=12, start=2, len=4, expect "tell"
                "Hello☃World",  # len=11, start=2, len=3, expect "llo"
                "test😉test",  # len=9, start=1, len=3, expect "est😉"
                "test🌈test",  # len=9, start=1, len=3, expect "est🌈"
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
    assert result.to_pydict() == {"col": ["llo", "hon pr", "ta s", "tif", "tell", "llo", "est", "est"]}
