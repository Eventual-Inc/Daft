from __future__ import annotations

import pytest

from daft import col, lit
from daft.datatype import DataType
from daft.recordbatch import MicroPartition


@pytest.fixture
def table():
    return MicroPartition.from_pydict(
        {
            "col": [None, None, [], [], ["a"], [None], ["a", "a"], ["a", None], ["a", None, "a"]],
            "values": [None, "b", None, "c", "d", "e", "f", "g", "h"],
        }
    )


@pytest.fixture
def fixed_table():
    table = MicroPartition.from_pydict(
        {
            "col": [["a", "a"], ["a", "a"], ["a", None], [None, None], None, [None, None], None, None],
            "values": [None, "b", "c", "d", None, "e", "f", None],
        }
    )

    fixed_dtype = DataType.fixed_size_list(DataType.string(), 2)
    return table.eval_expression_list([col("col").cast(fixed_dtype), col("values")])


def test_list_append_basic(table):
    df = table.eval_expression_list([col("col").list_append(col("values"))])
    result = df.to_pydict()

    expected = [
        [None],
        ["b"],
        [None],
        ["c"],
        ["a", "d"],
        [None, "e"],
        ["a", "a", "f"],
        ["a", None, "g"],
        ["a", None, "a", "h"],
    ]
    assert result["col"] == expected


def test_list_append_with_literal(table):
    df = table.eval_expression_list([col("col").list_append(lit("z"))])
    result = df.to_pydict()

    expected = [
        ["z"],
        ["z"],
        ["z"],
        ["z"],
        ["a", "z"],
        [None, "z"],
        ["a", "a", "z"],
        ["a", None, "z"],
        ["a", None, "a", "z"],
    ]
    assert result["col"] == expected


def test_fixed_list_append_basic(fixed_table):
    df = fixed_table.eval_expression_list([col("col").list_append(col("values"))])
    result = df.to_pydict()

    expected = [
        ["a", "a", None],
        ["a", "a", "b"],
        ["a", None, "c"],
        [None, None, "d"],
        [None],
        [None, None, "e"],
        ["f"],
        [None],
    ]
    assert result["col"] == expected


def test_fixed_list_append_literal(fixed_table):
    df = fixed_table.eval_expression_list([col("col").list_append(lit("b"))])
    result = df.to_pydict()

    expected = [
        ["a", "a", "b"],
        ["a", "a", "b"],
        ["a", None, "b"],
        [None, None, "b"],
        ["b"],
        [None, None, "b"],
        ["b"],
        ["b"],
    ]
    assert result["col"] == expected


def test_list_append_with_null_literal():
    """Test appending None to lists - fixes issue #5898."""
    table = MicroPartition.from_pydict(
        {
            "int_list": [[1, 2], [3, 4]],
            "str_list": [["a", "b"], ["c"]],
        }
    )

    # Integer list + None
    result = table.eval_expression_list([col("int_list").list_append(lit(None))])
    assert result.to_pydict()["int_list"] == [[1, 2, None], [3, 4, None]]

    # String list + None
    result = table.eval_expression_list([col("str_list").list_append(lit(None))])
    assert result.to_pydict()["str_list"] == [["a", "b", None], ["c", None]]


def test_list_append_null_column():
    """Test appending a column with null type."""
    table = MicroPartition.from_pydict(
        {
            "col": [[1, 2], [3]],
            "null_col": [None, None],
        }
    )

    result = table.eval_expression_list([col("col").list_append(col("null_col"))])
    assert result.to_pydict()["col"] == [[1, 2, None], [3, None]]


def test_list_append_fixed_size_list_with_null():
    """Test appending None to a fixed size list."""
    table = MicroPartition.from_pydict(
        {
            "col": [["a", "a"], ["a", "a"]],
        }
    )

    fixed_dtype = DataType.fixed_size_list(DataType.string(), 2)
    table = table.eval_expression_list([col("col").cast(fixed_dtype)])

    result = table.eval_expression_list([col("col").list_append(lit(None))])

    # Fixed size list becomes variable length list when appended to
    expected = [["a", "a", None], ["a", "a", None]]
    assert result.to_pydict()["col"] == expected
