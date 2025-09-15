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
