from __future__ import annotations

import pytest

from daft.daft import CountMode
from daft.datatype import DataType
from daft.expressions import col
from daft.table import MicroPartition


@pytest.fixture
def table():
    return MicroPartition.from_pydict({"col": [None, [], ["a"], [None], ["a", "a"], ["a", None], ["a", None, "a"]]})


@pytest.fixture
def fixed_table():
    table = MicroPartition.from_pydict({"col": [["a", "a"], ["a", "a"], ["a", None], [None, None], None]})
    fixed_dtype = DataType.fixed_size_list(DataType.string(), 2)
    return table.eval_expression_list([col("col").cast(fixed_dtype)])


def test_list_lengths(table):
    result = table.eval_expression_list([col("col").list.lengths()])
    assert result.to_pydict() == {"col": [None, 0, 1, 1, 2, 2, 3]}


def test_fixed_list_lengths(fixed_table):
    result = fixed_table.eval_expression_list([col("col").list.lengths()])
    assert result.to_pydict() == {"col": [2, 2, 2, 2, None]}


def test_list_count(table):
    result = table.eval_expression_list([col("col").list.count(CountMode.All)])
    assert result.to_pydict() == {"col": [None, 0, 1, 1, 2, 2, 3]}

    result = table.eval_expression_list([col("col").list.count(CountMode.Valid)])
    assert result.to_pydict() == {"col": [None, 0, 1, 0, 2, 1, 2]}

    result = table.eval_expression_list([col("col").list.count(CountMode.Null)])
    assert result.to_pydict() == {"col": [None, 0, 0, 1, 0, 1, 1]}


def test_fixed_list_count(fixed_table):
    result = fixed_table.eval_expression_list([col("col").list.count(CountMode.All)])
    assert result.to_pydict() == {"col": [2, 2, 2, 2, None]}

    result = fixed_table.eval_expression_list([col("col").list.count(CountMode.Valid)])
    assert result.to_pydict() == {"col": [2, 2, 1, 0, None]}

    result = fixed_table.eval_expression_list([col("col").list.count(CountMode.Null)])
    assert result.to_pydict() == {"col": [0, 0, 1, 2, None]}
