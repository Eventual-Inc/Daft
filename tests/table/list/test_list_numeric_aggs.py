from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.table import MicroPartition

table = MicroPartition.from_pydict({"a": [[1, 2], [3, 4], [5, None], [None, None], None]})
fixed_dtype = DataType.fixed_size_list(DataType.int64(), 2)
fixed_table = table.eval_expression_list([col("a").cast(fixed_dtype)])


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_sum(table):
    result = table.eval_expression_list([col("a").list.sum()])
    assert result.to_pydict() == {"a": [3, 7, 5, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_mean(table):
    result = table.eval_expression_list([col("a").list.mean()])
    assert result.to_pydict() == {"a": [1.5, 3.5, 5, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_min(table):
    result = table.eval_expression_list([col("a").list.min()])
    assert result.to_pydict() == {"a": [1, 3, 5, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_max(table):
    result = table.eval_expression_list([col("a").list.max()])
    assert result.to_pydict() == {"a": [2, 4, 5, None, None]}
