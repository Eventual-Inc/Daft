from __future__ import annotations

import pytest

from daft.datatype import DataType
from daft.expressions import col
from daft.recordbatch import MicroPartition

# Test data includes various combinations of boolean values and nulls
table = MicroPartition.from_pydict(
    {
        "a": [
            [True, True],  # all True
            [True, False],  # mixed
            [False, False],  # all False
            [True, None],  # True with null
            [False, None],  # False with null
            [None, None],  # all null
            None,  # null list
        ]
    }
)

fixed_dtype = DataType.fixed_size_list(DataType.bool(), 2)
fixed_table = table.eval_expression_list([col("a").cast(fixed_dtype)])


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_bool_and(table):
    result = table.eval_expression_list([col("a").list.bool_and()])
    assert result.to_pydict() == {"a": [True, False, False, True, False, None, None]}


@pytest.mark.parametrize("table", [table, fixed_table])
def test_list_bool_or(table):
    result = table.eval_expression_list([col("a").list.bool_or()])
    assert result.to_pydict() == {"a": [True, True, False, True, False, None, None]}
