from __future__ import annotations

import pytest

import daft
from daft.expressions import col
from daft.table import Table


def test_list_contains():
    table = Table.from_pydict({"col": [None, [], ["a"], [None], [None, "a"]]})
    result = table.eval_expression_list([col("col").list.contains("a")])
    assert result.to_pydict() == {"col": [None, False, True, False, True]}


def test_list_contains_null():
    table = Table.from_pydict({"col": [None, [], ["a"], [None], [None, "a"]]})
    result = table.eval_expression_list([col("col").list.contains(None)])
    # NOTE: Our contains method currently returns None instead of checking if each element "has none"
    # We might want a different .list.has_null() method?
    assert result.to_pydict() == {"col": [None, None, None, None, None]}


def test_fixed_size_list_contains():
    dt = daft.DataType.fixed_size_list("e", daft.DataType.int64(), 1)
    table = Table.from_pydict({"col": daft.Series.from_pylist([None, [1], [2], [None]]).cast(dt)})
    # TODO(jaychia): Contains not yet implemented for FixedSizeList
    with pytest.raises(ValueError) as err:
        table.eval_expression_list([col("col").list.contains(daft.lit(1))])
    assert "Contains not yet implemented for FixedSizeList." in str(err)


def test_list_contains_other_col():
    table = Table.from_pydict(
        {
            "x": [None, [None], ["a"], ["b"]],
            "y": ["a", "a", "a", "b"],
        }
    )
    result = table.eval_expression_list([col("x").list.contains(col("y"))])
    assert result.to_pydict() == {"x": [None, False, True, True]}
