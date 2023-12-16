from __future__ import annotations

import pytest

from daft.expressions import col
from daft.table import MicroPartition


def test_list_join():
    table = MicroPartition.from_pydict({"col": [None, [], ["a"], [None], ["a", "a"], ["a", None], ["a", None, "a"]]})
    result = table.eval_expression_list([col("col").list.join(",")])
    assert result.to_pydict() == {"col": [None, "", "a", "", "a,a", "a,", "a,,a"]}


def test_list_join_other_col():
    table = MicroPartition.from_pydict(
        {
            "col": [None, [], ["a"], [None], ["a", "a"], ["a", None], ["a", None, "a"]],
            "delimiter": ["1", "2", "3", "4", "5", "6", "7"],
        }
    )
    result = table.eval_expression_list([col("col").list.join(col("delimiter"))])
    assert result.to_pydict() == {"col": [None, "", "a", "", "a5a", "a6", "a77a"]}


def test_list_join_bad_type():
    table = MicroPartition.from_pydict({"col": [1, 2, 3]})
    with pytest.raises(ValueError):
        table.eval_expression_list([col("col").list.join(",")])

    table = MicroPartition.from_pydict({"col": [[1, 2, 3], [4, 5, 6], []]})
    with pytest.raises(ValueError):
        table.eval_expression_list([col("col").list.join(",")])
