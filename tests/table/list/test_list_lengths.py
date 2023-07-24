from __future__ import annotations

from daft.expressions import col
from daft.table import Table


def test_list_lengths():
    table = Table.from_pydict({"col": [None, [], ["a"], [None], ["a", "a"], ["a", None], ["a", None, "a"]]})
    result = table.eval_expression_list([col("col").list.lengths()])
    assert result.to_pydict() == {"col": [None, 0, 1, 1, 2, 2, 3]}
