from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_right():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barBaz", "quux", "1"]})
    result = table.eval_expression_list([col("col").str.right(3)])
    assert result.to_pydict() == {"col": ["foo", None, "Baz", "uux", "1"]}
