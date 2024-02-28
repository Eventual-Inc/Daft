from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_reverse():
    table = MicroPartition.from_pydict({"col": ["abc", None, "def", "ghi"]})
    result = table.eval_expression_list([col("col").str.reverse()])
    assert result.to_pydict() == {"col": ["cba", None, "fed", "ihg"]}
