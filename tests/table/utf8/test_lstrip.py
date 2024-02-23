from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_lstrip():
    table = MicroPartition.from_pydict({"col": ["\ta\t", None, "\nb\n", "\vc\t", "\td ", "e"]})
    result = table.eval_expression_list([col("col").str.lstrip()])
    assert result.to_pydict() == {"col": ["a\t", None, "b\n", "c\t", "d ", "e"]}
