from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_rstrip():
    table = MicroPartition.from_pydict({"col": ["\ta\t", None, "\nb\n", "\vc\t", "\td ", "e"]})
    result = table.eval_expression_list([col("col").str.rstrip()])
    assert result.to_pydict() == {"col": ["\ta", None, "\nb", "\vc", "\td", "e"]}
