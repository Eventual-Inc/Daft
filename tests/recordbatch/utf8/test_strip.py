from __future__ import annotations

from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_strip():
    table = MicroPartition.from_pydict({"col": ["\ta\t", None, "\nb\n", "\vc\t", "\td ", "\ne", "f\n", "g"]})
    result = table.eval_expression_list([col("col").strip()])
    assert result.to_pydict() == {"col": ["a", None, "b", "c", "d", "e", "f", "g"]}
