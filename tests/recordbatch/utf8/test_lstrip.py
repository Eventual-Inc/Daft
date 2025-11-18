from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_lstrip():
    table = MicroPartition.from_pydict({"col": ["\ta\t", None, "\nb\n", "\vc\t", "\td ", "e"]})
    result = table.eval_expression_list(
        [
            daft.functions.lstrip(
                col("col"),
            )
        ]
    )
    assert result.to_pydict() == {"col": ["a\t", None, "b\n", "c\t", "d ", "e"]}
