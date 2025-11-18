from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_rstrip():
    table = MicroPartition.from_pydict({"col": ["\ta\t", None, "\nb\n", "\vc\t", "\td ", "e"]})
    result = table.eval_expression_list(
        [
            daft.functions.rstrip(
                col("col"),
            )
        ]
    )
    assert result.to_pydict() == {"col": ["\ta", None, "\nb", "\vc", "\td", "e"]}
