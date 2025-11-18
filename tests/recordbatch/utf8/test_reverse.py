from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_reverse():
    table = MicroPartition.from_pydict({"col": ["abc", None, "def", "ghi"]})
    result = table.eval_expression_list(
        [
            daft.functions.reverse(
                col("col"),
            )
        ]
    )
    assert result.to_pydict() == {"col": ["cba", None, "fed", "ihg"]}
