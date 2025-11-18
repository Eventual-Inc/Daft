from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_length():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barbaz", "quux", "😉test", ""]})
    result = table.eval_expression_list(
        [
            daft.functions.length(
                col("col"),
            )
        ]
    )
    assert result.to_pydict() == {"col": [3, None, 6, 4, 5, 0]}
