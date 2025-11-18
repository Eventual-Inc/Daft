from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_lower():
    table = MicroPartition.from_pydict({"col": ["Foo", None, "BarBaz", "QUUX"]})
    result = table.eval_expression_list(
        [
            daft.functions.lower(
                col("col"),
            )
        ]
    )
    assert result.to_pydict() == {"col": ["foo", None, "barbaz", "quux"]}
