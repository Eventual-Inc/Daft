from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_upper():
    table = MicroPartition.from_pydict({"col": ["Foo", None, "BarBaz", "quux", "1"]})
    result = table.eval_expression_list(
        [
            daft.functions.upper(
                col("col"),
            )
        ]
    )
    assert result.to_pydict() == {"col": ["FOO", None, "BARBAZ", "QUUX", "1"]}
