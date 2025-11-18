from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_repeat():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barBaz", "quux", "1"]})
    result = table.eval_expression_list([daft.functions.repeat(col("col"), 2)])
    assert result.to_pydict() == {"col": ["foofoo", None, "barBazbarBaz", "quuxquux", "11"]}
