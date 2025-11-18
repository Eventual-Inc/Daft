from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_like():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barBaz", "quux", "1"]})
    result = table.eval_expression_list([daft.functions.like(col("col"), "foo%")])
    assert result.to_pydict() == {"col": [True, None, False, False, False]}
