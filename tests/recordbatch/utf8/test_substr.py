from __future__ import annotations

import daft
from daft.expressions import col
from daft.recordbatch import MicroPartition


def test_utf8_substr():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barbarbar", "quux", "1", ""]})
    result = table.eval_expression_list([daft.functions.substr(col("col"), 0, 5)])
    assert result.to_pydict() == {"col": ["foo", None, "barba", "quux", "1", None]}
