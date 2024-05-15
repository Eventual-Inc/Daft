from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_like():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barBaz", "quux", "1"]})
    result = table.eval_expression_list([col("col").str.like("foo%")])
    assert result.to_pydict() == {"col": [True, None, False, False, False]}
