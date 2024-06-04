from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_substr():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barbarbar", "quux", "1", ""]})
    result = table.eval_expression_list([col("col").str.substr(0, 5)])
    assert result.to_pydict() == {"col": ["foo", None, "barba", "quux", "1", None]}
