from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_upper():
    table = MicroPartition.from_pydict({"col": ["Foo", None, "BarBaz", "quux", "1"]})
    result = table.eval_expression_list([col("col").str.upper()])
    assert result.to_pydict() == {"col": ["FOO", None, "BARBAZ", "QUUX", "1"]}
