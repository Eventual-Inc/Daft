from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_capitalize():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barBaz", "quux", "1"]})
    result = table.eval_expression_list([col("col").str.capitalize()])
    assert result.to_pydict() == {"col": ["Foo", None, "Barbaz", "Quux", "1"]}
