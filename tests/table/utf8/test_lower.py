from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_lower():
    table = MicroPartition.from_pydict({"col": ["Foo", None, "BarBaz", "QUUX"]})
    result = table.eval_expression_list([col("col").str.lower()])
    assert result.to_pydict() == {"col": ["foo", None, "barbaz", "quux"]}
