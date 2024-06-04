from __future__ import annotations

from daft.expressions import col
from daft.table import MicroPartition


def test_utf8_ilike():
    table = MicroPartition.from_pydict({"col": ["foo", None, "barBaz", "quux", "1"]})
    result = table.eval_expression_list([col("col").str.ilike("Foo%")])
    assert result.to_pydict() == {"col": [True, None, False, False, False]}
