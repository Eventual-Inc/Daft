from __future__ import annotations

from daft.expressions import col
from daft.table import Table


def test_utf8_length():
    table = Table.from_pydict({"col": ["foo", None, "barbaz", "quux"]})
    result = table.eval_expression_list([col("col").str.length()])
    assert result.to_pydict() == {"col": [3, None, 6, 4]}
