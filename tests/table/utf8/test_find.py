from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data"],
    [
        (col("col").str.find("oo"), ["foo", "quux"]),
        (
            col("col").str.find(lit("oo")),
            ["foo", "quux"],
        ),
        (
            col("col").str.find(col("emptystrings") + lit("oo")),
            ["foo", "quux"],
        ),
    ],
)
def test_series_utf8_find_broadcast_pattern(expr, data) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [1, -1]}
