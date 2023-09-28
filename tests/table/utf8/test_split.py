from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import Table


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").str.split(","), ["a,b,c", "d,e", "f", "g,h"], [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]]),
        (
            col("col").str.split(lit(",")),
            ["a,b,c", "d,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
        (
            col("col").str.split(col("emptystrings") + lit(",")),
            ["a,b,c", "d,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
    ],
)
def test_series_utf8_split_broadcast_pattern(expr, data, expected) -> None:
    table = Table.from_pydict({"col": data, "emptystrings": ["", "", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
