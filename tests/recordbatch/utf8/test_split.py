from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").split(","), ["a,b,c", "d,e", "f", "g,h"], [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]]),
        (
            col("col").split(lit(",")),
            ["a,b,c", "d,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
        (
            col("col").split(col("emptystrings") + lit(",")),
            ["a,b,c", "d,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
        (
            col("col").regexp_split(r",+"),
            ["a,,,,b,,,,c", "d,,,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
        (
            col("col").regexp_split(lit(r",+")),
            ["a,,,,b,,,,c", "d,,,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
        (
            col("col").regexp_split(col("emptystrings") + lit(r",+")),
            ["a,,,,b,,,,c", "d,,,e", "f", "g,h"],
            [["a", "b", "c"], ["d", "e"], ["f"], ["g", "h"]],
        ),
    ],
)
def test_series_utf8_split_broadcast_pattern(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
