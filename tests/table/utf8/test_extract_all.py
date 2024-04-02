from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition

REGEX = r"\d+"


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").str.extract_all(REGEX), ["1 2 3", "4 5 6", "a b c"], [["1", "2", "3"], ["4", "5", "6"], []]),
        (col("col").str.extract_all(lit(REGEX)), ["1 2 3", "4 5 6", "a b c"], [["1", "2", "3"], ["4", "5", "6"], []]),
        (
            col("col").str.extract_all(col("emptystrings") + lit(REGEX)),
            ["1 2 3", "4 5 6", "a b c"],
            [["1", "2", "3"], ["4", "5", "6"], []],
        ),
    ],
)
def test_series_utf8_extract_all(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
