from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition

REGEX = r"^\d"


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").str.extract(REGEX), ["123", "456", "789", "abc"], ["1", "4", "7", None]),
        (col("col").str.extract(lit(REGEX)), ["123", "456", "789", "abc"], ["1", "4", "7", None]),
        (
            col("col").str.extract(col("emptystrings") + lit(REGEX)),
            ["123", "456", "789", "abc"],
            ["1", "4", "7", None],
        ),
    ],
)
def test_series_utf8_extract(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
