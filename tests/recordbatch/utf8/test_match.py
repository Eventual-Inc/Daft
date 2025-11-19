from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.recordbatch import MicroPartition

REGEX = r"^\d+$"  # match only digits


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").regexp(REGEX), ["123", "456", "789", "abc"], [True, True, True, False]),
        (col("col").regexp(lit(REGEX)), ["123", "456", "789", "abc"], [True, True, True, False]),
        (
            col("col").regexp(col("emptystrings") + lit(REGEX)),
            ["123", "456", "789", "abc"],
            [True, True, True, False],
        ),
    ],
)
def test_series_utf8_match(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
