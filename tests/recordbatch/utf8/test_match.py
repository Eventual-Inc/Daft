from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition

REGEX = r"^\d+$"  # match only digits


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (daft.functions.regexp(col("col"), REGEX), ["123", "456", "789", "abc"], [True, True, True, False]),
        (daft.functions.regexp(col("col"), lit(REGEX)), ["123", "456", "789", "abc"], [True, True, True, False]),
        (
            daft.functions.regexp(col("col"), col("emptystrings") + lit(REGEX)),
            ["123", "456", "789", "abc"],
            [True, True, True, False],
        ),
    ],
)
def test_series_utf8_match(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
