from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition

REGEX = r"^\d"


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (daft.functions.regexp_extract(col("col"), REGEX), ["123", "456", "789", "abc"], ["1", "4", "7", None]),
        (daft.functions.regexp_extract(col("col"), lit(REGEX)), ["123", "456", "789", "abc"], ["1", "4", "7", None]),
        (
            daft.functions.regexp_extract(col("col"), col("emptystrings") + lit(REGEX)),
            ["123", "456", "789", "abc"],
            ["1", "4", "7", None],
        ),
    ],
)
def test_series_utf8_extract(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
