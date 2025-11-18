from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (daft.functions.rpad(col("col"), 5, "."), ["foo", "abcdef", "quux"], ["foo..", "abcde", "quux."]),
        (daft.functions.rpad(col("col"), lit(5), lit("-")), ["foo", "abcdef", "quux"], ["foo--", "abcde", "quux-"]),
        (
            daft.functions.rpad(col("col"), col("zeroes") + lit(5), col("emptystrings") + lit("-")),
            ["foo", "abcdef", "quux"],
            ["foo--", "abcde", "quux-"],
        ),
    ],
)
def test_series_utf8_rpad_broadcast_pattern(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""], "zeroes": [0, 0, 0]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
