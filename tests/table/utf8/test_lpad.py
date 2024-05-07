from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").str.lpad(5, "."), ["foo", "abcdef", "quux"], ["..foo", "abcde", ".quux"]),
        (col("col").str.lpad(lit(5), lit("-")), ["foo", "abcdef", "quux"], ["--foo", "abcde", "-quux"]),
        (
            col("col").str.lpad(col("zeroes") + lit(5), col("emptystrings") + lit("-")),
            ["foo", "abcdef", "quux"],
            ["--foo", "abcde", "-quux"],
        ),
    ],
)
def test_series_utf8_lpad_broadcast_pattern(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""], "zeroes": [0, 0, 0]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
