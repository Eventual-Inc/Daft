from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.table import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (col("col").str.replace("a", "b"), daft.Series.from_pylist([]).cast(daft.DataType.string()), []),
        (col("col").str.replace("a", "b"), ["a", "ab", "c"], ["b", "bb", "c"]),
        (col("col").str.replace(lit("a"), lit("b")), ["a", "ab", "c"], ["b", "bb", "c"]),
        (
            col("col").str.replace(col("emptystrings") + lit("a"), col("emptystrings") + lit("b")),
            ["a", "ab", "c"],
            ["b", "bb", "c"],
        ),
        # regex pattern
        (col("col").str.replace(r"a+", "b", regex=True), ["aaa", "ab", "c"], ["b", "bb", "c"]),
        (col("col").str.replace(lit(r"a+"), lit("b"), regex=True), ["aaa", "ab", "c"], ["b", "bb", "c"]),
        (
            col("col").str.replace(col("emptystrings") + lit(r"a+"), col("emptystrings") + lit("b"), regex=True),
            ["aaa", "ab", "c"],
            ["b", "bb", "c"],
        ),
    ],
)
def test_series_utf8_replace(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": [""] * len(data)})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
