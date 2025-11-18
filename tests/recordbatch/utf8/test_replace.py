from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (daft.functions.replace(col("col"), "a", "b"), daft.Series.from_pylist([]).cast(daft.DataType.string()), []),
        (daft.functions.replace(col("col"), "a", "b"), ["a", "ab", "c"], ["b", "bb", "c"]),
        (daft.functions.replace(col("col"), lit("a"), lit("b")), ["a", "ab", "c"], ["b", "bb", "c"]),
        (
            daft.functions.replace(col("col"), col("emptystrings") + lit("a"), col("emptystrings") + lit("b")),
            ["a", "ab", "c"],
            ["b", "bb", "c"],
        ),
        # regex pattern
        (daft.functions.regexp_replace(col("col"), r"a+", "b"), ["aaa", "ab", "c"], ["b", "bb", "c"]),
        (daft.functions.regexp_replace(col("col"), lit(r"a+"), lit("b")), ["aaa", "ab", "c"], ["b", "bb", "c"]),
        (
            daft.functions.regexp_replace(col("col"), col("emptystrings") + lit(r"a+"), col("emptystrings") + lit("b")),
            ["aaa", "ab", "c"],
            ["b", "bb", "c"],
        ),
    ],
)
def test_series_utf8_replace(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": [""] * len(data)})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
