from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition

REGEX = r"\d+"


@pytest.mark.parametrize(
    ["expr", "data", "expected"],
    [
        (
            daft.functions.regexp_extract_all(col("col"), REGEX),
            ["1 2 3", "4 5 6", "a b c"],
            [["1", "2", "3"], ["4", "5", "6"], []],
        ),
        (
            daft.functions.regexp_extract_all(col("col"), lit(REGEX)),
            ["1 2 3", "4 5 6", "a b c"],
            [["1", "2", "3"], ["4", "5", "6"], []],
        ),
        (
            daft.functions.regexp_extract_all(col("col"), col("emptystrings") + lit(REGEX)),
            ["1 2 3", "4 5 6", "a b c"],
            [["1", "2", "3"], ["4", "5", "6"], []],
        ),
    ],
)
def test_series_utf8_extract_all(expr, data, expected) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": expected}
