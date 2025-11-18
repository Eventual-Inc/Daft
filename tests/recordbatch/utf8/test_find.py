from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data"],
    [
        (daft.functions.find(col("col"), "oo"), ["foo", "quux"]),
        (
            daft.functions.find(col("col"), lit("oo")),
            ["foo", "quux"],
        ),
        (
            daft.functions.find(col("col"), col("emptystrings") + lit("oo")),
            ["foo", "quux"],
        ),
    ],
)
def test_series_utf8_find_broadcast_pattern(expr, data) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [1, -1]}
