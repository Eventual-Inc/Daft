from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize(
    ["expr", "data"],
    [
        (col("col").find("oo"), ["foo", "quux"]),
        (
            col("col").find(lit("oo")),
            ["foo", "quux"],
        ),
        (
            col("col").find(col("emptystrings") + lit("oo")),
            ["foo", "quux"],
        ),
    ],
)
def test_series_utf8_find_broadcast_pattern(expr, data) -> None:
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [1, -1]}


def test_utf8_find_unicode_character_index() -> None:
    table = MicroPartition.from_pydict({"col": ["你好世界", "a😀b", "hello"]})
    result = table.eval_expression_list([col("col").find("世")])
    assert result.to_pydict() == {"col": [2, -1, -1]}
    result = table.eval_expression_list([col("col").find("b")])
    assert result.to_pydict() == {"col": [-1, 2, -1]}
