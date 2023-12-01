from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import MicroPartition

ENDSWITH_DATA = ["x_foo", "y_foo", "z_bar"]
STARTSWITH_DATA = ["foo_x", "foo_y", "bar_z"]
CONTAINS_DATA = ["x_foo_x", "y_foo_y", "z_bar_z"]


@pytest.mark.parametrize(
    ["expr", "data"],
    [
        (col("col").str.endswith("foo"), ENDSWITH_DATA),
        (col("col").str.endswith(lit("foo")), ENDSWITH_DATA),
        (col("col").str.endswith(col("emptystrings") + lit("foo")), ENDSWITH_DATA),
        (col("col").str.startswith("foo"), STARTSWITH_DATA),
        (col("col").str.startswith(lit("foo")), STARTSWITH_DATA),
        (col("col").str.startswith(col("emptystrings") + lit("foo")), STARTSWITH_DATA),
        (col("col").str.contains("foo"), CONTAINS_DATA),
        (col("col").str.contains(lit("foo")), CONTAINS_DATA),
        (col("col").str.contains(col("emptystrings") + lit("foo")), CONTAINS_DATA),
    ],
)
def test_utf8_substrs(expr, data):
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [True, True, False]}
