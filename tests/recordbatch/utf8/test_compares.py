from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.recordbatch import MicroPartition

ENDSWITH_DATA = ["x_foo", "y_foo", "z_bar"]
STARTSWITH_DATA = ["foo_x", "foo_y", "bar_z"]
CONTAINS_DATA = ["x_foo_x", "y_foo_y", "z_bar_z"]


@pytest.mark.parametrize(
    ["expr", "data"],
    [
        (col("col").endswith("foo"), ENDSWITH_DATA),
        (col("col").endswith(lit("foo")), ENDSWITH_DATA),
        (col("col").endswith(col("emptystrings") + lit("foo")), ENDSWITH_DATA),
        (col("col").startswith("foo"), STARTSWITH_DATA),
        (col("col").startswith(lit("foo")), STARTSWITH_DATA),
        (col("col").startswith(col("emptystrings") + lit("foo")), STARTSWITH_DATA),
        (col("col").contains("foo"), CONTAINS_DATA),
        (col("col").contains(lit("foo")), CONTAINS_DATA),
        (col("col").contains(col("emptystrings") + lit("foo")), CONTAINS_DATA),
    ],
)
def test_utf8_substrs(expr, data):
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [True, True, False]}
