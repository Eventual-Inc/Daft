from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition

ENDSWITH_DATA = ["x_foo", "y_foo", "z_bar"]
STARTSWITH_DATA = ["foo_x", "foo_y", "bar_z"]
CONTAINS_DATA = ["x_foo_x", "y_foo_y", "z_bar_z"]


@pytest.mark.parametrize(
    ["expr", "data"],
    [
        (daft.functions.endswith(col("col"), "foo"), ENDSWITH_DATA),
        (daft.functions.endswith(col("col"), lit("foo")), ENDSWITH_DATA),
        (daft.functions.endswith(col("col"), col("emptystrings") + lit("foo")), ENDSWITH_DATA),
        (daft.functions.startswith(col("col"), "foo"), STARTSWITH_DATA),
        (daft.functions.startswith(col("col"), lit("foo")), STARTSWITH_DATA),
        (daft.functions.startswith(col("col"), col("emptystrings") + lit("foo")), STARTSWITH_DATA),
        (daft.functions.contains(col("col"), "foo"), CONTAINS_DATA),
        (daft.functions.contains(col("col"), lit("foo")), CONTAINS_DATA),
        (daft.functions.contains(col("col"), col("emptystrings") + lit("foo")), CONTAINS_DATA),
    ],
)
def test_utf8_substrs(expr, data):
    table = MicroPartition.from_pydict({"col": data, "emptystrings": ["", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [True, True, False]}
