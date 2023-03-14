from __future__ import annotations

import pytest

from daft.expressions2 import col, lit
from daft.table import Table


@pytest.mark.parametrize(
    "expr",
    [
        col("col").str.endswith("foo"),
        col("col").str.endswith(lit("foo")),
        col("col").str.endswith(col("emptystrings") + lit("foo")),
    ],
)
def test_endswith(expr):
    table = Table.from_pydict({"col": ["x_foo", "y_foo", "z_bar"], "emptystrings": ["", "", ""]})
    result = table.eval_expression_list([expr])
    assert result.to_pydict() == {"col": [True, True, False]}
