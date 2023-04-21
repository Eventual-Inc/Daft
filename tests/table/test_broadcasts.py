from __future__ import annotations

import pytest

from daft.expressions import col, lit
from daft.table import Table


@pytest.mark.parametrize("data", [1, "a", True, b"Y", 0.5, None, object()])
def test_broadcast(data):
    table = Table.from_pydict({"x": [1, 2, 3]})
    new_table = table.eval_expression_list([col("x"), lit(data)])
    assert new_table.to_pydict() == {"x": [1, 2, 3], "literal": [data for _ in range(3)]}
