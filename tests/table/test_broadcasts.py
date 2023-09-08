from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.table import Table


@pytest.mark.parametrize("data", [1, "a", True, b"Y", 0.5, None, [1, 2, 3], object()])
def test_broadcast(data):
    table = Table.from_pydict({"x": [1, 2, 3]})
    new_table = table.eval_expression_list([col("x"), lit(data)])
    assert new_table.to_pydict() == {"x": [1, 2, 3], "literal": [data for _ in range(3)]}


def test_broadcast_fixed_size_list():
    data = [1, 2, 3]
    table = Table.from_pydict({"x": [1, 2, 3]})
    new_table = table.eval_expression_list(
        [col("x"), lit(data).cast(daft.DataType.fixed_size_list(daft.DataType.int64(), 3))]
    )
    assert new_table.to_pydict() == {"x": [1, 2, 3], "literal": [data for _ in range(3)]}
