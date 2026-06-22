from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.recordbatch import MicroPartition


@pytest.mark.parametrize("data", [1, "a", True, b"Y", 0.5, None, [1, 2, 3], object()])
def test_broadcast(data):
    table = MicroPartition.from_pydict({"x": [1, 2, 3]})
    new_table = table.eval_expression_list([col("x"), lit(data)])
    assert new_table.to_pydict() == {"x": [1, 2, 3], "literal": [data for _ in range(3)]}


def test_broadcast_fixed_size_list():
    data = [1, 2, 3]
    table = MicroPartition.from_pydict({"x": [1, 2, 3]})
    new_table = table.eval_expression_list(
        [col("x"), lit(data).cast(daft.DataType.fixed_size_list(daft.DataType.int64(), 3))]
    )
    assert new_table.to_pydict() == {"x": [1, 2, 3], "literal": [data for _ in range(3)]}


def test_broadcast_empty_struct():
    # Regression test for https://github.com/Eventual-Inc/Daft/issues/6659
    # daft.lit({}) should broadcast the empty struct to every row.
    df = daft.from_pydict({"id": ["a", "b", "c"]})
    df = df.with_column("meta", daft.lit({}))
    result = df.to_pydict()
    assert result == {"id": ["a", "b", "c"], "meta": [{}, {}, {}]}

    rows = list(df.iter_rows())
    assert rows == [
        {"id": "a", "meta": {}},
        {"id": "b", "meta": {}},
        {"id": "c", "meta": {}},
    ]
