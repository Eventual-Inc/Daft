import numpy as np

from daft.expressions import Expression, col
from daft.runners.blocks import DataBlock
from daft.runners.partitioning import PyListTile, vPartition


def resolve_expr(expr: Expression) -> Expression:
    for c in expr.required_columns():
        c._assign_id()
    expr._assign_id()
    return expr


def test_vpartition_eval_expression() -> None:
    expr = (col("a") + col("b")).alias("c")
    expr = resolve_expr(expr)
    tiles = {}
    for c in expr.required_columns():
        block = DataBlock.make_block(np.ones(10))
        tiles[c.get_id()] = PyListTile(column_id=c.get_id(), column_name=c.name(), partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    result_tile = part.eval_expression(expr=expr)
    assert result_tile.partition_id == 0
    assert result_tile.column_id == expr.get_id()
    assert result_tile.column_name == expr.name()
    assert result_tile.block == DataBlock.make_block(np.ones(10) * 2)
