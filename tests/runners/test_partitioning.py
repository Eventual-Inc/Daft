from typing import List

import numpy as np

from daft.expressions import Expression, col
from daft.logical.schema import ExpressionList
from daft.runners.blocks import DataBlock
from daft.runners.partitioning import PyListTile, vPartition


def resolve_expr(expr: Expression) -> Expression:
    for c in expr.required_columns():
        c._assign_id(strict=False)
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


def test_vpartition_eval_expression_list() -> None:
    list_of_expr: List[Expression] = []
    # we do this to have the same col ids for column access
    base_expr = col("a") + col("b")
    base_expr = resolve_expr(base_expr)
    for i in range(4):
        expr = (base_expr + i).alias(f"c_{i}")
        expr = resolve_expr(expr)
        list_of_expr.append(expr)
    expr_list = ExpressionList(list_of_expr)
    tiles = {}
    for c in expr.required_columns():
        block = DataBlock.make_block(np.ones(10))
        tiles[c.get_id()] = PyListTile(column_id=c.get_id(), column_name=c.name(), partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    result_vpart = part.eval_expression_list(exprs=expr_list)

    assert len(result_vpart.columns) == 4
    for i in range(4):
        expr = list_of_expr[i]
        col_id = expr.get_id()
        result_tile = result_vpart.columns[col_id]
        assert result_vpart.partition_id == 0
        assert result_tile.column_id == expr.get_id()
        assert result_tile.column_name == expr.name()
        assert result_tile.block == DataBlock.make_block((np.ones(10) * 2) + i)
