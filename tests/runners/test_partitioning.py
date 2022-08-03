from typing import List

import numpy as np
import pyarrow as pa
import pytest

from daft.expressions import ColID, Expression, col
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


def test_vpartition_to_arrow_table() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    arrow_table = part.to_arrow_table()
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(10) * i)


def test_vpartition_from_arrow_table() -> None:
    arrow_table = pa.table([np.ones(10) * i for i in range(4)], names=[f"col_{i}" for i in range(4)])
    vpart = vPartition.from_arrow_table(arrow_table, column_ids=[ColID(i) for i in range(4)], partition_id=0)
    for i, (col_id, tile) in enumerate(vpart.columns.items()):
        assert tile.block == DataBlock.make_block(data=np.ones(10) * i)
        assert tile.column_id == col_id
        assert tile.column_name == f"col_{i}"
        assert tile.partition_id == 0


def test_vpartition_uneven_tiles() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10 + i) * i)
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)

    with pytest.raises(ValueError):
        part = vPartition(columns=tiles, partition_id=0)


def test_vpartition_not_same_partition() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=i, block=block)

    with pytest.raises(ValueError):
        part = vPartition(columns=tiles, partition_id=0)
