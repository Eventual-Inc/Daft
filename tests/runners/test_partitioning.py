from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.expressions import ColID, Expression, col
from daft.logical.schema import ExpressionList
from daft.runners.blocks import ArrowDataBlock, DataBlock
from daft.runners.partitioning import PyListTile, vPartition


def resolve_expr(expr: Expression) -> Expression:
    for c in expr.required_columns():
        c._assign_id(strict=False)
    expr._assign_id(strict=False)
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
    list_of_expr: list[Expression] = []
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
    assert len(part) == 10

    result_vpart = part.eval_expression_list(exprs=expr_list)
    assert len(result_vpart) == 10

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
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(10) * i)


def test_vpartition_from_arrow_table() -> None:
    arrow_table = pa.table([np.ones(10) * i for i in range(4)], names=[f"col_{i}" for i in range(4)])
    vpart = vPartition.from_arrow_table(arrow_table, column_ids=[ColID(i) for i in range(4)], partition_id=0)
    assert len(vpart) == 10
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


def test_vpartition_wrong_col_id() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=i + i, block=block)

    with pytest.raises(ValueError):
        part = vPartition(columns=tiles, partition_id=0)


def test_vpartition_head() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    part = part.head(3)
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(3) * i)


def test_vpartition_sample() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    part = part.sample(3)
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(3) * i)


def test_vpartition_filter() -> None:
    expr = col("x") < 4
    expr = resolve_expr(expr)

    tiles = {}
    col_id = expr.required_columns()[0].get_id()
    for i in range(col_id, col_id + 4):
        block = DataBlock.make_block(np.arange(0, 10, 1))
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    part = part.filter(ExpressionList([expr]))
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(col_id, col_id + 4)]

    for i in range(4):
        assert np.all(arrow_table[i].to_numpy() < 4)


def test_vpartition_sort() -> None:
    expr = col("x")
    expr = resolve_expr(expr)

    tiles = {}
    col_id = expr.required_columns()[0].get_id()

    for i in range(col_id, col_id + 4):
        if i == col_id:
            block = DataBlock.make_block(-np.arange(0, 10, 1))
        else:
            block = DataBlock.make_block(np.arange(0, 10, 1))

        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    part = part.sort(ExpressionList([expr]))
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(col_id, col_id + 4)]

    is_sorted = lambda a: np.all(a[:-1] <= a[1:])
    assert is_sorted(arrow_table[0].to_numpy())
    for i in range(1, 4):
        assert is_sorted(arrow_table[i].to_numpy()[::-1])


def test_vpartition_sort_desc() -> None:
    expr = col("x")
    expr = resolve_expr(expr)

    tiles = {}
    col_id = expr.required_columns()[0].get_id()

    for i in range(col_id, col_id + 4):
        if i == col_id:
            block = DataBlock.make_block(-np.arange(0, 10, 1))
        else:
            block = DataBlock.make_block(np.arange(0, 10, 1))

        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    part = part.sort(ExpressionList([expr]), descending=[True])
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(col_id, col_id + 4)]

    is_sorted = lambda a: np.all(a[:-1] <= a[1:])
    assert is_sorted(arrow_table[0].to_numpy()[::-1])
    for i in range(1, 4):
        assert is_sorted(arrow_table[i].to_numpy())


@pytest.mark.parametrize("n", [1, 2, 3, 4])
def test_split_by_index_even(n) -> None:
    tiles = {}
    for i in range(0, 4):
        block = DataBlock.make_block(np.arange(0, 100, 1))
        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)
    part = vPartition(columns=tiles, partition_id=0)
    new_parts = part.split_by_index(n, DataBlock.make_block(data=np.arange(0, 100, 1) % n))
    assert len(new_parts) == n

    for i, new_part in enumerate(new_parts):
        expected_size = 100 // n
        remainder = 100 % n
        if remainder > 0 and i < remainder:
            expected_size += 1
        assert new_part.partition_id == i
        assert len(new_part) == expected_size
        for col in new_part.columns.values():
            pylist = col.block.iter_py()
            assert all(val % n == i for val in pylist)


@pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 10])
def test_hash_partition(n) -> None:
    expr = col("x")
    expr = resolve_expr(expr)

    tiles = {}
    col_id = expr.required_columns()[0].get_id()

    for i in range(col_id, col_id + 4):
        block = DataBlock.make_block(np.arange(0, 2 * n, 1) % n)

        tiles[i] = PyListTile(column_id=i, column_name=f"col_{i}", partition_id=0, block=block)

    part = vPartition(columns=tiles, partition_id=0)
    new_parts = part.split_by_hash(ExpressionList([expr]), n)
    values_seen = set()
    for i, new_part in enumerate(new_parts):
        assert new_part.partition_id == i
        values_expected = None
        for ncol in new_part.columns.values():
            pylist = list(ncol.block.iter_py())
            for val in pylist:
                assert val not in values_seen
            if values_expected is None:
                values_expected = pylist
            assert values_expected == pylist
        values_seen.update(pylist)


def test_partition_quantiles_small_sample_large_partitions():
    data = [1, 2, 3]
    partition = vPartition({1: PyListTile(1, "foo", 1, ArrowDataBlock(pa.chunked_array([data])))}, 1)
    quantiled_partition = partition.quantiles(10)
    quantile_boundaries = quantiled_partition.columns[1].block.data.to_pylist()
    assert sorted(quantile_boundaries) == quantile_boundaries, "quantile boundaries should be in sorted order"
