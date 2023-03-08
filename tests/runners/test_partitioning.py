from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from daft.expressions import Expression, ExpressionList, col
from daft.runners.blocks import ArrowDataBlock, DataBlock
from daft.runners.partitioning import PyListTile, vPartition


def test_vpartition_eval_expression() -> None:
    expr = (col("a") + col("b")).alias("c")
    tiles = {}
    for c in expr._required_columns():
        block = DataBlock.make_block(np.ones(10))
        tiles[c] = PyListTile(column_name=c, block=block)
    part = vPartition(columns=tiles)
    result_tile = part.eval_expression(expr=expr)
    assert result_tile.column_name == expr.name()
    assert result_tile.block == DataBlock.make_block(np.ones(10) * 2)


def test_vpartition_eval_expression_list() -> None:
    list_of_expr: list[Expression] = []
    # we do this to have the same col ids for column access
    base_expr = col("a") + col("b")
    for i in range(4):
        expr = (base_expr + i).alias(f"c_{i}")
        list_of_expr.append(expr)
    expr_list = ExpressionList(list_of_expr)
    tiles = {}
    for c in expr._required_columns():
        block = DataBlock.make_block(np.ones(10))
        tiles[c] = PyListTile(column_name=c, block=block)
    part = vPartition(columns=tiles)
    assert len(part) == 10

    result_vpart = part.eval_expression_list(exprs=expr_list)
    assert len(result_vpart) == 10

    assert len(result_vpart.columns) == 4
    for i in range(4):
        expr = list_of_expr[i]
        col_name = expr.name()
        result_tile = result_vpart.columns[col_name]
        assert result_tile.column_name == expr.name()
        assert result_tile.block == DataBlock.make_block((np.ones(10) * 2) + i)


def test_vpartition_to_arrow_table() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)
    part = vPartition(columns=tiles)
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(10) * i)


def test_vpartition_from_arrow_table() -> None:
    arrow_table = pa.table([np.ones(10) * i for i in range(4)], names=[f"col_{i}" for i in range(4)])
    vpart = vPartition.from_arrow_table(arrow_table)
    assert len(vpart) == 10
    for i, (col_name, tile) in enumerate(vpart.columns.items()):
        assert tile.block == DataBlock.make_block(data=np.ones(10) * i)
        assert tile.column_name == col_name


def test_vpartition_uneven_tiles() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10 + i) * i)
        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)

    with pytest.raises(ValueError):
        part = vPartition(columns=tiles)


def test_vpartition_head() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)
    part = vPartition(columns=tiles)
    part = part.head(3)
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(3) * i)


def test_vpartition_sample() -> None:
    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.ones(10) * i)
        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)
    part = vPartition(columns=tiles)
    part = part.sample(3)
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i] == np.ones(3) * i)


def test_vpartition_filter() -> None:
    expr = col("col_0") < 4

    # Need to make sure there are no conflicts with column ID of `expr`

    tiles = {}
    for i in range(4):
        block = DataBlock.make_block(np.arange(0, 10, 1))
        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)

    part = vPartition(columns=tiles)
    part = part.filter(ExpressionList([expr]))
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    for i in range(4):
        assert np.all(arrow_table[i].to_numpy() < 4)


def test_vpartition_sort() -> None:
    expr = col("col_0")

    tiles = {}

    for i in range(4):
        if i == 0:
            block = DataBlock.make_block(-np.arange(0, 10, 1))
        else:
            block = DataBlock.make_block(np.arange(0, 10, 1))

        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)
    part = vPartition(columns=tiles)
    part = part.sort(ExpressionList([expr]))
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    is_sorted = lambda a: np.all(a[:-1] <= a[1:])
    assert is_sorted(arrow_table[0].to_numpy())
    for i in range(1, 4):
        assert is_sorted(arrow_table[i].to_numpy()[::-1])


def test_vpartition_sort_desc() -> None:
    expr = col("col_0")

    tiles = {}

    for i in range(4):
        if i == 0:
            block = DataBlock.make_block(-np.arange(0, 10, 1))
        else:
            block = DataBlock.make_block(np.arange(0, 10, 1))

        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)
    part = vPartition(columns=tiles)
    part = part.sort(ExpressionList([expr]), descending=[True])
    arrow_table = pa.Table.from_pandas(part.to_pandas())
    assert arrow_table.column_names == [f"col_{i}" for i in range(4)]

    is_sorted = lambda a: np.all(a[:-1] <= a[1:])
    assert is_sorted(arrow_table[0].to_numpy()[::-1])
    for i in range(1, 4):
        assert is_sorted(arrow_table[i].to_numpy())


@pytest.mark.parametrize("n", [1, 2, 3, 4])
def test_split_by_index_even(n) -> None:
    tiles = {}
    for i in range(0, 4):
        block = DataBlock.make_block(np.arange(0, 100, 1))
        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)
    part = vPartition(columns=tiles)
    new_parts = part.split_by_index(n, DataBlock.make_block(data=np.arange(0, 100, 1) % n))
    assert len(new_parts) == n

    for i, new_part in enumerate(new_parts):
        expected_size = 100 // n
        remainder = 100 % n
        if remainder > 0 and i < remainder:
            expected_size += 1
        assert len(new_part) == expected_size
        for col in new_part.columns.values():
            pylist = col.block.iter_py()
            assert all(val % n == i for val in pylist)


@pytest.mark.parametrize("n", [1, 2, 3, 4, 5, 10])
def test_hash_partition(n) -> None:

    tiles = {}

    for i in range(4):
        block = DataBlock.make_block(np.arange(0, 2 * n, 1) % n)

        tiles[f"col_{i}"] = PyListTile(column_name=f"col_{i}", block=block)

    part = vPartition(columns=tiles)
    new_parts = part.split_by_hash(ExpressionList([col("col_0")]), n)
    values_seen = set()
    for i, new_part in enumerate(new_parts):
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
    partition = vPartition({"foo": PyListTile("foo", ArrowDataBlock(pa.chunked_array([data])))})
    quantiled_partition = partition.quantiles(10)
    quantile_boundaries = quantiled_partition.columns["foo"].block.data.to_pylist()
    assert sorted(quantile_boundaries) == quantile_boundaries, "quantile boundaries should be in sorted order"
