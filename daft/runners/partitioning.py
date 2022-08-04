from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, List

import numpy as np
import pyarrow as pa

from daft.expressions import ColID, Expression
from daft.logical.schema import ExpressionList
from daft.runners.blocks import DataBlock

PartID = int


@dataclass(frozen=True)
class PyListTile:
    column_id: ColID
    column_name: str
    partition_id: PartID
    block: DataBlock

    def __len__(self) -> int:
        return len(self.block)

    def apply(self, func: Callable[[DataBlock], DataBlock]) -> PyListTile:
        return dataclasses.replace(self, block=func(self.block))

    def split_by_index(self, num_partitions: int, target_partition_indices: DataBlock) -> List[PyListTile]:
        assert len(target_partition_indices) == len(self)
        new_blocks = self.block.partition(num_partitions, targets=target_partition_indices)
        assert len(new_blocks) == num_partitions
        return [dataclasses.replace(self, block=nb, partition_id=i) for i, nb in enumerate(new_blocks)]


@dataclass(frozen=True)
class vPartition:
    columns: Dict[ColID, PyListTile]
    partition_id: PartID

    def __post_init__(self) -> None:
        size = None
        for col_id, tile in self.columns.items():
            if tile.partition_id != self.partition_id:
                raise ValueError(f"mismatch of partition id: {tile.partition_id} vs {self.partition_id}")
            if size is None:
                size = len(tile)
            if len(tile) != size:
                raise ValueError(f"mismatch of tile lengths: {len(tile)} vs {size}")
            if col_id != tile.column_id:
                raise ValueError(f"mismatch of column id: {col_id} vs {tile.column_id}")

    def __len__(self) -> int:
        assert len(self.columns) > 0
        return len(next(iter(self.columns.values())))

    def eval_expression(self, expr: Expression) -> PyListTile:
        expr_col_id = expr.get_id()
        expr_name = expr.name()

        assert expr_col_id is not None
        assert expr_name is not None

        if not expr.has_call():
            return PyListTile(
                column_id=expr_col_id,
                column_name=expr_name,
                partition_id=self.partition_id,
                block=self.columns[expr_col_id].block,
            )

        required_cols = expr.required_columns()
        required_blocks = {}
        for c in required_cols:
            col_id = c.get_id()
            assert col_id is not None
            block = self.columns[col_id].block
            name = c.name()
            assert name is not None
            required_blocks[name] = block
        result = expr.eval(**required_blocks)
        expr_col_id = expr.get_id()
        expr_name = expr.name()
        assert expr_col_id is not None
        assert expr_name is not None
        return PyListTile(column_id=expr_col_id, column_name=expr_name, partition_id=self.partition_id, block=result)

    def eval_expression_list(self, exprs: ExpressionList) -> vPartition:
        tile_list = [self.eval_expression(e) for e in exprs]
        new_columns = {t.column_id: t for t in tile_list}
        return vPartition(columns=new_columns, partition_id=self.partition_id)

    @classmethod
    def from_arrow_table(cls, table: pa.Table, column_ids: List[ColID], partition_id: PartID) -> vPartition:
        names = table.column_names
        assert len(names) == len(column_ids)
        tiles = {}
        for i, (col_id, name) in enumerate(zip(column_ids, names)):
            arr = table[i]
            block = DataBlock.make_block(arr)
            tiles[col_id] = PyListTile(column_id=col_id, column_name=name, partition_id=partition_id, block=block)
        return vPartition(columns=tiles, partition_id=partition_id)

    def to_arrow_table(self) -> pa.Table:
        values = []
        names = []
        for tile in self.columns.values():
            name = tile.column_name
            names.append(name)
            values.append(tile.block.data)
        return pa.table(values, names=names)

    @classmethod
    def from_pydict(cls, data: Dict[str, List[Any]]) -> vPartition:
        raise NotImplementedError()

    def to_pydict(self) -> Dict[str, List[Any]]:
        raise NotImplementedError()

    def for_each_column_block(self, func: Callable[[DataBlock], DataBlock]) -> vPartition:
        return dataclasses.replace(self, columns={col_id: col.apply(func) for col_id, col in self.columns.items()})

    def head(self, num: int) -> vPartition:
        # TODO make optimization for when num=0
        return self.for_each_column_block(partial(DataBlock.head, num=num))

    def sample(self, num: int) -> vPartition:
        sample_idx = DataBlock.make_block(data=np.random.randint(0, len(self), num))
        return self.for_each_column_block(partial(DataBlock.take, indices=sample_idx))

    def filter(self, predicate: ExpressionList) -> vPartition:
        mask_list = self.eval_expression_list(predicate)
        assert len(mask_list) > 0
        mask = next(iter(mask_list.columns.values())).block
        for to_and in mask_list.columns.values():
            mask &= to_and.block
        return self.for_each_column_block(partial(DataBlock.filter, mask=mask))

    def sort(self, sort_key: Expression, desc: bool = False) -> vPartition:
        sort_tile = self.eval_expression(sort_key)
        argsort_idx = sort_tile.apply(partial(DataBlock.argsort, desc=desc))
        return self.for_each_column_block(partial(DataBlock.take, indices=argsort_idx.block))

    def split_by_index(self, num_partitions: int, target_partition_indices: DataBlock) -> List[vPartition]:
        assert len(target_partition_indices) == len(self)
        new_partition_to_columns: List[Dict[ColID, PyListTile]] = [{} for _ in range(num_partitions)]
        for col_id, tile in self.columns.items():
            new_tiles = tile.split_by_index(
                num_partitions=num_partitions, target_partition_indices=target_partition_indices
            )
            for part_id, nt in enumerate(new_tiles):
                new_partition_to_columns[part_id][col_id] = nt

        return [vPartition(partition_id=i, columns=columns) for i, columns in enumerate(new_partition_to_columns)]


@dataclass
class PartitionSet:
    partitions: Dict[PartID, vPartition]

    def to_arrow_table(self) -> pa.Table:
        partition_ids = sorted(list(self.partitions.keys()))
        assert partition_ids[0] == 0
        assert partition_ids[-1] + 1 == len(partition_ids)
        part_tables = [self.partitions[pid].to_arrow_table() for pid in partition_ids]
        return pa.concat_tables(part_tables)

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> List[int]:
        partition_ids = sorted(list(self.partitions.keys()))
        return [len(self.partitions[pid]) for pid in partition_ids]

    def num_partitions(self) -> int:
        return len(self.partitions)
