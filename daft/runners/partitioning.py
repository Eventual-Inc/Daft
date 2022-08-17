from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa

from daft.expressions import ColID, Expression, ExpressionExecutor
from daft.logical.schema import ExpressionList
from daft.runners.blocks import ArrowArrType, DataBlock, PyListDataBlock

from ..execution.operators import OperatorEnum, PythonExpressionType

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

    @classmethod
    def merge_tiles(cls, to_merge: List[PyListTile], verify_partition_id: bool = True) -> PyListTile:
        assert len(to_merge) > 0

        if len(to_merge) == 1:
            return to_merge[0]

        column_id = to_merge[0].column_id
        partition_id = to_merge[0].partition_id
        column_name = to_merge[0].column_name
        # first perform sanity check
        for part in to_merge[1:]:
            assert part.column_id == column_id
            assert not verify_partition_id or part.partition_id == partition_id
            assert part.column_name == column_name

        merged_block = DataBlock.merge_blocks([t.block for t in to_merge])
        return dataclasses.replace(to_merge[0], block=merged_block)


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
        exec = ExpressionExecutor()
        result = exec.eval(expr, required_blocks)
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
            block: DataBlock[ArrowArrType] = DataBlock.make_block(arr)
            tiles[col_id] = PyListTile(column_id=col_id, column_name=name, partition_id=partition_id, block=block)
        return vPartition(columns=tiles, partition_id=partition_id)

    @classmethod
    def from_pydict(cls, data: Dict[str, List[Any]], schema: ExpressionList, partition_id: PartID) -> vPartition:
        column_exprs = schema.to_column_expressions()
        tiles = {}
        for col_expr in column_exprs:
            col_id = col_expr.get_id()
            col_name = col_expr.name()
            arr = (
                data[col_name]
                if isinstance(col_expr.resolved_type(), PythonExpressionType)
                else pa.array(data[col_expr.name()])
            )
            block: DataBlock = DataBlock.make_block(arr)
            tiles[col_id] = PyListTile(column_id=col_id, column_name=col_name, partition_id=partition_id, block=block)
        return vPartition(columns=tiles, partition_id=partition_id)

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                tile.column_name: pd.Series(tile.block.data)
                if isinstance(tile.block, PyListDataBlock)
                else tile.block.data.to_pandas()
                for tile in self.columns.values()
            }
        )

    def for_each_column_block(self, func: Callable[[DataBlock], DataBlock]) -> vPartition:
        return dataclasses.replace(self, columns={col_id: col.apply(func) for col_id, col in self.columns.items()})

    def head(self, num: int) -> vPartition:
        # TODO make optimization for when num=0
        return self.for_each_column_block(partial(DataBlock.head, num=num))

    def sample(self, num: int) -> vPartition:
        if len(self) == 0:
            return self
        sample_idx: DataBlock[ArrowArrType] = DataBlock.make_block(data=np.random.randint(0, len(self), num))
        return self.for_each_column_block(partial(DataBlock.take, indices=sample_idx))

    def filter(self, predicate: ExpressionList) -> vPartition:
        mask_list = self.eval_expression_list(predicate)
        assert len(mask_list.columns) > 0
        mask = next(iter(mask_list.columns.values())).block
        for to_and in mask_list.columns.values():
            mask = mask.run_binary_operator(to_and.block, OperatorEnum.AND)
        return self.for_each_column_block(partial(DataBlock.filter, mask=mask))

    def sort(self, sort_key: Expression, desc: bool = False) -> vPartition:
        sort_tile = self.eval_expression(sort_key)
        argsort_idx = sort_tile.apply(partial(DataBlock.argsort, desc=desc))
        return self.take(argsort_idx.block)

    def take(self, indices: DataBlock) -> vPartition:
        return self.for_each_column_block(partial(DataBlock.take, indices=indices))

    def agg(self, to_agg: List[Tuple[Expression, str]], group_by: Optional[ExpressionList] = None) -> vPartition:
        evaled_expressions = self.eval_expression_list(ExpressionList([e for e, _ in to_agg]))
        ops = [op for _, op in to_agg]
        if group_by is None:
            agged = {}
            for op, (col_id, tile) in zip(ops, evaled_expressions.columns.items()):
                agged[col_id] = tile.apply(func=partial(tile.block.__class__.agg, op=op))
            return vPartition(partition_id=self.partition_id, columns=agged)
        else:
            grouped_blocked = self.eval_expression_list(group_by)
            assert len(evaled_expressions.columns) == len(ops)
            gcols, acols = DataBlock.group_by_agg(
                list(tile.block for tile in grouped_blocked.columns.values()),
                list(tile.block for tile in evaled_expressions.columns.values()),
                agg_ops=ops,
            )
            new_columns = {}

            for block, (col_id, tile) in zip(gcols, grouped_blocked.columns.items()):
                new_columns[col_id] = dataclasses.replace(tile, block=block)

            for block, (col_id, tile) in zip(acols, evaled_expressions.columns.items()):
                new_columns[col_id] = dataclasses.replace(tile, block=block)
            return vPartition(partition_id=self.partition_id, columns=new_columns)

    def split_by_hash(self, exprs: ExpressionList, num_partitions: int) -> List[vPartition]:
        values_to_hash = self.eval_expression_list(exprs)
        keys = list(values_to_hash.columns.keys())
        keys.sort()
        hsf = None
        assert len(keys) > 0
        for k in keys:
            block = values_to_hash.columns[k].block
            hsf = block.array_hash(seed=hsf)
        assert hsf is not None
        target_idx = hsf.run_binary_operator(num_partitions, OperatorEnum.MOD)
        return self.split_by_index(num_partitions, target_partition_indices=target_idx)

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

    @classmethod
    def merge_partitions(cls, to_merge: List[vPartition], verify_partition_id: bool = True) -> vPartition:
        assert len(to_merge) > 0

        if len(to_merge) == 1:
            return to_merge[0]

        pid = to_merge[0].partition_id
        col_ids = set(to_merge[0].columns.keys())
        # first perform sanity check
        for part in to_merge[1:]:
            assert not verify_partition_id or part.partition_id == pid
            assert set(part.columns.keys()) == col_ids

        new_columns = {}
        for col_id in to_merge[0].columns.keys():
            new_columns[col_id] = PyListTile.merge_tiles(
                [vp.columns[col_id] for vp in to_merge], verify_partition_id=verify_partition_id
            )
        return dataclasses.replace(to_merge[0], columns=new_columns)


@dataclass
class PartitionSet:
    partitions: Dict[PartID, vPartition]

    def to_pandas(self) -> pd.DataFrame:
        partition_ids = sorted(list(self.partitions.keys()))
        assert partition_ids[0] == 0
        assert partition_ids[-1] + 1 == len(partition_ids)
        part_dfs = [self.partitions[pid].to_pandas() for pid in partition_ids]
        return pd.concat(part_dfs, ignore_index=True)

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> List[int]:
        partition_ids = sorted(list(self.partitions.keys()))
        return [len(self.partitions[pid]) for pid in partition_ids]

    def num_partitions(self) -> int:
        return len(self.partitions)
