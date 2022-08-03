from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

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


@dataclass(frozen=True)
class vPartition:
    columns: Dict[ColID, PyListTile]
    partition_id: PartID

    def eval_expression(self, expr: Expression) -> PyListTile:
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


@dataclass
class PartitionSet:
    partitions: Dict[PartID, vPartition]
