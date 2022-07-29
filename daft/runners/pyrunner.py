from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from pyarrow import csv

from daft.logical.logical_plan import LogicalPlan, Projection, Scan
from daft.runners.runner import Runner


class PyRunnerColumnManager:
    def __init__(self) -> None:
        self._nid_to_node_output: Dict[int, NodeOutput] = {}

    def put(self, node_id: int, partition_id: int, column_id: int, column_name: str, values: List) -> None:
        if node_id not in self._nid_to_node_output:
            self._nid_to_node_output[node_id] = NodeOutput({})

        node_output = self._nid_to_node_output[node_id]
        if column_id not in node_output.col_id_to_sharded_column:
            node_output.col_id_to_sharded_column[column_id] = PyListShardedColumn({}, column_name=column_name)

        sharded_column = node_output.col_id_to_sharded_column[column_id]

        assert partition_id not in sharded_column.part_idx_to_tile

        sharded_column.part_idx_to_tile[partition_id] = PyListTile(
            node_id=node_id, column_id=column_id, column_name=column_name, partition_id=partition_id, data=values
        )

    def get(self, node_id: int, partition_id: int, column_id: int) -> PyListTile:
        assert node_id in self._nid_to_node_output
        node_output = self._nid_to_node_output[node_id]

        assert column_id in node_output.col_id_to_sharded_column

        sharded_column = node_output.col_id_to_sharded_column[column_id]

        assert partition_id in sharded_column.part_idx_to_tile

        return sharded_column.part_idx_to_tile[partition_id]

    def rm(self, id: int):
        ...


@dataclass(frozen=True)
class PyListTile:
    node_id: int
    column_id: int
    column_name: str
    partition_id: int
    data: List


@dataclass
class PyListShardedColumn:
    part_idx_to_tile: Dict[int, PyListTile]
    column_name: str


@dataclass
class NodeOutput:
    col_id_to_sharded_column: Dict[int, PyListShardedColumn]


# @dataclass
# class PyListDataset:
#     node_producer: int
#     partitions: Dict[int, PyListPartition]

#     def add_partition(self, part_idx: int, partition: PyListPartition) -> bool:
#         if self.exists(part_idx):
#             raise ValueError(f'index {part_idx} already in PyListDataset')
#         self.partitions[part_idx] = partition
#         return True

#     def add_column(self, part_idx: int, col_id: int, tile: PyListTile):
#         if self.exists(part_idx):

#     def

#     def exists(self, part_idx: int) -> bool:
#         return part_idx in self.partitions


class PyRunner(Runner):
    def __init__(self, plan: LogicalPlan) -> None:
        self._plan = plan
        self._run_order = self._plan.post_order()
        self._col_manager = PyRunnerColumnManager()

    def run(self) -> None:
        for node in self._run_order:
            print(node)
            if isinstance(node, Scan):
                self._handle_scan(node)
            elif isinstance(node, Projection):
                self._handle_projection(node)
        print("\n")
        print(self._col_manager._nid_to_node_output[node.id()])

    def _handle_scan(self, scan: Scan) -> None:
        n_partitions = scan.num_partitions()
        id = scan.id()
        if scan._source_info.scan_type == Scan.ScanType.in_memory:
            assert n_partitions == 1
            raise NotImplementedError()
        elif scan._source_info.scan_type == Scan.ScanType.csv:
            assert isinstance(scan._source_info.source, str)
            schema = scan.schema()
            table = csv.read_csv(scan._source_info.source)
            name_to_pylist = table.to_pydict()
            for expr in schema:
                col_id = expr.get_id()
                col_name = expr = expr.name()
                assert col_name in name_to_pylist
                col_values = name_to_pylist[col_name]
                self._col_manager.put(id, partition_id=0, column_id=col_id, column_name=col_name, values=col_values)

    def _handle_projection(self, proj: Projection) -> None:
        assert proj.num_partitions() == 1
        output = proj.schema()
        id = proj.id()
        child_id = proj._children()[0].id()
        for expr in output:
            if not expr.has_call():
                col_id = expr.get_id()
                assert col_id is not None
                output_name = expr.name()
                prev_node_value = self._col_manager.get(node_id=child_id, partition_id=0, column_id=col_id)
                self._col_manager.put(
                    node_id=id, partition_id=0, column_id=col_id, column_name=output_name, values=prev_node_value.data
                )
            else:
                raise NotImplementedError()

    def logical_node_dispatcher(self, op: LogicalPlan) -> None:
        {
            Scan: self._handle_scan,
        }
