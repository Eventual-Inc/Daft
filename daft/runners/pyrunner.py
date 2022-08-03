from __future__ import annotations

import collections
from bisect import bisect_right
from dataclasses import dataclass
from itertools import accumulate
from typing import Dict, List

import numpy as np
import pyarrow as pa
from pyarrow import csv

from daft.execution.execution_plan import ExecutionPlan
from daft.logical.logical_plan import (
    Filter,
    GlobalLimit,
    LocalLimit,
    LogicalPlan,
    PartitionScheme,
    Projection,
    Repartition,
    Scan,
    Sort,
)
from daft.runners.blocks import DataBlock
from daft.runners.runner import Runner


class PyRunnerColumnManager:
    def __init__(self) -> None:
        self._nid_to_node_output: Dict[int, NodeOutput] = {}

    def put(self, node_id: int, partition_id: int, column_id: int, column_name: str, block: DataBlock) -> None:
        if node_id not in self._nid_to_node_output:
            self._nid_to_node_output[node_id] = NodeOutput({})

        node_output = self._nid_to_node_output[node_id]
        if column_id not in node_output.col_id_to_sharded_column:
            node_output.col_id_to_sharded_column[column_id] = PyListShardedColumn({}, column_name=column_name)

        sharded_column = node_output.col_id_to_sharded_column[column_id]

        sharded_column.part_idx_to_tile[partition_id] = PyListTile(
            node_id=node_id, column_id=column_id, column_name=column_name, partition_id=partition_id, block=block
        )

    def get(self, node_id: int, partition_id: int, column_id: int) -> PyListTile:
        assert node_id in self._nid_to_node_output
        node_output = self._nid_to_node_output[node_id]

        assert (
            column_id in node_output.col_id_to_sharded_column
        ), f"{column_id} not found in {node_output.col_id_to_sharded_column.keys()}"

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
    block: DataBlock


@dataclass
class PyListShardedColumn:
    part_idx_to_tile: Dict[int, PyListTile]
    column_name: str


@dataclass
class NodeOutput:
    col_id_to_sharded_column: Dict[int, PyListShardedColumn]

    def to_arrow_table(self) -> pa.Table:
        values = []
        names = []
        for i in sorted(self.col_id_to_sharded_column.keys()):
            sharded_col = self.col_id_to_sharded_column[i]
            name = sharded_col.column_name
            num_blocks = len(sharded_col.part_idx_to_tile)
            blocks = [sharded_col.part_idx_to_tile[j].block for j in range(num_blocks)]
            merged_block = DataBlock.merge_blocks(blocks)
            names.append(name)
            values.append(merged_block.data)
        return pa.table(values, names=names)


class PyRunner(Runner):
    def __init__(self) -> None:
        self._col_manager = PyRunnerColumnManager()

    def run(self, plan: LogicalPlan) -> NodeOutput:
        exec_plan = ExecutionPlan.plan_from_logical(plan)
        for exec_op in exec_plan.execution_ops:
            for node in exec_op.logical_ops:
                if isinstance(node, Scan):
                    self._handle_scan(node)
                elif isinstance(node, Projection):
                    self._handle_projection(node)
                elif isinstance(node, Filter):
                    self._handle_filter(node)
                elif isinstance(node, LocalLimit):
                    self._handle_local_limit(node)
                elif isinstance(node, GlobalLimit):
                    self._handle_global_limit(node)
                elif isinstance(node, Sort):
                    self._handle_sort(node)
                elif isinstance(node, Repartition):
                    self._handle_repartition(node)
                else:
                    raise NotImplementedError(f"{node} not implemented")
        return self._col_manager._nid_to_node_output[node.id()]

    def _handle_scan(self, scan: Scan) -> None:
        n_partitions = scan.num_partitions()
        node_id = scan.id()
        if scan._source_info.scan_type == Scan.ScanType.IN_MEMORY:
            assert n_partitions == 1
            raise NotImplementedError()
        elif scan._source_info.scan_type == Scan.ScanType.CSV:
            assert isinstance(scan._source_info.source, str)
            schema = scan.schema()
            table = csv.read_csv(scan._source_info.source)
            for expr in schema:
                col_id = expr.get_id()
                col_name = expr = expr.name()
                assert col_name in table.column_names
                col_array = table[col_name]
                self._col_manager.put(
                    node_id=node_id,
                    partition_id=0,
                    column_id=col_id,
                    column_name=col_name,
                    block=DataBlock.make_block(col_array),
                )

    def _handle_projection(self, proj: Projection) -> None:
        output = proj.schema()
        node_id = proj.id()

        for i in range(proj.num_partitions()):
            child_id = proj._children()[0].id()
            for expr in output:
                col_id = expr.get_id()
                output_name = expr.name()
                if not expr.has_call():
                    assert col_id is not None
                    prev_node_value = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id)
                    self._col_manager.put(
                        node_id=node_id,
                        partition_id=i,
                        column_id=col_id,
                        column_name=output_name,
                        block=prev_node_value.block,
                    )
                else:
                    required_cols = expr.required_columns()
                    required_blocks = {}
                    for c in required_cols:
                        block = self._col_manager.get(node_id=child_id, partition_id=i, column_id=c.get_id()).block
                        required_blocks[c.name()] = block
                    result = expr.eval(**required_blocks)

                    self._col_manager.put(
                        node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=result
                    )

    def _handle_filter(self, filter: Filter) -> None:
        predicate = filter._predicate
        node_id = filter.id()
        child_id = filter._children()[0].id()

        for i in range(filter.num_partitions()):
            mask_so_far = None
            for expr in predicate:
                required_cols = expr.required_columns()
                required_blocks = {}
                for c in required_cols:
                    block = self._col_manager.get(node_id=child_id, partition_id=i, column_id=c.get_id()).block
                    required_blocks[c.name()] = block
                mask = expr.eval(**required_blocks)

                if mask_so_far is None:
                    mask_so_far = mask
                else:
                    mask_so_far = mask_so_far & mask

            for expr in filter.schema():
                assert not expr.has_call()
                col_id = expr.get_id()
                output_name = expr.name()
                unfiltered_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
                filtered_column = unfiltered_column.filter(mask_so_far)
                self._col_manager.put(
                    node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=filtered_column
                )

    def _handle_local_limit(self, limit: LocalLimit) -> None:
        num = limit._num
        child_id = limit._children()[0].id()
        node_id = limit.id()
        for i in range(limit.num_partitions()):
            for expr in limit.schema():
                col_id = expr.get_id()
                output_name = expr.name()
                unlimited_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
                limited_column = unlimited_column.head(num)
                self._col_manager.put(
                    node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=limited_column
                )

    def _handle_global_limit(self, limit: GlobalLimit) -> None:
        num = limit._num
        num_partitions = limit.num_partitions()
        child_id = limit._children()[0].id()
        node_id = limit.id()
        first_col_id = next(iter(limit.schema())).get_id()
        assert first_col_id is not None

        size_per_tile = []
        for i in range(num_partitions):
            column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=first_col_id)
            size_per_tile.append(len(column.block))
        cum_sum = list(accumulate(size_per_tile))
        if cum_sum[-1] <= num:
            for i in range(num_partitions):
                for expr in limit.schema():
                    col_id = expr.get_id()
                    output_name = expr.name()
                    unlimited_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
                    limited_column = unlimited_column.head(num)
                    self._col_manager.put(
                        node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=limited_column
                    )
        else:
            what_to_pick_idx = bisect_right(cum_sum, num)
            for i in range(what_to_pick_idx):
                for expr in limit.schema():
                    col_id = expr.get_id()
                    output_name = expr.name()
                    unlimited_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
                    self._col_manager.put(
                        node_id=node_id,
                        partition_id=i,
                        column_id=col_id,
                        column_name=output_name,
                        block=unlimited_column,
                    )
            count_so_far = cum_sum[what_to_pick_idx - 1]
            remainder = num - count_so_far
            assert remainder >= 0
            if remainder > 0:
                for expr in limit.schema():
                    col_id = expr.get_id()
                    output_name = expr.name()
                    unlimited_column = self._col_manager.get(
                        node_id=child_id, partition_id=what_to_pick_idx, column_id=col_id
                    ).block
                    limited_column = unlimited_column.head(remainder)
                    self._col_manager.put(
                        node_id=node_id,
                        partition_id=what_to_pick_idx,
                        column_id=col_id,
                        column_name=output_name,
                        block=limited_column,
                    )
            for i in range(what_to_pick_idx + 1, num_partitions):
                for expr in limit.schema():
                    col_id = expr.get_id()
                    output_name = expr.name()
                    unlimited_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
                    limited_column = unlimited_column.head(0)
                    self._col_manager.put(
                        node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=limited_column
                    )

    def _handle_sort(self, sort: Sort) -> None:
        desc = sort._desc
        child_id = sort._children()[0].id()
        node_id = sort.id()
        num_partitions = sort.num_partitions()
        NUM_SAMPLES_PER_PART = 20
        sampled_sort_keys = []
        sorted_keys_per_part = []

        part_to_column_sorted_by_key: Dict[int, Dict[int, DataBlock]] = collections.defaultdict(lambda: dict())
        assert desc == False
        for i in range(num_partitions):
            assert len(sort._sort_by.exprs) == 1
            expr = sort._sort_by.exprs[0]

            required_cols = expr.required_columns()
            required_blocks = {}
            for c in required_cols:
                block = self._col_manager.get(node_id=child_id, partition_id=i, column_id=c.get_id()).block
                required_blocks[c.name()] = block
            value_to_sort = expr.eval(**required_blocks)

            sort_indices = DataBlock.argsort([value_to_sort], desc=desc)
            sort_key = value_to_sort.take(sort_indices)
            sorted_keys_per_part.append(sort_key)
            for e in sort.schema():
                block = self._col_manager.get(node_id=child_id, partition_id=i, column_id=e.get_id()).block
                part_to_column_sorted_by_key[i][e.get_id()] = block.take(sort_indices)

            size = len(sort_key)
            sample_idx = DataBlock.make_block(data=pa.chunked_array([np.random.randint(0, size, NUM_SAMPLES_PER_PART)]))
            sampled_sort_keys.append(sort_key.take(sample_idx))

        combined_sort_key = DataBlock.merge_blocks(sampled_sort_keys)
        combined_sort_key_argsort_idx = DataBlock.argsort([combined_sort_key])
        combined_sort_key = combined_sort_key.take(combined_sort_key_argsort_idx)
        sample_size = len(combined_sort_key)
        pivot_idx = DataBlock.make_block(
            data=pa.chunked_array(
                [np.linspace(sample_size / num_partitions, sample_size, num_partitions).astype(np.int64)[:-1]]
            )
        )
        pivots = combined_sort_key.take(pivot_idx)

        indices_per_part = [sorted_keys_per_part[i].search_sorted(pivots) for i in range(num_partitions)]

        partition_argsort_idx = []
        to_reduce: List[List[DataBlock]] = [list() for _ in range(num_partitions)]
        for i in range(num_partitions):
            indices = indices_per_part[i]
            source_column = sorted_keys_per_part[i]
            target_partitions = source_column.partition(num=num_partitions, targets=indices)
            for j, t_part in enumerate(target_partitions):
                to_reduce[j].append(t_part)

        for i in range(num_partitions):
            merged_block = DataBlock.merge_blocks(to_reduce[i])
            argsort_idx = DataBlock.argsort([merged_block])
            partition_argsort_idx.append(argsort_idx)

        for expr in sort.schema():
            assert not expr.has_call()
            col_id = expr.get_id()
            output_name = expr.name()
            to_reduce = [list() for _ in range(num_partitions)]
            for i in range(num_partitions):
                indices = indices_per_part[i]
                source_column = part_to_column_sorted_by_key[i][col_id]
                target_partitions = source_column.partition(num=num_partitions, targets=indices)
                for j, t_part in enumerate(target_partitions):
                    to_reduce[j].append(t_part)

            for i in range(num_partitions):
                merged_block = DataBlock.merge_blocks(to_reduce[i])
                idx = partition_argsort_idx[i]
                sorted_order = merged_block.take(idx)
                self._col_manager.put(
                    node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=sorted_order
                )

    def _handle_repartition(self, repartition: Repartition) -> None:
        child_id = repartition._children()[0].id()
        node_id = repartition.id()
        output_schema = repartition.schema()
        assert repartition._scheme == PartitionScheme.ROUND_ROBIN
        source_num_partitions = repartition._children()[0].num_partitions()
        target_num_partitions = repartition.num_partitions()
        first_col_id = next(iter(output_schema)).get_id()
        assert first_col_id is not None
        size_per_tile = []
        for i in range(source_num_partitions):
            column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=first_col_id)
            size_per_tile.append(len(column.block))
        cum_sum = list(accumulate(size_per_tile))
        prefix_sum = [0] + cum_sum[:-1]

        targets = [
            DataBlock.make_block(
                pa.chunked_array([np.arange(prefix_sum[i], prefix_sum[i] + cum_sum[i], 1) % target_num_partitions])
            )
            for i in range(source_num_partitions)
        ]

        for expr in repartition.schema():
            assert not expr.has_call()
            col_id = expr.get_id()
            output_name = expr.name()
            to_reduce: List[List[DataBlock]] = [list() for _ in range(target_num_partitions)]
            for i in range(source_num_partitions):
                source_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
                target_partitions = source_column.partition(num=target_num_partitions, targets=targets[i])
                for j, t_part in enumerate(target_partitions):
                    to_reduce[j].append(t_part)

            for j in range(target_num_partitions):
                merged_block = DataBlock.merge_blocks(to_reduce[j])
                self._col_manager.put(
                    node_id=node_id, partition_id=j, column_id=col_id, column_name=output_name, block=merged_block
                )
