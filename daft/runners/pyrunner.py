from __future__ import annotations

import copy
from bisect import bisect_right
from itertools import accumulate
from typing import Dict

from pyarrow import csv

from daft.execution.execution_plan import ExecutionPlan
from daft.logical.logical_plan import (
    Filter,
    GlobalLimit,
    LocalLimit,
    LogicalPlan,
    Projection,
    Repartition,
    Scan,
    Sort,
)
from daft.runners.partitioning import PartitionSet, vPartition
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import RepartitionRandomOp, ShuffleOp, SortOp


class PyRunnerPartitionManager:
    def __init__(self) -> None:
        self._nid_to_partition_set: Dict[int, PartitionSet] = {}

    def put(self, node_id: int, partition_id: int, partition: vPartition) -> None:
        if node_id not in self._nid_to_partition_set:
            self._nid_to_partition_set[node_id] = PartitionSet({})

        pset = self._nid_to_partition_set[node_id]
        pset.partitions[partition_id] = partition

    def get(self, node_id: int, partition_id: int) -> vPartition:
        assert node_id in self._nid_to_partition_set
        pset = self._nid_to_partition_set[node_id]

        assert partition_id in pset.partitions
        return pset.partitions[partition_id]

    def get_partition_set(self, node_id: int) -> PartitionSet:
        assert node_id in self._nid_to_partition_set
        return self._nid_to_partition_set[node_id]

    def put_partition_set(self, node_id: int, pset: PartitionSet) -> None:
        self._nid_to_partition_set[node_id] = pset

    def rm(self, id: int):
        ...


class PyRunnerSimpleShuffler(ShuffleOp):
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}

        source_partitions = input.num_partitions()
        map_results = [
            self.map_fn(input=input.partitions[i], output_partitions=num_target_partitions, **map_args)
            for i in range(source_partitions)
        ]
        reduced_results = []
        for t in range(num_target_partitions):
            reduced_part = self.reduce_fn([map_results[i][t] for i in range(source_partitions)], **reduce_args)
            reduced_results.append(reduced_part)

        return PartitionSet({i: part for i, part in enumerate(reduced_results)})


class PyRunnerRepartitionRandom(PyRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class PyRunnerSortOp(PyRunnerSimpleShuffler, SortOp):
    ...


class PyRunner(Runner):
    def __init__(self) -> None:
        self._part_manager = PyRunnerPartitionManager()

    def run(self, plan: LogicalPlan) -> PartitionSet:
        exec_plan = ExecutionPlan.plan_from_logical(plan)
        for exec_op in exec_plan.execution_ops:

            if exec_op.is_global_op:
                for node in exec_op.logical_ops:
                    if isinstance(node, GlobalLimit):
                        self._handle_global_limit(node)
                    elif isinstance(node, Repartition):
                        self._handle_repartition(node)
                    elif isinstance(node, Sort):
                        self._handle_sort(node)
                    else:
                        raise NotImplementedError(f"{type(node)} not implemented")
            else:
                for i in range(exec_op.num_partitions):
                    for node in exec_op.logical_ops:
                        if isinstance(node, Scan):
                            self._handle_scan(node, partition_id=i)
                        elif isinstance(node, Projection):
                            self._handle_projection(node, partition_id=i)
                        elif isinstance(node, Filter):
                            self._handle_filter(node, partition_id=i)
                        elif isinstance(node, LocalLimit):
                            self._handle_local_limit(node, partition_id=i)
                        else:
                            raise NotImplementedError(f"{type(node)} not implemented")
        return self._part_manager.get_partition_set(node.id())

    def _handle_scan(self, scan: Scan, partition_id: int) -> None:
        n_partitions = scan.num_partitions()
        assert n_partitions == 1
        assert partition_id == 0
        if scan._source_info.scan_type == Scan.ScanType.IN_MEMORY:
            assert n_partitions == 1
            raise NotImplementedError()
        elif scan._source_info.scan_type == Scan.ScanType.CSV:
            assert isinstance(scan._source_info.source, str)
            schema = scan.schema()
            table = csv.read_csv(scan._source_info.source)
            column_ids = [col.get_id() for col in schema.to_column_expressions()]
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            self._part_manager.put(scan.id(), partition_id=partition_id, partition=vpart)

    def _handle_projection(self, proj: Projection, partition_id: int) -> None:
        child_id = proj._children()[0].id()
        prev_partition = self._part_manager.get(child_id, partition_id)
        new_partition = prev_partition.eval_expression_list(proj._projection)
        self._part_manager.put(proj.id(), partition_id=partition_id, partition=new_partition)

    def _handle_filter(self, filter: Filter, partition_id: int) -> None:
        predicate = filter._predicate
        child_id = filter._children()[0].id()
        prev_partition = self._part_manager.get(child_id, partition_id)
        new_partition = prev_partition.filter(predicate)
        self._part_manager.put(filter.id(), partition_id=partition_id, partition=new_partition)

    def _handle_local_limit(self, limit: LocalLimit, partition_id: int) -> None:
        num = limit._num
        child_id = limit._children()[0].id()
        prev_partition = self._part_manager.get(child_id, partition_id)
        new_partition = prev_partition.head(num)
        self._part_manager.put(limit.id(), partition_id=partition_id, partition=new_partition)

    def _handle_global_limit(self, limit: GlobalLimit) -> None:
        num = limit._num
        child_id = limit._children()[0].id()
        prev_pset = self._part_manager.get_partition_set(child_id)
        new_pset = copy.copy(self._part_manager.get_partition_set(child_id))

        size_per_partition = prev_pset.len_of_partitions()
        total_size = sum(size_per_partition)
        if total_size <= num:
            self._part_manager.put_partition_set(limit.id(), prev_pset)
            return

        cum_sum = list(accumulate(size_per_partition))
        where_to_cut_idx = bisect_right(cum_sum, num)
        count_so_far = cum_sum[where_to_cut_idx - 1]
        remainder = num - count_so_far
        assert remainder >= 0
        new_pset.partitions[where_to_cut_idx] = new_pset.partitions[where_to_cut_idx].head(remainder)
        for i in range(where_to_cut_idx + 1, limit.num_partitions()):
            new_pset.partitions[i] = new_pset.partitions[i].head(0)
        self._part_manager.put_partition_set(limit.id(), new_pset)

    def _handle_repartition(self, repartition: Repartition) -> None:
        child_id = repartition._children()[0].id()
        prev_pset = self._part_manager.get_partition_set(child_id)
        repartitioner = PyRunnerRepartitionRandom()
        new_pset = repartitioner.run(input=prev_pset, num_target_partitions=repartition.num_partitions())
        self._part_manager.put_partition_set(repartition.id(), new_pset)

    def _handle_sort(self, sort: Sort) -> None:
        SAMPLES_PER_PARTITION = 20
        num_partitions = sort.num_partitions()
        child_id = sort._children()[0].id()
        prev_pset = self._part_manager.get_partition_set(child_id)
        sampled_partitions = [prev_pset.partitions[i].sample(SAMPLES_PER_PARTITION) for i in range(num_partitions)]
        merged_samples = vPartition.merge_partitions(sampled_partitions, verify_partition_id=False)
        assert len(sort._sort_by.exprs) == 1
        expr = sort._sort_by.exprs[0]
        sampled_sort_key = merged_samples.eval_expression(expr)
        boundaries = sampled_sort_key.block.bucket(num_partitions)

        sort_op = PyRunnerSortOp(map_args={"expr": expr, "boundaries": boundaries}, reduce_args={"expr": expr})
        new_pset = sort_op.run(input=prev_pset, num_target_partitions=num_partitions)
        self._part_manager.put_partition_set(sort.id(), new_pset)

    # def _handle_sort(self, sort: Sort) -> None:
    #     desc = sort._desc
    #     child_id = sort._children()[0].id()
    #     node_id = sort.id()
    #     num_partitions = sort.num_partitions()
    #     NUM_SAMPLES_PER_PART = 20
    #     sampled_sort_keys = []
    #     sorted_keys_per_part = []

    #     part_to_column_sorted_by_key: Dict[int, Dict[int, DataBlock]] = collections.defaultdict(lambda: dict())
    #     assert desc == False
    #     for i in range(num_partitions):
    #         assert len(sort._sort_by.exprs) == 1
    #         expr = sort._sort_by.exprs[0]

    #         required_cols = expr.required_columns()
    #         required_blocks = {}
    #         for c in required_cols:
    #             block = self._col_manager.get(node_id=child_id, partition_id=i, column_id=c.get_id()).block
    #             required_blocks[c.name()] = block
    #         value_to_sort = expr.eval(**required_blocks)

    #         sort_indices = DataBlock.argsort([value_to_sort], desc=desc)
    #         sort_key = value_to_sort.take(sort_indices)
    #         sorted_keys_per_part.append(sort_key)
    #         for e in sort.schema():
    #             block = self._col_manager.get(node_id=child_id, partition_id=i, column_id=e.get_id()).block
    #             part_to_column_sorted_by_key[i][e.get_id()] = block.take(sort_indices)

    #         size = len(sort_key)
    #         sample_idx = DataBlock.make_block(data=pa.chunked_array([np.random.randint(0, size, NUM_SAMPLES_PER_PART)]))
    #         sampled_sort_keys.append(sort_key.take(sample_idx))

    #     combined_sort_key = DataBlock.merge_blocks(sampled_sort_keys)
    #     combined_sort_key_argsort_idx = DataBlock.argsort([combined_sort_key])
    #     combined_sort_key = combined_sort_key.take(combined_sort_key_argsort_idx)
    #     sample_size = len(combined_sort_key)
    #     pivot_idx = DataBlock.make_block(
    #         data=pa.chunked_array(
    #             [np.linspace(sample_size / num_partitions, sample_size, num_partitions).astype(np.int64)[:-1]]
    #         )
    #     )
    #     pivots = combined_sort_key.take(pivot_idx)

    #     indices_per_part = [sorted_keys_per_part[i].search_sorted(pivots) for i in range(num_partitions)]

    #     partition_argsort_idx = []
    #     to_reduce: List[List[DataBlock]] = [list() for _ in range(num_partitions)]
    #     for i in range(num_partitions):
    #         indices = indices_per_part[i]
    #         source_column = sorted_keys_per_part[i]
    #         target_partitions = source_column.partition(num=num_partitions, targets=indices)
    #         for j, t_part in enumerate(target_partitions):
    #             to_reduce[j].append(t_part)

    #     for i in range(num_partitions):
    #         merged_block = DataBlock.merge_blocks(to_reduce[i])
    #         argsort_idx = DataBlock.argsort([merged_block])
    #         partition_argsort_idx.append(argsort_idx)

    #     for expr in sort.schema():
    #         assert not expr.has_call()
    #         col_id = expr.get_id()
    #         output_name = expr.name()
    #         to_reduce = [list() for _ in range(num_partitions)]
    #         for i in range(num_partitions):
    #             indices = indices_per_part[i]
    #             source_column = part_to_column_sorted_by_key[i][col_id]
    #             target_partitions = source_column.partition(num=num_partitions, targets=indices)
    #             for j, t_part in enumerate(target_partitions):
    #                 to_reduce[j].append(t_part)

    #         for i in range(num_partitions):
    #             merged_block = DataBlock.merge_blocks(to_reduce[i])
    #             idx = partition_argsort_idx[i]
    #             sorted_order = merged_block.take(idx)
    #             self._col_manager.put(
    #                 node_id=node_id, partition_id=i, column_id=col_id, column_name=output_name, block=sorted_order
    #             )

    # def _handle_repartition(self, repartition: Repartition) -> None:
    #     child_id = repartition._children()[0].id()
    #     node_id = repartition.id()
    #     output_schema = repartition.schema()
    #     assert repartition._scheme == PartitionScheme.ROUND_ROBIN
    #     source_num_partitions = repartition._children()[0].num_partitions()
    #     target_num_partitions = repartition.num_partitions()
    #     first_col_id = next(iter(output_schema)).get_id()
    #     assert first_col_id is not None
    #     size_per_tile = []
    #     for i in range(source_num_partitions):
    #         column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=first_col_id)
    #         size_per_tile.append(len(column.block))
    #     cum_sum = list(accumulate(size_per_tile))
    #     prefix_sum = [0] + cum_sum[:-1]

    #     targets = [
    #         DataBlock.make_block(
    #             pa.chunked_array([np.arange(prefix_sum[i], prefix_sum[i] + cum_sum[i], 1) % target_num_partitions])
    #         )
    #         for i in range(source_num_partitions)
    #     ]

    #     for expr in repartition.schema():
    #         assert not expr.has_call()
    #         col_id = expr.get_id()
    #         output_name = expr.name()
    #         to_reduce: List[List[DataBlock]] = [list() for _ in range(target_num_partitions)]
    #         for i in range(source_num_partitions):
    #             source_column = self._col_manager.get(node_id=child_id, partition_id=i, column_id=col_id).block
    #             target_partitions = source_column.partition(num=target_num_partitions, targets=targets[i])
    #             for j, t_part in enumerate(target_partitions):
    #                 to_reduce[j].append(t_part)

    #         for j in range(target_num_partitions):
    #             merged_block = DataBlock.merge_blocks(to_reduce[j])
    #             self._col_manager.put(
    #                 node_id=node_id, partition_id=j, column_id=col_id, column_name=output_name, block=merged_block
    #             )
