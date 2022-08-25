from __future__ import annotations

import copy
from bisect import bisect_right
from itertools import accumulate
from typing import Dict, List, Optional

from pyarrow import csv, parquet

from daft.datasources import (
    CSVSourceInfo,
    InMemorySourceInfo,
    ParquetSourceInfo,
    ScanType,
)
from daft.execution.execution_plan import ExecutionPlan
from daft.filesystem import get_filesystem_from_path
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import (
    Coalesce,
    Filter,
    GlobalLimit,
    Join,
    LocalAggregate,
    LocalLimit,
    LogicalPlan,
    PartitionScheme,
    Projection,
    Repartition,
    Scan,
    Sort,
)
from daft.logical.optimizer import (
    DropRepartition,
    FoldProjections,
    PushDownLimit,
    PushDownPredicates,
)
from daft.runners.partitioning import PartitionSet, vPartition
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    SortOp,
)


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

    def rm(self, node_id: int, partition_id: Optional[int] = None):
        if partition_id is None:
            del self._nid_to_partition_set[node_id]
        else:
            del self._nid_to_partition_set[node_id].partitions[partition_id]
            if len(self._nid_to_partition_set[node_id].partitions) == 0:
                del self._nid_to_partition_set[node_id]


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
            reduced_part = self.reduce_fn(
                [map_results[i][t] for i in range(source_partitions) if t in map_results[i]], **reduce_args
            )
            reduced_results.append(reduced_part)

        return PartitionSet({i: part for i, part in enumerate(reduced_results)})


class PyRunnerRepartitionRandom(PyRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class PyRunnerRepartitionHash(PyRunnerSimpleShuffler, RepartitionHashOp):
    ...


class PyRunnerCoalesceOp(PyRunnerSimpleShuffler, CoalesceOp):
    ...


class PyRunnerSortOp(PyRunnerSimpleShuffler, SortOp):
    ...


class LogicalPartitionOpRunner:
    def run_node_list(self, inputs: Dict[int, vPartition], nodes: List[LogicalPlan], partition_id: int) -> vPartition:
        part_set = inputs.copy()
        for node in nodes:
            output = self.run_single_node(inputs=part_set, node=node, partition_id=partition_id)
            part_set[node.id()] = output
            for child in node._children():
                del part_set[child.id()]
        return output

    def run_single_node(self, inputs: Dict[int, vPartition], node: LogicalPlan, partition_id: int) -> vPartition:
        if isinstance(node, Scan):
            return self._handle_scan(inputs, node, partition_id=partition_id)
        elif isinstance(node, Projection):
            return self._handle_projection(inputs, node, partition_id=partition_id)
        elif isinstance(node, Filter):
            return self._handle_filter(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalLimit):
            return self._handle_local_limit(inputs, node, partition_id=partition_id)
        elif isinstance(node, LocalAggregate):
            return self._handle_local_aggregate(inputs, node, partition_id=partition_id)
        elif isinstance(node, Join):
            return self._handle_join(inputs, node, partition_id=partition_id)
        else:
            raise NotImplementedError(f"{type(node)} not implemented")

    def _handle_scan(self, inputs: Dict[int, vPartition], scan: Scan, partition_id: int) -> vPartition:
        schema = scan.schema()
        column_ids = [col.get_id() for col in schema.to_column_expressions()]
        if scan._source_info.scan_type() == ScanType.IN_MEMORY:
            assert isinstance(scan._source_info, InMemorySourceInfo)
            table_len = [len(scan._source_info.data[key]) for key in scan._source_info.data][0]
            partition_size = table_len // scan._source_info.num_partitions
            start, end = (partition_size * partition_id, partition_size * (partition_id + 1))
            data = {key: scan._source_info.data[key][start:end] for key in scan._source_info.data}
            vpart = vPartition.from_pydict(data, schema=schema, partition_id=partition_id)
            return vpart
        elif scan._source_info.scan_type() == ScanType.CSV:
            assert isinstance(scan._source_info, CSVSourceInfo)
            path = scan._source_info.filepaths[partition_id]
            fs = get_filesystem_from_path(path)
            table = csv.read_csv(
                fs.open(path),
                parse_options=csv.ParseOptions(
                    delimiter=scan._source_info.delimiter,
                ),
                read_options=csv.ReadOptions(
                    column_names=[expr.name() for expr in schema],
                    skip_rows_after_names=1 if scan._source_info.has_headers else 0,
                ),
            )
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            return vpart
        elif scan._source_info.scan_type() == ScanType.PARQUET:
            assert isinstance(scan._source_info, ParquetSourceInfo)
            table = parquet.read_table(scan._source_info.filepaths[partition_id])
            vpart = vPartition.from_arrow_table(table, column_ids=column_ids, partition_id=partition_id)
            return vpart
        else:
            raise NotImplementedError(f"PyRunner has not implemented scan: {scan._source_info.scan_type()}")

    def _handle_projection(self, inputs: Dict[int, vPartition], proj: Projection, partition_id: int) -> vPartition:
        child_id = proj._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.eval_expression_list(proj._projection)

    def _handle_filter(self, inputs: Dict[int, vPartition], filter: Filter, partition_id: int) -> vPartition:
        predicate = filter._predicate
        child_id = filter._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.filter(predicate)

    def _handle_local_limit(self, inputs: Dict[int, vPartition], limit: LocalLimit, partition_id: int) -> vPartition:
        num = limit._num
        child_id = limit._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.head(num)

    def _handle_local_aggregate(
        self, inputs: Dict[int, vPartition], agg: LocalAggregate, partition_id: int
    ) -> vPartition:
        child_id = agg._children()[0].id()
        prev_partition = inputs[child_id]
        return prev_partition.agg(agg._agg, group_by=agg._group_by)

    def _handle_join(self, inputs: Dict[int, vPartition], join: Join, partition_id: int) -> vPartition:
        left_id = join._children()[0].id()
        right_id = join._children()[1].id()
        left_partition = inputs[left_id]
        right_partition = inputs[right_id]
        return left_partition.join(
            right_partition,
            left_on=join._left_on,
            right_on=join._right_on,
            output_schema=join.schema(),
            how=join._how.value,
        )


class PyRunner(Runner):
    def __init__(self) -> None:
        self._part_manager = PyRunnerPartitionManager()
        self._part_op_runner = LogicalPartitionOpRunner()
        self._optimizer = RuleRunner(
            [
                RuleBatch(
                    "SinglePassPushDowns",
                    Once,
                    [PushDownPredicates(), FoldProjections(), DropRepartition()],
                ),
                RuleBatch(
                    "PushDownLimits",
                    FixedPointPolicy(3),
                    [PushDownLimit()],
                ),
            ]
        )

    def run(self, plan: LogicalPlan) -> PartitionSet:
        plan = self._optimizer.optimize(plan)
        # plan.to_dot_file()
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
                    elif isinstance(node, Coalesce):
                        self._handle_coalesce(node)
                    else:
                        raise NotImplementedError(f"{type(node)} not implemented")
                    for child in node._children():
                        self._part_manager.rm(child.id())
            else:
                data_deps = exec_op.data_deps
                for i in range(exec_op.num_partitions):
                    try:
                        inputs = {nid: self._part_manager.get(nid, i) for nid in data_deps}
                    except:
                        import ipdb

                        ipdb.set_trace()
                    result = self._part_op_runner.run_node_list(inputs, nodes=exec_op.logical_ops, partition_id=i)
                    self._part_manager.put(exec_op.logical_ops[-1].id(), i, result)
        last = exec_plan.execution_ops[-1].logical_ops[-1]
        return self._part_manager.get_partition_set(last.id())

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
        repartitioner: PyRunnerSimpleShuffler
        if repartition._scheme == PartitionScheme.RANDOM:
            repartitioner = PyRunnerRepartitionRandom()
        elif repartition._scheme == PartitionScheme.HASH:
            repartitioner = PyRunnerRepartitionHash(map_args={"exprs": repartition._partition_by.exprs})
        else:
            raise NotImplementedError()
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
        boundaries = sampled_sort_key.block.quantiles(num_partitions)

        sort_op = PyRunnerSortOp(
            map_args={"expr": expr, "boundaries": boundaries, "desc": sort._desc},
            reduce_args={"expr": expr, "desc": sort._desc},
        )
        new_pset = sort_op.run(input=prev_pset, num_target_partitions=num_partitions)
        self._part_manager.put_partition_set(sort.id(), new_pset)

    def _handle_coalesce(self, coal: Coalesce) -> None:
        num_partitions = coal.num_partitions()
        child_id = coal._children()[0].id()
        prev_pset = self._part_manager.get_partition_set(child_id)
        coalesce_op = PyRunnerCoalesceOp(
            map_args={"num_input_partitions": prev_pset.num_partitions()},
        )
        new_pset = coalesce_op.run(input=prev_pset, num_target_partitions=num_partitions)
        self._part_manager.put_partition_set(coal.id(), new_pset)
