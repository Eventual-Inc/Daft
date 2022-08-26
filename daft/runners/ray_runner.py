from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, ClassVar, Dict, List, Optional, Type

import pandas as pd
import ray

from daft.execution.execution_plan import ExecutionPlan
from daft.execution.logical_op_runners import (
    LogicalGlobalOpRunner,
    LogicalPartitionOpRunner,
)
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical.logical_plan import LogicalPlan
from daft.logical.optimizer import (
    DropRepartition,
    FoldProjections,
    PushDownLimit,
    PushDownPredicates,
)
from daft.logical.schema import ExpressionList
from daft.runners.partitioning import PartID, PartitionManager, PartitionSet, vPartition
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
    SortOp,
)


@dataclass
class RayPartitionSet(PartitionSet[ray.ObjectRef]):
    _partitions: Dict[PartID, ray.ObjectRef]

    def to_pandas(self, schema: Optional[ExpressionList] = None) -> "pd.DataFrame":
        partition_ids = sorted(list(self._partitions.keys()))
        assert partition_ids[0] == 0
        assert partition_ids[-1] + 1 == len(partition_ids)
        all_partitions = ray.get([self._partitions[pid] for pid in partition_ids])
        part_dfs = [part.to_pandas(schema=schema) for part in all_partitions]
        return pd.concat([pdf for pdf in part_dfs if not pdf.empty], ignore_index=True)

    def get_partition(self, idx: PartID) -> ray.ObjectRef:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: ray.ObjectRef) -> None:
        self._partitions[idx] = part

    def delete_partition(self, idx: PartID) -> None:
        del self._partitions[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._partitions

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> List[int]:
        partition_ids = sorted(list(self._partitions.keys()))

        @ray.remote
        def remote_len(p: vPartition) -> int:
            return len(p)

        result: List[int] = ray.get([remote_len.remote(self._partitions[pid]) for pid in partition_ids])
        return result

    def num_partitions(self) -> int:
        return len(self._partitions)


class RayRunnerSimpleShuffler(Shuffler):
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}
        # map_args_ref = ray.put(map_args)
        # reduce_args_ref = ray.put(reduce_args)

        source_partitions = input.num_partitions()

        def reduce_wrapper(to_reduce: List[vPartition]):
            return self.reduce_fn(ray.get(to_reduce), **reduce_args)

        def map_wrapper(input: vPartition):
            output_dict = self.map_fn(input=input, output_partitions=num_target_partitions, **map_args)
            output_list: List[Optional[vPartition]] = [None for _ in range(num_target_partitions)]
            for part_id, part in output_dict.items():
                output_list[part_id] = part

            if num_target_partitions == 1:
                return part
            else:
                return output_list

        remote_reduce_fn = ray.remote(reduce_wrapper)

        remote_map_fn = ray.remote(map_wrapper).options(num_returns=num_target_partitions)

        map_results = [remote_map_fn.remote(input=input.get_partition(i)) for i in range(source_partitions)]
        reduced_results = []
        for t in range(num_target_partitions):
            if num_target_partitions == 1:
                map_subset = map_results
            else:
                map_subset = [map_results[i][t] for i in range(source_partitions)]
            reduced_part = remote_reduce_fn.remote(map_subset)
            reduced_results.append(reduced_part)
        # reduced_results = ray.get(reduced_results)
        return RayPartitionSet({i: part for i, part in enumerate(reduced_results)})


class RayRunnerRepartitionRandom(RayRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class RayRunnerRepartitionHash(RayRunnerSimpleShuffler, RepartitionHashOp):
    ...


class RayRunnerCoalesceOp(RayRunnerSimpleShuffler, CoalesceOp):
    ...


class RayRunnerSortOp(RayRunnerSimpleShuffler, SortOp):
    ...


class RayLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    def run_node_list(
        self, inputs: Dict[int, PartitionSet], nodes: List[LogicalPlan], num_partitions: int
    ) -> PartitionSet:
        @ray.remote
        def func(input_refs: Dict[int, ray.ObjectRef], partition_id: int) -> vPartition:
            ids = list(input_refs.keys())
            values = ray.get([input_refs[id] for id in ids])
            input_partitions = {id: val for id, val in zip(ids, values)}
            return self.run_node_list_single_partition(input_partitions, nodes=nodes, partition_id=partition_id)

        result = RayPartitionSet({})
        for i in range(num_partitions):
            input_partitions = {nid: inputs[nid].get_partition(i) for nid in inputs}
            result_partition = func.remote(input_partitions, partition_id=i)
            result.set_partition(i, result_partition)
        return result


class RayLogicalGlobalOpRunner(LogicalGlobalOpRunner):
    shuffle_ops: ClassVar[Dict[Type[ShuffleOp], Type[Shuffler]]] = {
        RepartitionRandomOp: RayRunnerRepartitionRandom,
        RepartitionHashOp: RayRunnerRepartitionHash,
        CoalesceOp: RayRunnerCoalesceOp,
        SortOp: RayRunnerSortOp,
    }

    def map_partitions(self, pset: PartitionSet, func: Callable[[vPartition], vPartition]) -> PartitionSet:
        remote_func = ray.remote(func)
        return RayPartitionSet({i: remote_func.remote(pset.get_partition(i)) for i in range(pset.num_partitions())})

    def reduce_partitions(self, pset: PartitionSet, func: Callable[[List[vPartition]], vPartition]) -> vPartition:
        @ray.remote
        def remote_func(*parts: vPartition) -> vPartition:
            return func(list(parts))

        data = [pset.get_partition(i) for i in range(pset.num_partitions())]
        result: vPartition = ray.get(remote_func.remote(*data))
        return result


class RayRunner(Runner):
    def __init__(self) -> None:
        self._part_manager = PartitionManager(lambda: RayPartitionSet({}))
        self._part_op_runner = RayLogicalPartitionOpRunner()
        self._global_op_runner = RayLogicalGlobalOpRunner()
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
        result_partition_set: PartitionSet
        for exec_op in exec_plan.execution_ops:

            data_deps = exec_op.data_deps
            input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}

            if exec_op.is_global_op:
                input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}
                result_partition_set = self._global_op_runner.run_node_list(input_partition_set, exec_op.logical_ops)

                for child_id in exec_op.data_deps:
                    self._part_manager.rm(child_id)

            else:
                result_partition_set = self._part_op_runner.run_node_list(
                    input_partition_set, exec_op.logical_ops, exec_op.num_partitions
                )
            self._part_manager.put_partition_set(exec_op.logical_ops[-1].id(), result_partition_set)

        last = exec_plan.execution_ops[-1].logical_ops[-1]
        return self._part_manager.get_partition_set(last.id())
