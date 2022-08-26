from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, ClassVar, Dict, List, Optional, Type

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
from daft.runners.partitioning import PartID, PartitionSet, vPartition
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
class RayPartitionSet(PartitionSet[ray.ObjectRef[vPartition]]):
    _partitions: Dict[PartID, ray.ObjectRef[vPartition]]

    def to_pandas(self, schema: Optional[ExpressionList] = None) -> "pd.DataFrame":
        raise NotImplementedError()

    def get_partition(self, idx: PartID) -> ray.ObjectRef[vPartition]:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: ray.ObjectRef[vPartition]) -> None:
        self._partitions[idx] = part

    def delete_partition(self, idx: PartID) -> None:
        del self._partitions[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._partitions

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> List[int]:
        partition_ids = sorted(list(self._partitions.keys()))
        remote_len = ray.remote(len)
        return ray.get([remote_len.remote(self._partitions[pid]) for pid in partition_ids])

    def num_partitions(self) -> int:
        return len(self._partitions)


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


import ray


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
            output_list = [None for _ in range(num_target_partitions)]
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
        reduced_results = ray.get(reduced_results)
        return LocalPartitionSet({i: part for i, part in enumerate(reduced_results)})


class RayRunnerRepartitionRandom(RayRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class RayRunnerRepartitionHash(RayRunnerSimpleShuffler, RepartitionHashOp):
    ...


class RayRunnerCoalesceOp(RayRunnerSimpleShuffler, CoalesceOp):
    ...


class RayRunnerSortOp(RayRunnerSimpleShuffler, SortOp):
    ...


LocalLogicalPartitionOpRunner = LogicalPartitionOpRunner


class RayLogicalGlobalOpRunner(LogicalGlobalOpRunner):
    shuffle_ops: ClassVar[Dict[Type[ShuffleOp], Type[Shuffler]]] = {
        RepartitionRandomOp: RayRunnerRepartitionRandom,
        RepartitionHashOp: RayRunnerRepartitionHash,
        CoalesceOp: RayRunnerCoalesceOp,
        SortOp: RayRunnerSortOp,
    }

    def map_partitions(self, pset: PartitionSet, func: Callable[[vPartition], vPartition]) -> PartitionSet:
        return PartitionSet({i: func(part) for i, part in pset.partitions.items()})

    def reduce_partitions(self, pset: PartitionSet, func: Callable[[List[vPartition]], vPartition]) -> vPartition:
        data = list(pset.partitions.values())
        return func(data)


class PyRunner(Runner):
    def __init__(self) -> None:
        self._part_manager = PyRunnerPartitionManager()
        self._part_op_runner = LocalLogicalPartitionOpRunner()
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
        for exec_op in exec_plan.execution_ops:
            if exec_op.is_global_op:
                data_deps = exec_op.data_deps
                input_partition_set = {nid: self._part_manager.get_partition_set(nid) for nid in data_deps}
                result_partition_set = self._global_op_runner.run_node_list(input_partition_set, exec_op.logical_ops)
                self._part_manager.put_partition_set(exec_op.logical_ops[-1].id(), result_partition_set)

                for child_id in exec_op.data_deps:
                    self._part_manager.rm(child_id)

            else:
                data_deps = exec_op.data_deps
                for i in range(exec_op.num_partitions):
                    input_partitions = {nid: self._part_manager.get(nid, i) for nid in data_deps}
                    result_partition = self._part_op_runner.run_node_list(
                        input_partitions, nodes=exec_op.logical_ops, partition_id=i
                    )
                    self._part_manager.put(exec_op.logical_ops[-1].id(), i, result_partition)
                    for child_id in data_deps:
                        self._part_manager.rm(child_id, i)

        last = exec_plan.execution_ops[-1].logical_ops[-1]
        return self._part_manager.get_partition_set(last.id())
