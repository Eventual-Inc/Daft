from __future__ import annotations

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
from daft.runners.partitioning import PartitionSet, vPartition
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
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


class PyRunnerSimpleShuffler(Shuffler):
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


LocalLogicalPartitionOpRunner = LogicalPartitionOpRunner


class LocalLogicalGlobalOpRunner(LogicalGlobalOpRunner):
    shuffle_ops: ClassVar[Dict[Type[ShuffleOp], Type[Shuffler]]] = {
        RepartitionRandomOp: PyRunnerRepartitionRandom,
        RepartitionHashOp: PyRunnerRepartitionHash,
        CoalesceOp: PyRunnerCoalesceOp,
        SortOp: PyRunnerSortOp,
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
        self._global_op_runner = LocalLogicalGlobalOpRunner()
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

            else:
                data_deps = exec_op.data_deps
                for i in range(exec_op.num_partitions):
                    input_partitions = {nid: self._part_manager.get(nid, i) for nid in data_deps}
                    result_partition = self._part_op_runner.run_node_list(
                        input_partitions, nodes=exec_op.logical_ops, partition_id=i
                    )
                    self._part_manager.put(exec_op.logical_ops[-1].id(), i, result_partition)
        last = exec_plan.execution_ops[-1].logical_ops[-1]
        return self._part_manager.get_partition_set(last.id())
