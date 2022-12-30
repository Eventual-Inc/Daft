from __future__ import annotations

import multiprocessing
from dataclasses import dataclass
from typing import Callable, ClassVar

import psutil

from daft.execution.execution_plan import ExecutionPlan
from daft.execution.logical_op_runners import (
    LogicalGlobalOpRunner,
    LogicalPartitionOpRunner,
    ReduceType,
)
from daft.expressions import ColumnExpression
from daft.filesystem import glob_path
from daft.internal.gpu import cuda_device_count
from daft.internal.rule_runner import FixedPointPolicy, Once, RuleBatch, RuleRunner
from daft.logical import logical_plan
from daft.logical.optimizer import (
    DropProjections,
    DropRepartition,
    FoldProjections,
    PruneColumns,
    PushDownClausesIntoScan,
    PushDownLimit,
    PushDownPredicates,
)
from daft.logical.schema import ExpressionList
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionSet,
    PartitionSetFactory,
    vPartition,
)
from daft.runners.profiler import profiler
from daft.runners.runner import Runner
from daft.runners.shuffle_ops import (
    CoalesceOp,
    RepartitionHashOp,
    RepartitionRandomOp,
    ShuffleOp,
    Shuffler,
    SortOp,
)
from daft.types import ExpressionType


@dataclass
class LocalPartitionSet(PartitionSet[vPartition]):
    _partitions: dict[PartID, vPartition]

    def items(self) -> list[tuple[PartID, vPartition]]:
        return sorted(self._partitions.items())

    def _get_merged_vpartition(self) -> vPartition:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        return vPartition.merge_partitions([part for id, part in ids_and_partitions], verify_partition_id=False)

    def get_partition(self, idx: PartID) -> vPartition:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: vPartition) -> None:
        self._partitions[idx] = part

    def delete_partition(self, idx: PartID) -> None:
        del self._partitions[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._partitions

    def __len__(self) -> int:
        return sum(self.len_of_partitions())

    def len_of_partitions(self) -> list[int]:
        partition_ids = sorted(list(self._partitions.keys()))
        return [len(self._partitions[pid]) for pid in partition_ids]

    def num_partitions(self) -> int:
        return len(self._partitions)

    def wait(self) -> None:
        pass


class LocalPartitionSetFactory(PartitionSetFactory[vPartition]):
    def glob_filepaths(
        self,
        source_path: str,
    ) -> tuple[LocalPartitionSet, ExpressionList]:
        filepaths = glob_path(source_path)
        schema = ExpressionList([ColumnExpression(self.FILEPATH_COLUMN_NAME, ExpressionType.string())]).resolve()
        pset = LocalPartitionSet(
            {
                i: vPartition.from_pydict(
                    data={self.FILEPATH_COLUMN_NAME: [filepaths[i]]}, schema=schema, partition_id=i
                )
                for i in range(len(filepaths))  # Hardcoded to 1 path per partition
            },
        )
        return pset, schema


class PyRunnerSimpleShuffler(Shuffler):
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        map_args = self._map_args if self._map_args is not None else {}
        reduce_args = self._reduce_args if self._reduce_args is not None else {}

        source_partitions = input.num_partitions()
        map_results = [
            self.map_fn(input=input.get_partition(i), output_partitions=num_target_partitions, **map_args)
            for i in range(source_partitions)
        ]
        reduced_results = []
        for t in range(num_target_partitions):
            reduced_part = self.reduce_fn(
                [map_results[i][t] for i in range(source_partitions) if t in map_results[i]], **reduce_args
            )
            reduced_results.append(reduced_part)

        return LocalPartitionSet({i: part for i, part in enumerate(reduced_results)})


class PyRunnerRepartitionRandom(PyRunnerSimpleShuffler, RepartitionRandomOp):
    ...


class PyRunnerRepartitionHash(PyRunnerSimpleShuffler, RepartitionHashOp):
    ...


class PyRunnerCoalesceOp(PyRunnerSimpleShuffler, CoalesceOp):
    ...


class PyRunnerSortOp(PyRunnerSimpleShuffler, SortOp):
    ...


class LocalLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    def run_node_list(
        self,
        inputs: dict[int, PartitionSet],
        nodes: list[logical_plan.LogicalPlan],
        num_partitions: int,
        resource_request: ResourceRequest,
    ) -> PartitionSet:
        # NOTE: resource_request is ignored since there isn't any actual distribution of workloads in PyRunner
        result = LocalPartitionSet({})
        for i in range(num_partitions):
            input_partitions = {nid: inputs[nid].get_partition(i) for nid in inputs}
            result_partition = self.run_node_list_single_partition(input_partitions, nodes=nodes, partition_id=i)
            result.set_partition(i, result_partition)
        return result


class LocalLogicalGlobalOpRunner(LogicalGlobalOpRunner):
    shuffle_ops: ClassVar[dict[type[ShuffleOp], type[Shuffler]]] = {
        RepartitionRandomOp: PyRunnerRepartitionRandom,
        RepartitionHashOp: PyRunnerRepartitionHash,
        CoalesceOp: PyRunnerCoalesceOp,
        SortOp: PyRunnerSortOp,
    }

    def map_partitions(
        self, pset: PartitionSet, func: Callable[[vPartition], vPartition], resource_request: ResourceRequest
    ) -> PartitionSet:
        # NOTE: resource_request is ignored since there isn't any actual distribution of workloads in PyRunner
        return LocalPartitionSet({i: func(pset.get_partition(i)) for i in range(pset.num_partitions())})

    def reduce_partitions(self, pset: PartitionSet, func: Callable[[list[vPartition]], ReduceType]) -> ReduceType:
        data = [pset.get_partition(i) for i in range(pset.num_partitions())]
        return func(data)


class PyRunner(Runner):
    def __init__(self) -> None:
        super().__init__()
        self._part_op_runner = LocalLogicalPartitionOpRunner()
        self._global_op_runner = LocalLogicalGlobalOpRunner()
        self._optimizer = RuleRunner(
            [
                RuleBatch(
                    "SinglePassPushDowns",
                    Once,
                    [
                        DropRepartition(),
                        PushDownPredicates(),
                        PruneColumns(),
                        FoldProjections(),
                        PushDownClausesIntoScan(),
                    ],
                ),
                RuleBatch(
                    "PushDownLimitsAndRepartitions",
                    FixedPointPolicy(3),
                    [PushDownLimit(), DropRepartition(), DropProjections()],
                ),
            ]
        )

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        return self._optimizer.optimize(plan)

    def partition_set_factory(self) -> PartitionSetFactory:
        return LocalPartitionSetFactory()

    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        optimized_plan = self.optimize(plan)

        exec_plan = ExecutionPlan.plan_from_logical(optimized_plan)
        result_partition_set: PartitionSet

        # Check that the local machine has sufficient resources available for execution
        for exec_op in exec_plan.execution_ops:
            resource_request = exec_op.resource_request()
            if resource_request.num_cpus is not None and resource_request.num_cpus > multiprocessing.cpu_count():
                raise RuntimeError(
                    f"Requested {resource_request.num_cpus} CPUs but found only {multiprocessing.cpu_count()} available"
                )
            if resource_request.num_gpus is not None and resource_request.num_gpus > cuda_device_count():
                raise RuntimeError(
                    f"Requested {resource_request.num_gpus} GPUs but found only {cuda_device_count()} available"
                )
            if (
                resource_request.memory_bytes is not None
                and resource_request.memory_bytes > psutil.virtual_memory().total
            ):
                raise RuntimeError(
                    f"Requested {resource_request.memory_bytes} bytes of memory but found only {psutil.virtual_memory().total} available"
                )
        partition_intermediate_results: dict[int, PartitionSet] = {}
        with profiler("profile.json"):
            for exec_op in exec_plan.execution_ops:

                data_deps = exec_op.data_deps
                input_partition_set = {nid: partition_intermediate_results[nid] for nid in data_deps}

                if exec_op.is_global_op:
                    result_partition_set = self._global_op_runner.run_node_list(
                        input_partition_set, exec_op.logical_ops
                    )
                else:
                    result_partition_set = self._part_op_runner.run_node_list(
                        input_partition_set, exec_op.logical_ops, exec_op.num_partitions, exec_op.resource_request()
                    )

                for child_id in data_deps:
                    del partition_intermediate_results[child_id]

                partition_intermediate_results[exec_op.logical_ops[-1].id()] = result_partition_set

            last = exec_plan.execution_ops[-1].logical_ops[-1]
            final_result = partition_intermediate_results[last.id()]
            pset_entry = self._part_set_cache.put_partition_set(final_result)
            return pset_entry
