from __future__ import annotations

import multiprocessing
from concurrent import futures
from dataclasses import dataclass
from typing import Iterable

import psutil

from daft.execution import physical_plan_factory
from daft.execution.execution_step import (
    ExecutionStep,
    Instruction,
    MaterializedResult,
    MultiOutputExecutionStep,
    SingleOutputExecutionStep,
)
from daft.execution.logical_op_runners import LogicalPartitionOpRunner
from daft.filesystem import glob_path_with_stats
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
from daft.logical.schema import Schema
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
    PartitionSetFactory,
    vPartition,
)
from daft.runners.profiler import profiler
from daft.runners.runner import Runner


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
    def glob_paths_details(
        self,
        source_path: str,
    ) -> tuple[LocalPartitionSet, Schema]:
        files_info = glob_path_with_stats(source_path)

        if len(files_info) == 0:
            raise FileNotFoundError(f"No files found at {source_path}")

        schema = self._get_listing_paths_details_schema()
        pset = LocalPartitionSet(
            {
                # Hardcoded to 1 partition
                0: vPartition.from_pydict(
                    data={
                        self.FS_LISTING_PATH_COLUMN_NAME: [f.path for f in files_info],
                        self.FS_LISTING_SIZE_COLUMN_NAME: [f.size for f in files_info],
                        self.FS_LISTING_TYPE_COLUMN_NAME: [f.type for f in files_info],
                    },
                    schema=schema,
                    partition_id=0,
                ),
            }
        )
        return pset, schema


class LocalLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    ...


class PyRunner(Runner):
    def __init__(self) -> None:
        super().__init__()
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

        self.num_cpus = multiprocessing.cpu_count()
        self.num_gpus = cuda_device_count()
        self.bytes_memory = psutil.virtual_memory().total

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        # From PyRunner
        return self._optimizer.optimize(plan)

    def partition_set_factory(self) -> PartitionSetFactory:
        return LocalPartitionSetFactory()

    def run(self, logplan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        logplan = self.optimize(logplan)
        psets = {
            key: entry.value.values()
            for key, entry in self._part_set_cache._uuid_to_partition_set.items()
            if entry.value is not None
        }
        plan = physical_plan_factory.get_materializing_physical_plan(logplan, psets)
        result_pset = LocalPartitionSet({})

        inflight_tasks: dict[str, ExecutionStep] = dict()
        inflight_tasks_resources: dict[str, ResourceRequest] = dict()
        future_to_task: dict[futures.Future, str] = dict()

        next_step = None
        with futures.ThreadPoolExecutor() as thread_pool, profiler(
            "profile_PyRunner.run_{datetime.now().isoformat()}.json"
        ):
            try:
                while True:
                    # Get the next task to dispatch.
                    if next_step is None:
                        next_step = next(plan)

                    # Try dispatching tasks (up to resource limits) until there are no more tasks to dispatch.
                    while next_step is not None and self._can_admit_task(
                        next_step.resource_request, inflight_tasks_resources.values()
                    ):

                        # If this task is a no-op, just run it locally immediately.
                        if len(next_step.instructions) == 0:
                            assert isinstance(next_step, SingleOutputExecutionStep)
                            [partition] = next_step.inputs
                            next_step.result = PyMaterializedResult(partition)

                        else:
                            # Submit the task for execution.
                            future = thread_pool.submit(
                                self.build_partitions, next_step.instructions, *next_step.inputs
                            )
                            # Register the inflight task and resources used.
                            future_to_task[future] = next_step.id()
                            inflight_tasks[next_step.id()] = next_step
                            inflight_tasks_resources[next_step.id()] = next_step.resource_request

                        # Get the next task to dispatch.
                        next_step = next(plan)

                    # Await at least task and process the results.
                    assert len(future_to_task) > 0, f"Scheduler deadlocked; should never happen. next_step={next_step}"
                    done_set, _ = futures.wait(list(future_to_task.keys()), return_when=futures.FIRST_COMPLETED)
                    for done in done_set:
                        done_id = future_to_task.pop(done)
                        del inflight_tasks_resources[done_id]
                        done_task = inflight_tasks.pop(done_id)

                        partitions = done.result()

                        if isinstance(done_task, MultiOutputExecutionStep):
                            done_task.results = [PyMaterializedResult(partition) for partition in partitions]
                        elif isinstance(done_task, SingleOutputExecutionStep):
                            [partition] = partitions
                            done_task.result = PyMaterializedResult(partition)
                        else:
                            raise TypeError(f"Could not type match input {done_task}")

            except StopIteration as e:
                for i, partition in enumerate(e.value):
                    result_pset.set_partition(i, partition)

            pset_entry = self.put_partition_set_into_cache(result_pset)
            return pset_entry

    def _check_resource_requests(self, resource_request: ResourceRequest) -> None:
        """Validates that the requested ResourceRequest is possible to run locally"""

        if resource_request.num_cpus is not None and resource_request.num_cpus > self.num_cpus:
            raise RuntimeError(f"Requested {resource_request.num_cpus} CPUs but found only {self.num_cpus} available")
        if resource_request.num_gpus is not None and resource_request.num_gpus > self.num_gpus:
            raise RuntimeError(f"Requested {resource_request.num_gpus} GPUs but found only {self.num_gpus} available")
        if resource_request.memory_bytes is not None and resource_request.memory_bytes > self.bytes_memory:
            raise RuntimeError(
                f"Requested {resource_request.memory_bytes} bytes of memory but found only {self.bytes_memory} available"
            )

    def _can_admit_task(self, resource_request: ResourceRequest, inflight_resources: Iterable[ResourceRequest]) -> bool:
        self._check_resource_requests(resource_request)

        total_inflight_resources: ResourceRequest = sum(inflight_resources, ResourceRequest())
        cpus_okay = (total_inflight_resources.num_cpus or 0) + (resource_request.num_cpus or 0) <= self.num_cpus
        gpus_okay = (total_inflight_resources.num_gpus or 0) + (resource_request.num_gpus or 0) <= self.num_gpus
        memory_okay = (total_inflight_resources.memory_bytes or 0) + (
            resource_request.memory_bytes or 0
        ) <= self.bytes_memory

        return all((cpus_okay, gpus_okay, memory_okay))

    @staticmethod
    def build_partitions(instruction_stack: list[Instruction], *inputs: vPartition) -> list[vPartition]:
        partitions = list(inputs)
        for instruction in instruction_stack:
            partitions = instruction.run(partitions)

        return partitions


@dataclass(frozen=True)
class PyMaterializedResult(MaterializedResult[vPartition]):
    _partition: vPartition

    def partition(self) -> vPartition:
        return self._partition

    def vpartition(self) -> vPartition:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        return self._partition.metadata()

    def cancel(self) -> None:
        return None

    def _noop(self, _: vPartition) -> None:
        return None
