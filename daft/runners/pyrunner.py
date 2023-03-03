from __future__ import annotations

import multiprocessing
from concurrent import futures
from dataclasses import dataclass
from typing import Iterable

import psutil
import pyarrow as pa

from daft.datasources import SourceInfo
from daft.execution import physical_plan, physical_plan_factory
from daft.execution.execution_step import Instruction, MaterializedResult, PartitionTask
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
        return vPartition.concat([part for id, part in ids_and_partitions])

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
        source_info: SourceInfo | None = None,
    ) -> tuple[LocalPartitionSet, Schema]:
        files_info = glob_path_with_stats(source_path, source_info)

        if len(files_info) == 0:
            raise FileNotFoundError(f"No files found at {source_path}")

        partition = vPartition.from_pydict(
            {
                "path": pa.array([file_info.path for file_info in files_info], type=pa.string()),
                "size": pa.array([file_info.size for file_info in files_info], type=pa.int64()),
                "type": pa.array([file_info.type for file_info in files_info], type=pa.string()),
                "rows": pa.array([file_info.rows for file_info in files_info], type=pa.int64()),
            },
        )

        # Make sure that the schema is consistent with what we expect
        schema = self._get_listing_paths_details_schema()
        assert partition.schema() == schema, f"Schema should be expected: {schema}, but received: {partition.schema()}"

        pset = LocalPartitionSet(
            {
                # Hardcoded to 1 partition
                0: partition,
            }
        )
        return pset, partition.schema()


class LocalLogicalPartitionOpRunner(LogicalPartitionOpRunner):
    ...


class PyRunner(Runner):
    def __init__(self, use_thread_pool: bool | None) -> None:
        super().__init__()
        self._use_thread_pool: bool = use_thread_pool if use_thread_pool is not None else True

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

        with profiler("profile_PyRunner.run_{datetime.now().isoformat()}.json"):
            partitions = self._physical_plan_to_partitions(plan)

            result_pset = LocalPartitionSet({})
            for i, partition in enumerate(partitions):
                result_pset.set_partition(i, partition)

            pset_entry = self.put_partition_set_into_cache(result_pset)
            return pset_entry

    def _physical_plan_to_partitions(self, plan: physical_plan.MaterializedPhysicalPlan) -> list[vPartition]:
        inflight_tasks: dict[str, PartitionTask] = dict()
        inflight_tasks_resources: dict[str, ResourceRequest] = dict()
        future_to_task: dict[futures.Future, str] = dict()

        result = []
        next_step = None
        with futures.ThreadPoolExecutor() as thread_pool:
            try:
                while True:
                    # Get the next task to dispatch.
                    if next_step is None:
                        next_step = next(plan)

                    # Try dispatching tasks (up to resource limits) until there are no more tasks to dispatch.
                    while next_step is not None and self._can_admit_task(
                        next_step.resource_request, inflight_tasks_resources.values()
                    ):

                        # Run the task in the main thread, instead of the thread pool, in certain conditions:
                        # - Threading is disabled in runner config.
                        # - Task is a no-op.
                        # - Task requires GPU.
                        if (
                            not self._use_thread_pool
                            or len(next_step.instructions) == 0
                            or (
                                next_step.resource_request.num_gpus is not None
                                and next_step.resource_request.num_gpus > 0
                            )
                        ):
                            partitions = self.build_partitions(next_step.instructions, *next_step.inputs)
                            next_step.set_result([PyMaterializedResult(partition) for partition in partitions])

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
                    assert (
                        len(future_to_task) > 0
                    ), f"Scheduler deadlocked! This should never happen. Please file an issue."
                    done_set, _ = futures.wait(list(future_to_task.keys()), return_when=futures.FIRST_COMPLETED)
                    for done in done_set:
                        done_id = future_to_task.pop(done)
                        del inflight_tasks_resources[done_id]
                        done_task = inflight_tasks.pop(done_id)

                        partitions = done.result()
                        done_task.set_result([PyMaterializedResult(partition) for partition in partitions])

            except StopIteration as e:
                result = e.value

        return result

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
        return PartitionMetadata.from_table(self._partition)

    def cancel(self) -> None:
        return None

    def _noop(self, _: vPartition) -> None:
        return None
