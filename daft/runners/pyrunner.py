from __future__ import annotations

import multiprocessing
from concurrent import futures
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, Iterator

import psutil
from loguru import logger

from daft.daft import (
    FileFormatConfig,
    FileInfos,
    PythonStorageConfig,
    ResourceRequest,
    StorageConfig,
)
from daft.execution import physical_plan
from daft.execution.execution_step import Instruction, MaterializedResult, PartitionTask
from daft.filesystem import get_filesystem_from_path, glob_path_with_stats
from daft.internal.gpu import cuda_device_count
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.runners import runner_io
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
)
from daft.runners.profiler import profiler
from daft.runners.runner import Runner
from daft.table import Table

if TYPE_CHECKING:
    import fsspec


@dataclass
class LocalPartitionSet(PartitionSet[Table]):
    _partitions: dict[PartID, Table]

    def items(self) -> list[tuple[PartID, Table]]:
        return sorted(self._partitions.items())

    def _get_merged_vpartition(self) -> Table:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        return Table.concat([part for id, part in ids_and_partitions])

    def get_partition(self, idx: PartID) -> Table:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: Table) -> None:
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


class PyRunnerIO(runner_io.RunnerIO):
    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_config: StorageConfig | None = None,
    ) -> FileInfos:
        if fs is None and storage_config is not None:
            config = storage_config.config
            if isinstance(config, PythonStorageConfig):
                fs = config.fs
        file_infos = FileInfos()
        file_format = file_format_config.file_format() if file_format_config is not None else None
        for source_path in source_paths:
            if fs is None:
                fs = get_filesystem_from_path(source_path)

            path_file_infos = glob_path_with_stats(source_path, file_format, fs, storage_config)

            if len(path_file_infos) == 0:
                raise FileNotFoundError(f"No files found at {source_path}")

            file_infos.extend(path_file_infos)

        return file_infos

    def get_schema_from_first_filepath(
        self,
        file_infos: FileInfos,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
    ) -> Schema:
        if len(file_infos) == 0:
            raise ValueError("No files to get schema from")
        # Naively retrieve the first filepath in the PartitionSet
        return runner_io.sample_schema(file_infos[0].file_path, file_format_config, storage_config)


class PyRunner(Runner[Table]):
    def __init__(self, use_thread_pool: bool | None) -> None:
        super().__init__()
        self._use_thread_pool: bool = use_thread_pool if use_thread_pool is not None else True

        self.num_cpus = multiprocessing.cpu_count()
        self.num_gpus = cuda_device_count()
        self.bytes_memory = psutil.virtual_memory().total

    def runner_io(self) -> PyRunnerIO:
        return PyRunnerIO()

    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        partitions = list(self.run_iter(builder))

        result_pset = LocalPartitionSet({})
        for i, partition in enumerate(partitions):
            result_pset.set_partition(i, partition)

        pset_entry = self.put_partition_set_into_cache(result_pset)
        return pset_entry

    def run_iter(self, builder: LogicalPlanBuilder) -> Iterator[Table]:
        # Optimize the logical plan.
        builder = builder.optimize()
        # Finalize the logical plan and get a physical plan scheduler for translating the
        # physical plan to executable tasks.
        plan_scheduler = builder.to_physical_plan_scheduler()
        psets = {
            key: entry.value.values()
            for key, entry in self._part_set_cache._uuid_to_partition_set.items()
            if entry.value is not None
        }
        # Get executable tasks from planner.
        tasks = plan_scheduler.to_partition_tasks(psets, is_ray_runner=False)

        with profiler("profile_PyRunner.run_{datetime.now().isoformat()}.json"):
            partitions_gen = self._physical_plan_to_partitions(tasks)
            yield from partitions_gen

    def run_iter_tables(self, builder: LogicalPlanBuilder) -> Iterator[Table]:
        return self.run_iter(builder)

    def _physical_plan_to_partitions(self, plan: physical_plan.MaterializedPhysicalPlan) -> Iterator[Table]:
        inflight_tasks: dict[str, PartitionTask] = dict()
        inflight_tasks_resources: dict[str, ResourceRequest] = dict()
        future_to_task: dict[futures.Future, str] = dict()

        with futures.ThreadPoolExecutor() as thread_pool:
            try:
                next_step = next(plan)

                # Dispatch->Await loop.
                while True:
                    # Dispatch loop.
                    while True:
                        if next_step is None:
                            # Blocked on already dispatched tasks; await some tasks.
                            break

                        elif isinstance(next_step, Table):
                            # A final result.
                            yield next_step
                            next_step = next(plan)
                            continue

                        elif not self._can_admit_task(next_step.resource_request, inflight_tasks_resources.values()):
                            # Insufficient resources; await some tasks.
                            break

                        else:
                            # next_task is a task to run.

                            # Run the task in the main thread, instead of the thread pool, in certain conditions:
                            # - Threading is disabled in runner config.
                            # - Task is a no-op.
                            # - Task requires GPU.
                            # TODO(charles): Queue these up until the physical plan is blocked to avoid starving cluster.
                            if (
                                not self._use_thread_pool
                                or len(next_step.instructions) == 0
                                or (
                                    next_step.resource_request.num_gpus is not None
                                    and next_step.resource_request.num_gpus > 0
                                )
                            ):
                                logger.debug(
                                    "Running task synchronously in main thread: {next_step}", next_step=next_step
                                )
                                partitions = self.build_partitions(next_step.instructions, *next_step.inputs)
                                next_step.set_result([PyMaterializedResult(partition) for partition in partitions])

                            else:
                                # Submit the task for execution.
                                logger.debug("Submitting task for execution: {next_step}", next_step=next_step)
                                future = thread_pool.submit(
                                    self.build_partitions, next_step.instructions, *next_step.inputs
                                )
                                # Register the inflight task and resources used.
                                future_to_task[future] = next_step.id()
                                inflight_tasks[next_step.id()] = next_step
                                inflight_tasks_resources[next_step.id()] = next_step.resource_request

                            next_step = next(plan)

                    # Await at least one task and process the results.
                    assert (
                        len(future_to_task) > 0
                    ), f"Scheduler deadlocked! This should never happen. Please file an issue."
                    done_set, _ = futures.wait(list(future_to_task.keys()), return_when=futures.FIRST_COMPLETED)
                    for done_future in done_set:
                        done_id = future_to_task.pop(done_future)
                        del inflight_tasks_resources[done_id]
                        done_task = inflight_tasks.pop(done_id)
                        partitions = done_future.result()

                        logger.debug(
                            "Task completed: {done_id} -> {partitions}", done_id=done_id, partitions=partitions
                        )
                        done_task.set_result([PyMaterializedResult(partition) for partition in partitions])

                    if next_step is None:
                        next_step = next(plan)

            except StopIteration:
                return

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
    def build_partitions(instruction_stack: list[Instruction], *inputs: Table) -> list[Table]:
        partitions = list(inputs)
        for instruction in instruction_stack:
            partitions = instruction.run(partitions)

        return partitions


@dataclass(frozen=True)
class PyMaterializedResult(MaterializedResult[Table]):
    _partition: Table

    def partition(self) -> Table:
        return self._partition

    def vpartition(self) -> Table:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        return PartitionMetadata.from_table(self._partition)

    def cancel(self) -> None:
        return None

    def _noop(self, _: Table) -> None:
        return None
