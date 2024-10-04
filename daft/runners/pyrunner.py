from __future__ import annotations

import contextlib
import logging
import threading
import uuid
from concurrent import futures
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator

from daft.context import get_context
from daft.daft import FileFormatConfig, FileInfos, IOConfig, ResourceRequest, SystemInfo
from daft.execution.native_executor import NativeExecutor
from daft.expressions import ExpressionsProjection
from daft.filesystem import glob_path_with_stats
from daft.internal.gpu import cuda_device_count
from daft.runners import runner_io
from daft.runners.partitioning import (
    MaterializedResult,
    PartialPartitionMetadata,
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
)
from daft.runners.profiler import profiler
from daft.runners.progress_bar import ProgressBar
from daft.runners.runner import Runner
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.execution import physical_plan
    from daft.execution.execution_step import Instruction, PartitionTask
    from daft.logical.builder import LogicalPlanBuilder
    from daft.udf import UserProvidedPythonFunction

logger = logging.getLogger(__name__)


# Unique UUID for each execution
ExecutionID = str

# Unique ID for each task
TaskID = str


class LocalPartitionSet(PartitionSet[MicroPartition]):
    _partitions: dict[PartID, MaterializedResult[MicroPartition]]

    def __init__(self) -> None:
        super().__init__()
        self._partitions = {}

    def items(self) -> list[tuple[PartID, MaterializedResult[MicroPartition]]]:
        return sorted(self._partitions.items())

    def _get_merged_micropartition(self) -> MicroPartition:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        return MicroPartition.concat([part.partition() for id, part in ids_and_partitions])

    def _get_preview_micropartitions(self, num_rows: int) -> list[MicroPartition]:
        ids_and_partitions = self.items()
        preview_parts = []
        for _, mat_result in ids_and_partitions:
            part: MicroPartition = mat_result.partition()
            part_len = len(part)
            if part_len >= num_rows:  # if this part has enough rows, take what we need and break
                preview_parts.append(part.slice(0, num_rows))
                break
            else:  # otherwise, take the whole part and keep going
                num_rows -= part_len
                preview_parts.append(part)
        return preview_parts

    def get_partition(self, idx: PartID) -> MaterializedResult[MicroPartition]:
        return self._partitions[idx]

    def set_partition(self, idx: PartID, part: MaterializedResult[MicroPartition]) -> None:
        self._partitions[idx] = part

    def set_partition_from_table(self, idx: PartID, part: MicroPartition) -> None:
        self._partitions[idx] = PyMaterializedResult(part, PartitionMetadata.from_table(part))

    def delete_partition(self, idx: PartID) -> None:
        del self._partitions[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._partitions

    def __len__(self) -> int:
        return sum(len(partition.partition()) for partition in self._partitions.values())

    def size_bytes(self) -> int | None:
        size_bytes_ = [partition.partition().size_bytes() for partition in self._partitions.values()]
        size_bytes: list[int] = [size for size in size_bytes_ if size is not None]
        if len(size_bytes) != len(size_bytes_):
            return None
        else:
            return sum(size_bytes)

    def num_partitions(self) -> int:
        return len(self._partitions)

    def wait(self) -> None:
        pass


class PyActorPool:
    initialized_stateful_udfs_process_singleton: dict[str, UserProvidedPythonFunction] | None = None

    def __init__(
        self,
        pool_id: str,
        num_actors: int,
        resource_request: ResourceRequest,
        projection: ExpressionsProjection,
    ):
        self._pool_id = pool_id
        self._num_actors = num_actors
        self._resource_request = resource_request
        self._executor: futures.ProcessPoolExecutor | None = None
        self._projection = projection

    @staticmethod
    def initialize_actor_global_state(uninitialized_projection: ExpressionsProjection):
        from daft.daft import extract_partial_stateful_udf_py

        if PyActorPool.initialized_stateful_udfs_process_singleton is not None:
            raise RuntimeError("Cannot initialize Python process actor twice.")
        else:
            partial_stateful_udfs = {
                name: psu
                for expr in uninitialized_projection
                for name, psu in extract_partial_stateful_udf_py(expr._expr).items()
            }

            logger.info("Initializing stateful UDFs: %s", ", ".join(partial_stateful_udfs.keys()))

            PyActorPool.initialized_stateful_udfs_process_singleton = {}
            for name, (partial_udf, init_args) in partial_stateful_udfs.items():
                if init_args is None:
                    PyActorPool.initialized_stateful_udfs_process_singleton[name] = partial_udf.func_cls()
                else:
                    args, kwargs = init_args
                    PyActorPool.initialized_stateful_udfs_process_singleton[name] = partial_udf.func_cls(
                        *args, **kwargs
                    )

    @staticmethod
    def build_partitions_with_stateful_project(
        uninitialized_projection: ExpressionsProjection,
        partition: MicroPartition,
        partial_metadata: PartialPartitionMetadata,
    ) -> list[MaterializedResult[MicroPartition]]:
        # Bind the expressions to the initialized stateful UDFs, which should already have been initialized at process start-up
        initialized_stateful_udfs = PyActorPool.initialized_stateful_udfs_process_singleton
        assert (
            initialized_stateful_udfs is not None
        ), "PyActor process must be initialized with stateful UDFs before execution"
        initialized_projection = ExpressionsProjection(
            [e._bind_stateful_udfs(initialized_stateful_udfs) for e in uninitialized_projection]
        )
        new_part = partition.eval_expression_list(initialized_projection)
        return [
            PyMaterializedResult(new_part, PartitionMetadata.from_table(new_part).merge_with_partial(partial_metadata))
        ]

    def submit(
        self,
        instruction_stack: list[Instruction],
        partitions: list[MicroPartition],
        final_metadata: list[PartialPartitionMetadata],
    ) -> futures.Future[list[MaterializedResult[MicroPartition]]]:
        from daft.execution import execution_step

        assert self._executor is not None, "Cannot submit to uninitialized PyActorPool"

        # PyActorPools can only handle 1 to 1 projections (no fanouts/fan-ins) and only
        # StatefulUDFProject instructions (no filters etc)
        assert len(partitions) == 1
        assert len(final_metadata) == 1
        assert len(instruction_stack) == 1
        instruction = instruction_stack[0]
        assert isinstance(instruction, execution_step.StatefulUDFProject)
        projection = instruction.projection
        partition = partitions[0]
        partial_metadata = final_metadata[0]

        return self._executor.submit(
            PyActorPool.build_partitions_with_stateful_project,
            projection,
            partition,
            partial_metadata,
        )

    def teardown(self) -> None:
        # Shut down the executor
        assert self._executor is not None, "Should have an executor when exiting context"
        self._executor.shutdown()
        self._executor = None

    def setup(self) -> None:
        self._executor = futures.ProcessPoolExecutor(
            self._num_actors, initializer=PyActorPool.initialize_actor_global_state, initargs=(self._projection,)
        )


class PyRunnerIO(runner_io.RunnerIO):
    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> FileInfos:
        file_infos = FileInfos()
        file_format = file_format_config.file_format() if file_format_config is not None else None
        for source_path in source_paths:
            path_file_infos = glob_path_with_stats(source_path, file_format, io_config)

            if len(path_file_infos) == 0:
                raise FileNotFoundError(f"No files found at {source_path}")

            file_infos.extend(path_file_infos)

        return file_infos


class PyRunner(Runner[MicroPartition]):
    def __init__(self, use_thread_pool: bool | None) -> None:
        super().__init__()

        self._use_thread_pool: bool = use_thread_pool if use_thread_pool is not None else True
        self._thread_pool = futures.ThreadPoolExecutor()

        # Registry of active ActorPools
        self._actor_pools: dict[str, PyActorPool] = {}

        # Global accounting of tasks and resources
        self._inflight_futures: dict[tuple[ExecutionID, TaskID], futures.Future] = {}

        system_info = SystemInfo()
        num_cpus = system_info.cpu_count()
        if num_cpus is None:
            import multiprocessing

            self.num_cpus = multiprocessing.cpu_count()
        else:
            self.num_cpus = num_cpus

        self.num_gpus = cuda_device_count()
        self.total_bytes_memory = system_info.total_memory()

        # Resource accounting:
        self._resource_accounting_lock = threading.Lock()
        self._available_bytes_memory = self.total_bytes_memory
        self._available_cpus = float(self.num_cpus)
        self._available_gpus = float(self.num_gpus)

    def runner_io(self) -> PyRunnerIO:
        return PyRunnerIO()

    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        results = list(self.run_iter(builder))

        result_pset = LocalPartitionSet()
        for i, result in enumerate(results):
            result_pset.set_partition(i, result)

        pset_entry = self.put_partition_set_into_cache(result_pset)
        return pset_entry

    def run_iter(
        self,
        builder: LogicalPlanBuilder,
        results_buffer_size: int | None = None,
    ) -> Iterator[PyMaterializedResult]:
        # NOTE: Freeze and use this same execution config for the entire execution
        daft_execution_config = get_context().daft_execution_config
        execution_id = str(uuid.uuid4())

        # Optimize the logical plan.
        builder = builder.optimize()

        if daft_execution_config.enable_aqe:
            adaptive_planner = builder.to_adaptive_physical_plan_scheduler(daft_execution_config)
            while not adaptive_planner.is_done():
                source_id, plan_scheduler = adaptive_planner.next()
                # don't store partition sets in variable to avoid reference
                tasks = plan_scheduler.to_partition_tasks(
                    {k: v.values() for k, v in self._part_set_cache.get_all_partition_sets().items()},
                    results_buffer_size,
                )
                del plan_scheduler
                results_gen = self._physical_plan_to_partitions(execution_id, tasks)
                # if source_id is none that means this is the final stage
                if source_id is None:
                    yield from results_gen
                else:
                    intermediate = LocalPartitionSet()
                    for i, rg in enumerate(results_gen):
                        intermediate.set_partition(i, rg)
                    cache_entry = self._part_set_cache.put_partition_set(intermediate)
                    del intermediate
                    adaptive_planner.update(source_id, cache_entry)
                    del cache_entry
        else:
            # Finalize the logical plan and get a physical plan scheduler for translating the
            # physical plan to executable tasks.
            if daft_execution_config.enable_native_executor:
                logger.info("Using native executor")
                executor = NativeExecutor.from_logical_plan_builder(builder)
                results_gen = executor.run(
                    {k: v.values() for k, v in self._part_set_cache.get_all_partition_sets().items()},
                    daft_execution_config,
                    results_buffer_size,
                )
                yield from results_gen
            else:
                logger.info("Using python executor")

                plan_scheduler = builder.to_physical_plan_scheduler(daft_execution_config)
                psets = {k: v.values() for k, v in self._part_set_cache.get_all_partition_sets().items()}
                # Get executable tasks from planner.
                tasks = plan_scheduler.to_partition_tasks(psets, results_buffer_size)
                del psets
                with profiler("profile_PyRunner.run_{datetime.now().isoformat()}.json"):
                    results_gen = self._physical_plan_to_partitions(execution_id, tasks)
                    yield from results_gen

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size=results_buffer_size):
            yield result.partition()

    @contextlib.contextmanager
    def actor_pool_context(
        self,
        name: str,
        actor_resource_request: ResourceRequest,
        _task_resource_request: ResourceRequest,
        num_actors: int,
        projection: ExpressionsProjection,
    ) -> Iterator[str]:
        actor_pool_id = f"py_actor_pool-{name}"

        total_resource_request = actor_resource_request * num_actors
        admitted = self._attempt_admit_task(total_resource_request)

        if not admitted:
            raise RuntimeError(
                f"Not enough resources available to admit {num_actors} actors, each with resource request: {actor_resource_request}"
            )

        try:
            self._actor_pools[actor_pool_id] = PyActorPool(
                actor_pool_id, num_actors, actor_resource_request, projection
            )
            self._actor_pools[actor_pool_id].setup()
            logger.debug("Created actor pool %s with resources: %s", actor_pool_id, total_resource_request)
            yield actor_pool_id
        # NOTE: Ensure that teardown always occurs regardless of any errors that occur during actor pool setup or execution
        finally:
            logger.debug("Tearing down actor pool: %s", actor_pool_id)
            self._release_resources(total_resource_request)
            self._actor_pools[actor_pool_id].teardown()
            del self._actor_pools[actor_pool_id]

    def _physical_plan_to_partitions(
        self,
        execution_id: str,
        plan: physical_plan.MaterializedPhysicalPlan[MicroPartition],
    ) -> Iterator[PyMaterializedResult]:
        local_futures_to_task: dict[futures.Future, PartitionTask] = {}
        pbar = ProgressBar(use_ray_tqdm=False)

        try:
            next_step = next(plan)

            # Dispatch->Await loop.
            while True:
                # Dispatch loop.
                while True:
                    if next_step is None:
                        # Blocked on already dispatched tasks; await some tasks.
                        logger.debug(
                            "execution[%s] Skipping to wait on dispatched tasks: plan waiting on work", execution_id
                        )
                        break

                    elif isinstance(next_step, MaterializedResult):
                        assert isinstance(next_step, PyMaterializedResult)

                        # A final result.
                        logger.debug("execution[%s] Yielding completed step", execution_id)
                        yield next_step
                        next_step = next(plan)
                        continue

                    else:
                        # next_task is a task to run.
                        task_admitted = self._attempt_admit_task(
                            next_step.resource_request,
                        )

                        if not task_admitted:
                            # Insufficient resources; await some tasks.
                            logger.debug(
                                "execution[%s] Skipping to wait on dispatched tasks: insufficient resources",
                                execution_id,
                            )
                            break

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
                                "execution[%s] Running task synchronously in main thread: %s",
                                execution_id,
                                next_step,
                            )
                            materialized_results = self.build_partitions(
                                next_step.instructions,
                                next_step.inputs,
                                next_step.partial_metadatas,
                            )

                            self._release_resources(next_step.resource_request)

                            next_step.set_result(materialized_results)

                        else:
                            # Submit the task for execution.
                            logger.debug("execution[%s] Submitting task for execution: %s", execution_id, next_step)

                            # update progress bar
                            pbar.mark_task_start(next_step)

                            if next_step.actor_pool_id is None:
                                future = self._thread_pool.submit(
                                    self.build_partitions,
                                    next_step.instructions,
                                    next_step.inputs,
                                    next_step.partial_metadatas,
                                )
                            else:
                                actor_pool = self._actor_pools.get(next_step.actor_pool_id)
                                assert (
                                    actor_pool is not None
                                ), f"PyActorPool={next_step.actor_pool_id} must outlive the tasks that need to be run on it."
                                future = actor_pool.submit(
                                    next_step.instructions,
                                    next_step.inputs,
                                    next_step.partial_metadatas,
                                )

                            resource_request = next_step.resource_request

                            future.add_done_callback(lambda _: self._release_resources(resource_request))

                            # Register the inflight task
                            assert (
                                next_step.id() not in local_futures_to_task
                            ), "Step IDs should be unique - this indicates an internal error, please file an issue!"
                            self._inflight_futures[(execution_id, next_step.id())] = future
                            local_futures_to_task[future] = next_step

                        next_step = next(plan)

                if next_step is None and not len(local_futures_to_task) > 0:
                    raise RuntimeError(
                        f"Scheduler deadlocked! This should never happen. Please file an issue. Current step: {type(next_step)}"
                    )

                # Await at least one task in the global futures to finish before proceeding
                _ = futures.wait(list(self._inflight_futures.values()), return_when=futures.FIRST_COMPLETED)

                # Now await at a task in the local futures to finish, so as to progress the local execution
                done_set, _ = futures.wait(list(local_futures_to_task), return_when=futures.FIRST_COMPLETED)
                for done_future in done_set:
                    done_task = local_futures_to_task.pop(done_future)
                    materialized_results = done_future.result()

                    pbar.mark_task_done(done_task)
                    del self._inflight_futures[(execution_id, done_task.id())]

                    logger.debug(
                        "execution[%s] Task completed: %s -> <%s partitions>",
                        execution_id,
                        done_task.id(),
                        len(materialized_results),
                    )

                    done_task.set_result(materialized_results)

                if next_step is None:
                    next_step = next(plan)

        # StopIteration is raised when the plan is exhausted, and all materialized results have been yielded.
        except StopIteration:
            logger.debug("execution[%s] Exhausted all materialized results", execution_id)

        # Perform any cleanups when the generator is closed (StopIteration is raised, generator is deleted with `__del__` on GC, etc)
        finally:
            # Close the progress bar
            pbar.close()

            # Cleanup any remaining inflight futures/results from this local execution
            for (exec_id, task_id), _ in list(self._inflight_futures.items()):
                if exec_id == execution_id:
                    del self._inflight_futures[(exec_id, task_id)]

    def _check_resource_requests(self, resource_request: ResourceRequest) -> None:
        """Validates that the requested ResourceRequest is possible to run locally"""

        if resource_request.num_cpus is not None and resource_request.num_cpus > self.num_cpus:
            raise RuntimeError(f"Requested {resource_request.num_cpus} CPUs but found only {self.num_cpus} available")
        if resource_request.num_gpus is not None and resource_request.num_gpus > self.num_gpus:
            raise RuntimeError(f"Requested {resource_request.num_gpus} GPUs but found only {self.num_gpus} available")
        if resource_request.memory_bytes is not None and resource_request.memory_bytes > self.total_bytes_memory:
            raise RuntimeError(
                f"Requested {resource_request.memory_bytes} bytes of memory but found only {self.total_bytes_memory} available"
            )

    def _attempt_admit_task(
        self,
        resource_request: ResourceRequest,
    ) -> bool:
        self._check_resource_requests(resource_request)

        with self._resource_accounting_lock:
            memory_okay = (resource_request.memory_bytes or 0) <= self._available_bytes_memory
            cpus_okay = (resource_request.num_cpus or 0) <= self._available_cpus
            gpus_okay = (resource_request.num_gpus or 0) <= self._available_gpus
            all_okay = all((cpus_okay, gpus_okay, memory_okay))

            # Update resource accounting if we have the resources (this is considered as the task being "admitted")
            if all_okay:
                self._available_bytes_memory -= resource_request.memory_bytes or 0
                self._available_cpus -= resource_request.num_cpus or 0.0
                self._available_gpus -= resource_request.num_gpus or 0.0

            return all_okay

    def _release_resources(self, resource_request: ResourceRequest) -> None:
        with self._resource_accounting_lock:
            self._available_bytes_memory += resource_request.memory_bytes or 0
            self._available_cpus += resource_request.num_cpus or 0.0
            self._available_gpus += resource_request.num_gpus or 0.0

    def build_partitions(
        self,
        instruction_stack: list[Instruction],
        partitions: list[MicroPartition],
        final_metadata: list[PartialPartitionMetadata],
    ) -> list[MaterializedResult[MicroPartition]]:
        for instruction in instruction_stack:
            partitions = instruction.run(partitions)

        results: list[MaterializedResult[MicroPartition]] = [
            PyMaterializedResult(part, PartitionMetadata.from_table(part).merge_with_partial(partial))
            for part, partial in zip(partitions, final_metadata)
        ]
        return results


@dataclass
class PyMaterializedResult(MaterializedResult[MicroPartition]):
    _partition: MicroPartition
    _metadata: PartitionMetadata | None = None

    def partition(self) -> MicroPartition:
        return self._partition

    def micropartition(self) -> MicroPartition:
        return self._partition

    def metadata(self) -> PartitionMetadata:
        if self._metadata is None:
            self._metadata = PartitionMetadata.from_table(self._partition)
        return self._metadata

    def cancel(self) -> None:
        return None

    def _noop(self, _: MicroPartition) -> None:
        return None
