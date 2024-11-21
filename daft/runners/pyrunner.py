from __future__ import annotations

import contextlib
import logging
import multiprocessing as mp
import threading
import uuid
from concurrent import futures
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Iterator

from daft.context import get_context
from daft.daft import FileFormatConfig, FileInfos, IOConfig, ResourceRequest, SystemInfo
from daft.execution.native_executor import NativeExecutor
from daft.execution.physical_plan import ActorPoolManager
from daft.filesystem import glob_path_with_stats
from daft.internal.gpu import cuda_visible_devices
from daft.runners import runner_io
from daft.runners.partitioning import (
    LocalMaterializedResult,
    LocalPartitionSet,
    MaterializedResult,
    PartialPartitionMetadata,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSetCache,
)
from daft.runners.profiler import profiler
from daft.runners.progress_bar import ProgressBar
from daft.runners.runner import LOCAL_PARTITION_SET_CACHE, Runner
from daft.table import MicroPartition

if TYPE_CHECKING:
    from daft.execution import physical_plan
    from daft.execution.execution_step import Instruction, PartitionTask
    from daft.expressions import ExpressionsProjection
    from daft.logical.builder import LogicalPlanBuilder

logger = logging.getLogger(__name__)


# Unique UUID for each execution
ExecutionID = str

# Unique ID for each task
TaskID = str


@dataclass
class AcquiredResources:
    num_cpus: float
    gpus: dict[str, float]
    memory_bytes: int


class PyRunnerResources:
    def __init__(self, num_cpus: float, gpus: list[str], memory_bytes: int):
        gpus_dict = {gpu: 1.0 for gpu in gpus}
        self.num_cpus = num_cpus
        self.num_gpus = len(gpus)
        self.memory_bytes = memory_bytes

        self.available_resources = AcquiredResources(num_cpus, gpus_dict, memory_bytes)
        self.lock = threading.Lock()

    def try_acquire(self, resource_request: ResourceRequest) -> AcquiredResources | None:
        resources = self.try_acquire_multiple([resource_request])
        return resources[0] if resources is not None else None

    def try_acquire_multiple(self, resource_requests: list[ResourceRequest]) -> list[AcquiredResources] | None:
        """
        Attempts to acquire the requested resources.

        If the requested resources are available, returns a list of `AcquiredResources` with the amount of acquired CPUs and memory, as well as the specific GPUs that were acquired per request.

        If the requested resources are not available, returns None.
        """
        all_requested_cpus = [r.num_cpus or 0.0 for r in resource_requests]
        total_requested_cpus = sum(all_requested_cpus)

        all_requested_memory_bytes = [r.memory_bytes or 0 for r in resource_requests]
        total_requested_memory_bytes = sum(all_requested_memory_bytes)

        total_requested_gpus = sum([r.num_gpus or 0.0 for r in resource_requests])

        for resource_name, requested, total in [
            ("CPUs", total_requested_cpus, self.num_cpus),
            ("bytes of memory", total_requested_memory_bytes, self.memory_bytes),
            ("GPUs", total_requested_gpus, self.num_gpus),
        ]:
            if requested > total:
                raise RuntimeError(f"Requested {requested} {resource_name} but found only {total} available")

        with self.lock:
            if total_requested_cpus > self.available_resources.num_cpus:
                return None

            if total_requested_memory_bytes > self.available_resources.memory_bytes:
                return None

            remaining_available_gpus = self.available_resources.gpus.copy()
            all_requested_gpus = []

            # choose GPUs for resource requests
            for r in resource_requests:
                num_gpus = r.num_gpus or 0.0
                chosen_gpus = {}

                if num_gpus.is_integer():
                    for device in remaining_available_gpus:
                        if num_gpus == 0:
                            break

                        if remaining_available_gpus[device] == 1.0:
                            chosen_gpus[device] = 1.0
                            num_gpus -= 1.0

                    if num_gpus > 0:
                        return None
                else:
                    # do not allow fractional GPUs above 1.0, similar to Ray's behavior
                    # this should have been validated when creating the resource request so we only do an assert here
                    assert 0 <= num_gpus < 1

                    chosen_gpu = None

                    # greedily choose GPU that has lowest fraction available which can fit the requested fraction
                    for device, fraction in remaining_available_gpus.items():
                        if fraction >= num_gpus:
                            if chosen_gpu is None or fraction < remaining_available_gpus[chosen_gpu]:
                                chosen_gpu = device

                    if chosen_gpu is None:
                        return None

                    chosen_gpus[chosen_gpu] = num_gpus

                for device, fraction in chosen_gpus.items():
                    remaining_available_gpus[device] -= fraction

                all_requested_gpus.append(chosen_gpus)

            self.available_resources.num_cpus -= total_requested_cpus
            self.available_resources.memory_bytes -= total_requested_memory_bytes
            self.available_resources.gpus = remaining_available_gpus

            return [
                AcquiredResources(num_cpus, gpus, memory_bytes)
                for num_cpus, gpus, memory_bytes in zip(
                    all_requested_cpus, all_requested_gpus, all_requested_memory_bytes
                )
            ]

    def release(self, resources: AcquiredResources | list[AcquiredResources]):
        """Admit the resources back into the resource pool."""
        with self.lock:
            if not isinstance(resources, list):
                resources = [resources]

            for r in resources:
                self.available_resources.num_cpus += r.num_cpus
                self.available_resources.memory_bytes += r.memory_bytes
                for gpu, amount in r.gpus.items():
                    self.available_resources.gpus[gpu] += amount


class PyStatefulActorSingleton:
    """
    This class stores the singleton `initialized_projection` that is isolated to each Python process. It stores the projection with initialized stateful UDF objects of a single actor.

    Currently, only one stateful UDF per actor is supported, but we allow multiple here in case we want to support multiple stateful UDFs in the future.

    Note: The class methods should only be called inside of actor processes.
    """

    initialized_projection: ExpressionsProjection | None = None

    @staticmethod
    def initialize_actor_global_state(
        uninitialized_projection: ExpressionsProjection,
        cuda_device_queue: mp.Queue[str],
    ):
        if PyStatefulActorSingleton.initialized_projection is not None:
            raise RuntimeError("Cannot initialize Python process actor twice.")

        import os

        from daft.execution.stateful_actor import initialize_actor_pool_projection

        os.environ["CUDA_VISIBLE_DEVICES"] = cuda_device_queue.get(timeout=1)

        PyStatefulActorSingleton.initialized_projection = initialize_actor_pool_projection(uninitialized_projection)

    @staticmethod
    def build_partitions_with_stateful_project(
        partition: MicroPartition,
        partial_metadata: PartialPartitionMetadata,
    ) -> list[MaterializedResult[MicroPartition]]:
        # Bind the expressions to the initialized stateful UDFs, which should already have been initialized at process start-up
        assert (
            PyStatefulActorSingleton.initialized_projection is not None
        ), "PyActor process must be initialized with stateful UDFs before execution"

        new_part = partition.eval_expression_list(PyStatefulActorSingleton.initialized_projection)
        return [
            LocalMaterializedResult(
                new_part, PartitionMetadata.from_table(new_part).merge_with_partial(partial_metadata)
            )
        ]


class PyActorPool:
    def __init__(
        self,
        pool_id: str,
        num_actors: int,
        resources: list[AcquiredResources],
        projection: ExpressionsProjection,
    ):
        self._pool_id = pool_id
        self._num_actors = num_actors
        self._resources = resources
        self._executor: futures.ProcessPoolExecutor | None = None
        self._projection = projection

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
        partition = partitions[0]
        partial_metadata = final_metadata[0]

        return self._executor.submit(
            PyStatefulActorSingleton.build_partitions_with_stateful_project,
            partition,
            partial_metadata,
        )

    def teardown(self) -> None:
        # Shut down the executor
        assert self._executor is not None, "Should have an executor when exiting context"
        self._executor.shutdown()
        self._executor = None

    def setup(self) -> None:
        cuda_device_queue: mp.Queue[str] = mp.Queue()
        for r in self._resources:
            visible_device_str = ",".join(r.gpus.keys())
            cuda_device_queue.put(visible_device_str)

        self._executor = futures.ProcessPoolExecutor(
            self._num_actors,
            initializer=PyStatefulActorSingleton.initialize_actor_global_state,
            initargs=(self._projection, cuda_device_queue),
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


class PyRunner(Runner[MicroPartition], ActorPoolManager):
    name = "py"

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

            num_cpus = multiprocessing.cpu_count()

        gpus = cuda_visible_devices()
        memory_bytes = system_info.total_memory()

        self._resources = PyRunnerResources(
            num_cpus,
            gpus,
            memory_bytes,
        )

    def initialize_partition_set_cache(self) -> PartitionSetCache:
        return LOCAL_PARTITION_SET_CACHE

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
    ) -> Iterator[LocalMaterializedResult]:
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
                    self,
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
                tasks = plan_scheduler.to_partition_tasks(psets, self, results_buffer_size)
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

        resources = self._resources.try_acquire_multiple([actor_resource_request] * num_actors)
        if resources is None:
            raise RuntimeError(
                f"Not enough resources available to admit {num_actors} actors, each with resource request: {actor_resource_request}"
            )

        try:
            self._actor_pools[actor_pool_id] = PyActorPool(
                actor_pool_id,
                num_actors,
                resources,
                projection,
            )
            self._actor_pools[actor_pool_id].setup()
            logger.debug(
                "Created actor pool %s with %s actors, each with resources: %s",
                actor_pool_id,
                num_actors,
                actor_resource_request,
            )
            yield actor_pool_id
        # NOTE: Ensure that teardown always occurs regardless of any errors that occur during actor pool setup or execution
        finally:
            logger.debug("Tearing down actor pool: %s", actor_pool_id)
            self._resources.release(resources)
            self._actor_pools[actor_pool_id].teardown()
            del self._actor_pools[actor_pool_id]

    def _create_resource_release_callback(self, resources: AcquiredResources) -> Callable[[futures.Future], None]:
        """
        This higher order function is used so that the `resources` released by the callback
        are from the ones stored in the variable at the creation of the callback instead of during its call.
        """
        return lambda _: self._resources.release(resources)

    def _physical_plan_to_partitions(
        self,
        execution_id: str,
        plan: physical_plan.MaterializedPhysicalPlan[MicroPartition],
    ) -> Iterator[LocalMaterializedResult]:
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
                        assert isinstance(next_step, LocalMaterializedResult)

                        # A final result.
                        logger.debug("execution[%s] Yielding completed step", execution_id)
                        yield next_step
                        next_step = next(plan)
                        continue

                    else:
                        # next_task is a task to run.
                        resources = self._resources.try_acquire(next_step.resource_request)

                        if resources is None:
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

                            self._resources.release(resources)

                            next_step.set_result(materialized_results)
                            next_step.set_done()

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

                            future.add_done_callback(self._create_resource_release_callback(resources))

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
                    done_task.set_done()

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

    def build_partitions(
        self,
        instruction_stack: list[Instruction],
        partitions: list[MicroPartition],
        final_metadata: list[PartialPartitionMetadata],
    ) -> list[MaterializedResult[MicroPartition]]:
        for instruction in instruction_stack:
            partitions = instruction.run(partitions)

        results: list[MaterializedResult[MicroPartition]] = [
            LocalMaterializedResult(part, PartitionMetadata.from_table(part).merge_with_partial(partial))
            for part, partial in zip(partitions, final_metadata)
        ]
        return results
