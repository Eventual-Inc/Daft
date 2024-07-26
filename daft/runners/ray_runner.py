from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime
from queue import Full, Queue
from typing import TYPE_CHECKING, Any, Generator, Iterable, Iterator

import pyarrow as pa

from daft.context import get_context, set_execution_config
from daft.logical.builder import LogicalPlanBuilder
from daft.plan_scheduler import PhysicalPlanScheduler
from daft.runners.progress_bar import ProgressBar

logger = logging.getLogger(__name__)

try:
    import ray
except ImportError:
    logger.error(
        "Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
    )
    raise

from daft.daft import (
    FileFormatConfig,
    FileInfos,
    IOConfig,
    PyDaftExecutionConfig,
    ResourceRequest,
)
from daft.datatype import DataType
from daft.execution.execution_step import (
    FanoutInstruction,
    Instruction,
    MultiOutputPartitionTask,
    PartitionTask,
    ReduceInstruction,
    ScanWithTask,
    SingleOutputPartitionTask,
)
from daft.filesystem import glob_path_with_stats
from daft.runners import runner_io
from daft.runners.partitioning import (
    MaterializedResult,
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
)
from daft.runners.profiler import profiler
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner
from daft.table import MicroPartition

if TYPE_CHECKING:
    import dask
    import pandas as pd
    from ray.data.block import Block as RayDatasetBlock
    from ray.data.dataset import Dataset as RayDataset

_RAY_FROM_ARROW_REFS_AVAILABLE = True
try:
    from ray.data import from_arrow_refs
except ImportError:
    _RAY_FROM_ARROW_REFS_AVAILABLE = False

from daft.logical.schema import Schema

RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])


@ray.remote
def _glob_path_into_file_infos(
    paths: list[str],
    file_format_config: FileFormatConfig | None,
    io_config: IOConfig | None,
) -> MicroPartition:
    file_infos = FileInfos()
    file_format = file_format_config.file_format() if file_format_config is not None else None
    for path in paths:
        path_file_infos = glob_path_with_stats(path, file_format=file_format, io_config=io_config)
        if len(path_file_infos) == 0:
            raise FileNotFoundError(f"No files found at {path}")
        file_infos.extend(path_file_infos)

    return MicroPartition._from_pytable(file_infos.to_table())


@ray.remote
def _make_ray_block_from_vpartition(partition: MicroPartition) -> RayDatasetBlock:
    try:
        return partition.to_arrow(cast_tensors_to_ray_tensor_dtype=True)
    except pa.ArrowInvalid:
        return partition.to_pylist()


@ray.remote
def _make_daft_partition_from_ray_dataset_blocks(
    ray_dataset_block: pa.MicroPartition, daft_schema: Schema
) -> MicroPartition:
    return MicroPartition.from_arrow(ray_dataset_block)


@ray.remote(num_returns=2)
def _make_daft_partition_from_dask_dataframe_partitions(
    dask_df_partition: pd.DataFrame,
) -> tuple[MicroPartition, pa.Schema]:
    vpart = MicroPartition.from_pandas(dask_df_partition)
    return vpart, vpart.schema()


def _to_pandas_ref(df: pd.DataFrame | ray.ObjectRef[pd.DataFrame]) -> ray.ObjectRef[pd.DataFrame]:
    """Ensures that the provided pandas DataFrame partition is in the Ray object store."""
    import pandas as pd

    if isinstance(df, pd.DataFrame):
        return ray.put(df)
    elif isinstance(df, ray.ObjectRef):
        return df
    else:
        raise ValueError("Expected a Ray object ref or a Pandas DataFrame, " f"got {type(df)}")


class RayPartitionSet(PartitionSet[ray.ObjectRef]):
    _results: dict[PartID, RayMaterializedResult]

    def __init__(self) -> None:
        super().__init__()
        self._results = {}

    def items(self) -> list[tuple[PartID, MaterializedResult[ray.ObjectRef]]]:
        return [(pid, result) for pid, result in sorted(self._results.items())]

    def _get_merged_vpartition(self) -> MicroPartition:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        all_partitions = ray.get([part.partition() for id, part in ids_and_partitions])
        return MicroPartition.concat(all_partitions)

    def _get_preview_vpartition(self, num_rows: int) -> list[MicroPartition]:
        ids_and_partitions = self.items()
        preview_parts = []
        for _, mat_result in ids_and_partitions:
            ref: ray.ObjectRef = mat_result.partition()
            part: MicroPartition = ray.get(ref)
            part_len = len(part)
            if part_len >= num_rows:  # if this part has enough rows, take what we need and break
                preview_parts.append(part.slice(0, num_rows))
                break
            else:  # otherwise, take the whole part and keep going
                num_rows -= part_len
                preview_parts.append(part)
        return preview_parts

    def to_ray_dataset(self) -> RayDataset:
        if not _RAY_FROM_ARROW_REFS_AVAILABLE:
            raise ImportError(
                "Unable to import `ray.data.from_arrow_refs`. Please ensure that you have a compatible version of Ray >= 1.10 installed."
            )

        blocks = [_make_ray_block_from_vpartition.remote(self._results[k].partition()) for k in self._results.keys()]
        # NOTE: although the Ray method is called `from_arrow_refs`, this method works also when the blocks are List[T] types
        # instead of Arrow tables as the codepath for Dataset creation is the same.
        return from_arrow_refs(blocks)

    def to_dask_dataframe(
        self,
        meta: (pd.DataFrame | pd.Series | dict[str, Any] | Iterable[Any] | tuple[Any] | None) = None,
    ) -> dask.DataFrame:
        import dask
        import dask.dataframe as dd
        from ray.util.dask import ray_dask_get

        dask.config.set(scheduler=ray_dask_get)

        @dask.delayed
        def _make_dask_dataframe_partition_from_vpartition(partition: MicroPartition) -> pd.DataFrame:
            return partition.to_pandas()

        ddf_parts = [
            _make_dask_dataframe_partition_from_vpartition(self._results[k].partition()) for k in self._results.keys()
        ]
        return dd.from_delayed(ddf_parts, meta=meta)

    def get_partition(self, idx: PartID) -> ray.ObjectRef:
        return self._results[idx].partition()

    def set_partition(self, idx: PartID, result: MaterializedResult[ray.ObjectRef]) -> None:
        assert isinstance(result, RayMaterializedResult)
        self._results[idx] = result

    def delete_partition(self, idx: PartID) -> None:
        del self._results[idx]

    def has_partition(self, idx: PartID) -> bool:
        return idx in self._results

    def __len__(self) -> int:
        return sum(result.metadata().num_rows for result in self._results.values())

    def size_bytes(self) -> int | None:
        size_bytes_ = [result.metadata().size_bytes for result in self._results.values()]
        size_bytes: list[int] = [size for size in size_bytes_ if size is not None]
        if len(size_bytes) != len(size_bytes_):
            return None
        else:
            return sum(size_bytes)

    def num_partitions(self) -> int:
        return len(self._results)

    def wait(self) -> None:
        deduped_object_refs = {r.partition() for r in self._results.values()}
        ray.wait(list(deduped_object_refs))


class RayRunnerIO(runner_io.RunnerIO):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> FileInfos:
        # Synchronously fetch the file infos, for now.
        return FileInfos.from_table(
            ray.get(_glob_path_into_file_infos.remote(source_paths, file_format_config, io_config=io_config))
            .to_table()
            ._table
        )

    def partition_set_from_ray_dataset(
        self,
        ds: RayDataset,
    ) -> tuple[RayPartitionSet, Schema]:
        arrow_schema = ds.schema(fetch_if_missing=True)
        if not isinstance(arrow_schema, pa.Schema):
            # Convert Dataset to an Arrow dataset.
            extra_kwargs = {}
            if RAY_VERSION >= (2, 3, 0):
                # The zero_copy_batch kwarg was added in Ray 2.3.0.
                extra_kwargs["zero_copy_batch"] = True
            ds = ds.map_batches(
                lambda x: x,
                batch_size=None,
                batch_format="pyarrow",
                **extra_kwargs,
            )
            arrow_schema = ds.schema(fetch_if_missing=True)

            # Ray 2.5.0 broke the API by using its own `ray.data.dataset.Schema` instead of PyArrow schemas
            if RAY_VERSION >= (2, 5, 0):
                arrow_schema = pa.schema({name: t for name, t in zip(arrow_schema.names, arrow_schema.types)})

        daft_schema = Schema._from_field_name_and_types(
            [(arrow_field.name, DataType.from_arrow_type(arrow_field.type)) for arrow_field in arrow_schema]
        )
        block_refs = ds.get_internal_block_refs()

        # NOTE: This materializes the entire Ray Dataset - we could make this more intelligent by creating a new RayDatasetScan node
        # which can iterate on Ray Dataset blocks and materialize as-needed
        daft_vpartitions = [
            _make_daft_partition_from_ray_dataset_blocks.remote(block, daft_schema) for block in block_refs
        ]
        pset = RayPartitionSet()

        for i, obj in enumerate(daft_vpartitions):
            pset.set_partition(i, RayMaterializedResult(obj))
        return (
            pset,
            daft_schema,
        )

    def partition_set_from_dask_dataframe(
        self,
        ddf: dask.DataFrame,
    ) -> tuple[RayPartitionSet, Schema]:
        import dask
        from ray.util.dask import ray_dask_get

        partitions = ddf.to_delayed()
        if not partitions:
            raise ValueError("Can't convert an empty Dask DataFrame (with no partitions) to a Daft DataFrame.")
        persisted_partitions = dask.persist(*partitions, scheduler=ray_dask_get)
        parts = [_to_pandas_ref(next(iter(part.dask.values()))) for part in persisted_partitions]
        daft_vpartitions, schemas = zip(*(_make_daft_partition_from_dask_dataframe_partitions.remote(p) for p in parts))
        schemas = ray.get(list(schemas))
        # Dask shouldn't allow inconsistent schemas across partitions, but we double-check here.
        if not all(schemas[0] == schema for schema in schemas[1:]):
            raise ValueError(
                "Can't convert a Dask DataFrame with inconsistent schemas across partitions to a Daft DataFrame:",
                schemas,
            )

        pset = RayPartitionSet()

        for i, obj in enumerate(daft_vpartitions):
            pset.set_partition(i, RayMaterializedResult(obj))
        return (
            pset,
            schemas[0],
        )


def _get_ray_task_options(resource_request: ResourceRequest) -> dict[str, Any]:
    options = {}
    # FYI: Ray's default resource behaviour is documented here:
    # https://docs.ray.io/en/latest/ray-core/tasks/resources.html
    if resource_request.num_cpus is not None:
        # Ray worker pool will thrash if a request comes in for fractional cpus,
        # so we floor the request to at least 1 cpu here.
        options["num_cpus"] = max(1, resource_request.num_cpus)
    if resource_request.num_gpus:
        options["num_gpus"] = resource_request.num_gpus
    if resource_request.memory_bytes:
        # Note that lower versions of Ray do not accept a value of 0 here,
        # so the if-clause is load-bearing.
        options["memory"] = resource_request.memory_bytes
    return options


def build_partitions(
    instruction_stack: list[Instruction], partial_metadatas: list[PartitionMetadata], *inputs: MicroPartition
) -> list[list[PartitionMetadata] | MicroPartition]:
    partitions = list(inputs)
    for instruction in instruction_stack:
        partitions = instruction.run(partitions)

    assert len(partial_metadatas) == len(partitions), f"{len(partial_metadatas)} vs {len(partitions)}"

    metadatas = [PartitionMetadata.from_table(p).merge_with_partial(m) for p, m in zip(partitions, partial_metadatas)]

    return [metadatas, *partitions]


# Give the same function different names to aid in profiling data distribution.


@ray.remote
def single_partition_pipeline(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    *inputs: MicroPartition,
) -> list[list[PartitionMetadata] | MicroPartition]:
    set_execution_config(daft_execution_config)
    return build_partitions(instruction_stack, partial_metadatas, *inputs)


@ray.remote
def fanout_pipeline(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    *inputs: MicroPartition,
) -> list[list[PartitionMetadata] | MicroPartition]:
    set_execution_config(daft_execution_config)
    return build_partitions(instruction_stack, partial_metadatas, *inputs)


@ray.remote(scheduling_strategy="SPREAD")
def reduce_pipeline(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    inputs: list,
) -> list[list[PartitionMetadata] | MicroPartition]:
    import ray

    set_execution_config(daft_execution_config)
    return build_partitions(instruction_stack, partial_metadatas, *ray.get(inputs))


@ray.remote(scheduling_strategy="SPREAD")
def reduce_and_fanout(
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    inputs: list,
) -> list[list[PartitionMetadata] | MicroPartition]:
    import ray

    set_execution_config(daft_execution_config)
    return build_partitions(instruction_stack, partial_metadatas, *ray.get(inputs))


@ray.remote
def get_metas(*partitions: MicroPartition) -> list[PartitionMetadata]:
    return [PartitionMetadata.from_table(partition) for partition in partitions]


def _ray_num_cpus_provider(ttl_seconds: int = 1) -> Generator[int, None, None]:
    """Helper that gets the number of CPUs from Ray

    Used as a generator as it provides a guard against calling ray.cluster_resources()
    more than once per `ttl_seconds`.

    Example:
    >>> p = _ray_num_cpus_provider()
    >>> next(p)
    """
    last_checked_time = time.time()
    last_num_cpus_queried = int(ray.cluster_resources().get("CPU", 0))
    while True:
        currtime = time.time()
        if currtime - last_checked_time < ttl_seconds:
            yield last_num_cpus_queried
        else:
            last_checked_time = currtime
            last_num_cpus_queried = int(ray.cluster_resources().get("CPU", 0))
            yield last_num_cpus_queried


class Scheduler:
    def __init__(self, max_task_backlog: int | None, use_ray_tqdm: bool) -> None:
        """
        max_task_backlog: Max number of inflight tasks waiting for cores.
        """

        # As of writing, Ray does not seem to be guaranteed to support
        # more than this number of pending scheduling tasks.
        # Ray has an internal proto that reports backlogged tasks [1],
        # and each task proto can be up to 10 MiB [2],
        # and protobufs have a max size of 2GB (from errors empirically encountered).
        #
        # https://github.com/ray-project/ray/blob/8427de2776717b30086c277e5e8e140316dbd193/src/ray/protobuf/node_manager.proto#L32
        # https://github.com/ray-project/ray/blob/fb95f03f05981f232aa7a9073dd2c2512729e99a/src/ray/common/ray_config_def.h#LL513C1-L513C1
        self.max_task_backlog = max_task_backlog if max_task_backlog is not None else 180

        self.reserved_cores = 0

        self.execution_configs_objref_by_df: dict[str, ray.ObjectRef] = dict()
        self.threads_by_df: dict[str, threading.Thread] = dict()
        self.results_by_df: dict[str, Queue] = {}
        self.active_by_df: dict[str, bool] = dict()
        self.results_buffer_size_by_df: dict[str, int | None] = dict()

        self.use_ray_tqdm = use_ray_tqdm

    def next(self, result_uuid: str) -> RayMaterializedResult | StopIteration:
        # Case: thread is terminated and no longer exists.
        # Should only be hit for repeated calls to next() after StopIteration.
        if result_uuid not in self.threads_by_df:
            return StopIteration()

        # Case: thread needs to be terminated
        if not self.active_by_df.get(result_uuid, False):
            return StopIteration()

        # Common case: get the next result from the thread.
        result = self.results_by_df[result_uuid].get()

        return result

    def start_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None = None,
    ) -> None:
        self.execution_configs_objref_by_df[result_uuid] = ray.put(daft_execution_config)
        self.results_by_df[result_uuid] = Queue(maxsize=1 if results_buffer_size is not None else -1)
        self.active_by_df[result_uuid] = True
        self.results_buffer_size_by_df[result_uuid] = results_buffer_size

        t = threading.Thread(
            target=self._run_plan,
            name=result_uuid,
            kwargs={
                "plan_scheduler": plan_scheduler,
                "psets": psets,
                "result_uuid": result_uuid,
            },
        )
        t.start()
        self.threads_by_df[result_uuid] = t

    def active_plans(self) -> list[str]:
        return [r_uuid for r_uuid, is_active in self.active_by_df.items() if is_active]

    def stop_plan(self, result_uuid: str) -> None:
        if result_uuid in self.active_by_df:
            # Mark df as non-active
            self.active_by_df[result_uuid] = False
            # wait till thread gracefully completes
            self.threads_by_df[result_uuid].join()
            # remove thread and history of df
            del self.threads_by_df[result_uuid]
            del self.active_by_df[result_uuid]
            del self.results_by_df[result_uuid]
            del self.results_buffer_size_by_df[result_uuid]

    def _run_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
    ) -> None:
        # Get executable tasks from plan scheduler.
        results_buffer_size = self.results_buffer_size_by_df[result_uuid]
        tasks = plan_scheduler.to_partition_tasks(
            psets,
            # Attempt to subtract 1 from results_buffer_size because the return Queue size is already 1
            # If results_buffer_size=1 though, we can't do much and the total buffer size actually has to be >= 2
            # because we have two buffers (the Queue and the buffer inside the `materialize` generator)
            None if results_buffer_size is None else max(results_buffer_size - 1, 1),
        )

        daft_execution_config = self.execution_configs_objref_by_df[result_uuid]
        inflight_tasks: dict[str, PartitionTask[ray.ObjectRef]] = dict()
        inflight_ref_to_task: dict[ray.ObjectRef, str] = dict()
        pbar = ProgressBar(use_ray_tqdm=self.use_ray_tqdm)
        num_cpus_provider = _ray_num_cpus_provider()

        start = datetime.now()
        profile_filename = (
            f"profile_RayRunner.run()_"
            f"{datetime.replace(datetime.now(), second=0, microsecond=0).isoformat()[:-3]}.json"
        )

        def is_active():
            return self.active_by_df.get(result_uuid, False)

        def place_in_queue(item):
            while is_active():
                try:
                    self.results_by_df[result_uuid].put(item, timeout=0.1)
                    break
                except Full:
                    pass

        with profiler(profile_filename):
            try:
                next_step = next(tasks)

                while is_active():  # Loop: Dispatch -> await.
                    while is_active():  # Loop: Dispatch (get tasks -> batch dispatch).
                        tasks_to_dispatch: list[PartitionTask] = []

                        # TODO: improve control loop code to be more understandable and dynamically adjust backlog
                        cores: int = max(
                            next(num_cpus_provider) - self.reserved_cores, 1
                        )  # assume at least 1 CPU core for bootstrapping clusters that scale from zero
                        max_inflight_tasks = cores + self.max_task_backlog
                        dispatches_allowed = max_inflight_tasks - len(inflight_tasks)
                        dispatches_allowed = min(cores, dispatches_allowed)

                        # Loop: Get a batch of tasks.
                        while len(tasks_to_dispatch) < dispatches_allowed and is_active():
                            if next_step is None:
                                # Blocked on already dispatched tasks; await some tasks.
                                break

                            elif isinstance(next_step, MaterializedResult):
                                # A final result.
                                place_in_queue(next_step)
                                next_step = next(tasks)

                            # next_step is a task.

                            # If it is a no-op task, just run it locally immediately.
                            elif len(next_step.instructions) == 0:
                                logger.debug("Running task synchronously in main thread: %s", next_step)
                                assert (
                                    len(next_step.partial_metadatas) == 1
                                ), "No-op tasks must have one output by definition, since there are no instructions to run"
                                [single_partial] = next_step.partial_metadatas
                                if single_partial.num_rows is None:
                                    [single_meta] = ray.get(get_metas.remote(next_step.inputs))
                                    accessor = PartitionMetadataAccessor.from_metadata_list(
                                        [single_meta.merge_with_partial(single_partial)]
                                    )
                                else:
                                    accessor = PartitionMetadataAccessor.from_metadata_list(
                                        [
                                            PartitionMetadata(
                                                num_rows=single_partial.num_rows,
                                                size_bytes=single_partial.size_bytes,
                                                boundaries=single_partial.boundaries,
                                            )
                                        ]
                                    )

                                next_step.set_result(
                                    [RayMaterializedResult(partition, accessor, 0) for partition in next_step.inputs]
                                )
                                next_step = next(tasks)

                            else:
                                # Add the task to the batch.
                                tasks_to_dispatch.append(next_step)
                                next_step = next(tasks)

                        # Dispatch the batch of tasks.
                        logger.debug(
                            "%ss: RayRunner dispatching %s tasks",
                            (datetime.now() - start).total_seconds(),
                            len(tasks_to_dispatch),
                        )

                        if not is_active():
                            break

                        for task in tasks_to_dispatch:
                            results = _build_partitions(daft_execution_config, task)
                            logger.debug("%s -> %s", task, results)
                            inflight_tasks[task.id()] = task
                            for result in results:
                                inflight_ref_to_task[result] = task.id()

                            pbar.mark_task_start(task)

                        if dispatches_allowed == 0 or next_step is None:
                            break

                    # Await a batch of tasks.
                    # (Awaits the next task, and then the next batch of tasks within 10ms.)

                    dispatch = datetime.now()
                    completed_task_ids = []
                    for wait_for in ("next_one", "next_batch"):
                        if not is_active():
                            break

                        if wait_for == "next_one":
                            num_returns = 1
                            timeout = None
                        elif wait_for == "next_batch":
                            num_returns = len(inflight_ref_to_task)
                            timeout = 0.01  # 10ms

                        if num_returns == 0:
                            break

                        readies, _ = ray.wait(
                            list(inflight_ref_to_task.keys()),
                            num_returns=num_returns,
                            timeout=timeout,
                            fetch_local=False,
                        )

                        for ready in readies:
                            if ready in inflight_ref_to_task:
                                task_id = inflight_ref_to_task[ready]
                                completed_task_ids.append(task_id)
                                # Mark the entire task associated with the result as done.
                                task = inflight_tasks[task_id]
                                if isinstance(task, SingleOutputPartitionTask):
                                    del inflight_ref_to_task[ready]
                                elif isinstance(task, MultiOutputPartitionTask):
                                    for partition in task.partitions():
                                        del inflight_ref_to_task[partition]

                                pbar.mark_task_done(task)
                                del inflight_tasks[task_id]

                    logger.debug(
                        "%ss to await results from %s", (datetime.now() - dispatch).total_seconds(), completed_task_ids
                    )

                    if next_step is None:
                        next_step = next(tasks)

            except StopIteration as e:
                place_in_queue(e)

            # Ensure that all Exceptions are correctly propagated to the consumer before reraising to kill thread
            except Exception as e:
                place_in_queue(e)
                pbar.close()
                raise

        pbar.close()


SCHEDULER_ACTOR_NAME = "scheduler"
SCHEDULER_ACTOR_NAMESPACE = "daft"


@ray.remote(num_cpus=1)
class SchedulerActor(Scheduler):
    def __init__(self, *n, **kw) -> None:
        super().__init__(*n, **kw)
        self.reserved_cores = 1


def _build_partitions(
    daft_execution_config_objref: ray.ObjectRef, task: PartitionTask[ray.ObjectRef]
) -> list[ray.ObjectRef]:
    """Run a PartitionTask and return the resulting list of partitions."""
    ray_options: dict[str, Any] = {"num_returns": task.num_results + 1, "name": task.name()}

    if task.resource_request is not None:
        ray_options = {**ray_options, **_get_ray_task_options(task.resource_request)}

    if isinstance(task.instructions[0], ReduceInstruction):
        build_remote = (
            reduce_and_fanout
            if task.instructions and isinstance(task.instructions[-1], FanoutInstruction)
            else reduce_pipeline
        )
        build_remote = build_remote.options(**ray_options)
        [metadatas_ref, *partitions] = build_remote.remote(
            daft_execution_config_objref, task.instructions, task.partial_metadatas, task.inputs
        )

    else:
        build_remote = (
            fanout_pipeline
            if task.instructions and isinstance(task.instructions[-1], FanoutInstruction)
            else single_partition_pipeline
        )
        if task.instructions and isinstance(task.instructions[0], ScanWithTask):
            ray_options["scheduling_strategy"] = "SPREAD"
        build_remote = build_remote.options(**ray_options)
        [metadatas_ref, *partitions] = build_remote.remote(
            daft_execution_config_objref, task.instructions, task.partial_metadatas, *task.inputs
        )

    metadatas_accessor = PartitionMetadataAccessor(metadatas_ref)
    task.set_result(
        [
            RayMaterializedResult(
                partition=partition,
                metadatas=metadatas_accessor,
                metadata_idx=i,
            )
            for i, partition in enumerate(partitions)
        ]
    )

    return partitions


class RayRunner(Runner[ray.ObjectRef]):
    def __init__(
        self,
        address: str | None,
        max_task_backlog: int | None,
    ) -> None:
        super().__init__()
        if ray.is_initialized():
            if address is not None:
                logger.warning(
                    "Ray has already been initialized, Daft will reuse the existing Ray context and ignore the "
                    "supplied address: %s",
                    address,
                )
        else:
            ray.init(address=address)

        # Check if Ray is running in "client mode" (connected to a Ray cluster via a Ray client)
        self.ray_client_mode = ray.util.client.ray.get_context().is_connected()

        if self.ray_client_mode:
            # Run scheduler remotely if the cluster is connected remotely.
            self.scheduler_actor = SchedulerActor.options(  # type: ignore
                name=SCHEDULER_ACTOR_NAME,
                namespace=SCHEDULER_ACTOR_NAMESPACE,
                get_if_exists=True,
            ).remote(  # type: ignore
                max_task_backlog=max_task_backlog,
                use_ray_tqdm=True,
            )
        else:
            self.scheduler = Scheduler(
                max_task_backlog=max_task_backlog,
                use_ray_tqdm=False,
            )

    def active_plans(self) -> list[str]:
        if self.ray_client_mode:
            return ray.get(self.scheduler_actor.active_plans.remote())
        else:
            return self.scheduler.active_plans()

    def _start_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None = None,
    ) -> str:
        psets = {k: v.values() for k, v in self._part_set_cache.get_all_partition_sets().items()}
        result_uuid = str(uuid.uuid4())
        if self.ray_client_mode:
            ray.get(
                self.scheduler_actor.start_plan.remote(
                    daft_execution_config=daft_execution_config,
                    plan_scheduler=plan_scheduler,
                    psets=psets,
                    result_uuid=result_uuid,
                    results_buffer_size=results_buffer_size,
                )
            )
        else:
            self.scheduler.start_plan(
                daft_execution_config=daft_execution_config,
                plan_scheduler=plan_scheduler,
                psets=psets,
                result_uuid=result_uuid,
                results_buffer_size=results_buffer_size,
            )
        return result_uuid

    def _stream_plan(self, result_uuid: str) -> Iterator[RayMaterializedResult]:
        try:
            while True:
                if self.ray_client_mode:
                    result = ray.get(self.scheduler_actor.next.remote(result_uuid))
                else:
                    result = self.scheduler.next(result_uuid)

                if isinstance(result, StopIteration):
                    break
                elif isinstance(result, Exception):
                    raise result

                yield result
        finally:
            # Generator is out of scope, ensure that state has been cleaned up
            if self.ray_client_mode:
                ray.get(self.scheduler_actor.stop_plan.remote(result_uuid))
            else:
                self.scheduler.stop_plan(result_uuid)

    def run_iter(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[RayMaterializedResult]:
        # Grab and freeze the current DaftExecutionConfig
        daft_execution_config = get_context().daft_execution_config

        # Optimize the logical plan.
        builder = builder.optimize()

        if daft_execution_config.enable_aqe:
            adaptive_planner = builder.to_adaptive_physical_plan_scheduler(daft_execution_config)
            while not adaptive_planner.is_done():
                source_id, plan_scheduler = adaptive_planner.next()
                # don't store partition sets in variable to avoid reference
                result_uuid = self._start_plan(
                    plan_scheduler, daft_execution_config, results_buffer_size=results_buffer_size
                )
                del plan_scheduler
                results_iter = self._stream_plan(result_uuid)
                # if source_id is None that means this is the final stage
                if source_id is None:
                    yield from results_iter
                else:
                    cache_entry = self._collect_into_cache(results_iter)
                    adaptive_planner.update(source_id, cache_entry)
                    del cache_entry
        else:
            # Finalize the logical plan and get a physical plan scheduler for translating the
            # physical plan to executable tasks.
            plan_scheduler = builder.to_physical_plan_scheduler(daft_execution_config)

            result_uuid = self._start_plan(
                plan_scheduler, daft_execution_config, results_buffer_size=results_buffer_size
            )

            yield from self._stream_plan(result_uuid)

    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        for result in self.run_iter(builder, results_buffer_size=results_buffer_size):
            yield ray.get(result.partition())

    def _collect_into_cache(self, results_iter: Iterator[RayMaterializedResult]) -> PartitionCacheEntry:
        result_pset = RayPartitionSet()

        for i, result in enumerate(results_iter):
            result_pset.set_partition(i, result)

        pset_entry = self._part_set_cache.put_partition_set(result_pset)

        return pset_entry

    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry:
        results_iter = self.run_iter(builder)
        return self._collect_into_cache(results_iter)

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        if isinstance(pset, LocalPartitionSet):
            new_pset = RayPartitionSet()
            metadata_accessor = PartitionMetadataAccessor.from_metadata_list([v.metadata() for v in pset.values()])
            for i, (pid, py_mat_result) in enumerate(pset.items()):
                new_pset.set_partition(
                    pid, RayMaterializedResult(ray.put(py_mat_result.partition()), metadata_accessor, i)
                )
            pset = new_pset
        return self._part_set_cache.put_partition_set(pset=pset)

    def runner_io(self) -> RayRunnerIO:
        return RayRunnerIO()


class RayMaterializedResult(MaterializedResult[ray.ObjectRef]):
    def __init__(
        self,
        partition: ray.ObjectRef,
        metadatas: PartitionMetadataAccessor | None = None,
        metadata_idx: int | None = None,
    ):
        self._partition = partition
        if metadatas is None:
            assert metadata_idx is None
            metadatas = PartitionMetadataAccessor(get_metas.remote(self._partition))
            metadata_idx = 0
        self._metadatas = metadatas
        self._metadata_idx = metadata_idx

    def partition(self) -> ray.ObjectRef:
        return self._partition

    def vpartition(self) -> MicroPartition:
        return ray.get(self._partition)

    def metadata(self) -> PartitionMetadata:
        return self._metadatas.get_index(self._metadata_idx)

    def cancel(self) -> None:
        return ray.cancel(self._partition)

    def _noop(self, _: ray.ObjectRef) -> None:
        return None


class PartitionMetadataAccessor:
    """Wrapper class around Remote[List[PartitionMetadata]] to memoize lookups."""

    def __init__(self, ref: ray.ObjectRef) -> None:
        self._ref: ray.ObjectRef = ref
        self._metadatas: None | list[PartitionMetadata] = None

    def _get_metadatas(self) -> list[PartitionMetadata]:
        if self._metadatas is None:
            self._metadatas = ray.get(self._ref)
        return self._metadatas

    def get_index(self, key) -> PartitionMetadata:
        return self._get_metadatas()[key]

    @classmethod
    def from_metadata_list(cls, meta: list[PartitionMetadata]) -> PartitionMetadataAccessor:
        ref = ray.put(meta)
        accessor = cls(ref)
        accessor._metadatas = meta
        return accessor
