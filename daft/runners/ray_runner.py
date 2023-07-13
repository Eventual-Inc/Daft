from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from queue import Queue
from typing import TYPE_CHECKING, Any, Iterable, Iterator

import fsspec
import pyarrow as pa
from loguru import logger

try:
    import ray
except ImportError:
    logger.error(
        f"Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
    )
    raise

from daft.datasources import SourceInfo
from daft.datatype import DataType
from daft.execution import physical_plan_factory
from daft.execution.execution_step import (
    FanoutInstruction,
    Instruction,
    MaterializedResult,
    MultiOutputPartitionTask,
    PartitionTask,
    ReduceInstruction,
    SingleOutputPartitionTask,
)
from daft.filesystem import get_filesystem_from_path, glob_path_with_stats
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
from daft.resource_request import ResourceRequest
from daft.runners import runner_io
from daft.runners.partitioning import (
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
)
from daft.runners.profiler import profiler
from daft.runners.pyrunner import LocalPartitionSet
from daft.runners.runner import Runner
from daft.table import Table

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

RAY_VERSION = tuple(int(s) for s in ray.__version__.split("."))


@ray.remote
def _glob_path_into_details_vpartitions(
    paths: list[str],
    schema: Schema,
    source_info: SourceInfo | None,
    fs: fsspec.AbstractFileSystem | None,
) -> list[tuple[PartID, Table]]:
    all_listing_infos = []
    for path in paths:
        if fs is None:
            fs = get_filesystem_from_path(path)

        listing_infos = glob_path_with_stats(path, source_info, fs)
        if len(listing_infos) == 0:
            raise FileNotFoundError(f"No files found at {path}")
        all_listing_infos.extend(listing_infos)

    # Hardcoded to 1 partition
    partition = Table.from_pydict(
        {
            "path": pa.array([file_info.path for file_info in all_listing_infos], type=pa.string()),
            "size": pa.array([file_info.size for file_info in all_listing_infos], type=pa.int64()),
            "type": pa.array([file_info.type for file_info in all_listing_infos], type=pa.string()),
            "rows": pa.array([file_info.rows for file_info in all_listing_infos], type=pa.int64()),
        },
    )
    assert partition.schema() == schema, f"Schema should be expected: {schema}, but received: {partition.schema()}"

    partition_ref = ray.put(partition)
    partition_refs = [(0, partition_ref)]

    return partition_refs


@ray.remote
def _make_ray_block_from_vpartition(partition: Table) -> RayDatasetBlock:
    try:
        return partition.to_arrow(cast_tensors_to_ray_tensor_dtype=True)
    except pa.ArrowInvalid:
        return partition.to_pylist()


@ray.remote
def _make_daft_partition_from_ray_dataset_blocks(ray_dataset_block: pa.Table, daft_schema: Schema) -> Table:
    return Table.from_arrow(ray_dataset_block)


@ray.remote(num_returns=2)
def _make_daft_partition_from_dask_dataframe_partitions(dask_df_partition: pd.DataFrame) -> tuple[Table, pa.Schema]:
    vpart = Table.from_pandas(dask_df_partition)
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


@ray.remote
def remote_len_partition(p: Table) -> int:
    return len(p)


@ray.remote
def sample_schema_from_filepath_vpartition(
    p: Table,
    filepath_column: str,
    source_info: SourceInfo,
    fs: fsspec.AbstractFileSystem | None,
) -> Schema:
    """Ray remote function to run schema sampling on top of a Table containing filepaths"""
    assert len(p) > 0

    # Currently just samples the Schema from the first file
    first_filepath = p.to_pydict()[filepath_column][0]
    return runner_io.sample_schema(first_filepath, source_info, fs)


@dataclass
class RayPartitionSet(PartitionSet[ray.ObjectRef]):
    _partitions: dict[PartID, ray.ObjectRef]

    def items(self) -> list[tuple[PartID, ray.ObjectRef]]:
        return sorted(self._partitions.items())

    def _get_merged_vpartition(self) -> Table:
        ids_and_partitions = self.items()
        assert ids_and_partitions[0][0] == 0
        assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        all_partitions = ray.get([part for id, part in ids_and_partitions])
        return Table.concat(all_partitions)

    def to_ray_dataset(self) -> RayDataset:
        if not _RAY_FROM_ARROW_REFS_AVAILABLE:
            raise ImportError(
                "Unable to import `ray.data.from_arrow_refs`. Please ensure that you have a compatible version of Ray >= 1.10 installed."
            )

        blocks = [_make_ray_block_from_vpartition.remote(self._partitions[k]) for k in self._partitions.keys()]
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
        def _make_dask_dataframe_partition_from_vpartition(partition: Table) -> pd.DataFrame:
            return partition.to_pandas()

        ddf_parts = [
            _make_dask_dataframe_partition_from_vpartition(self._partitions[k]) for k in self._partitions.keys()
        ]
        return dd.from_delayed(ddf_parts, meta=meta)

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

    def len_of_partitions(self) -> list[int]:
        partition_ids = sorted(list(self._partitions.keys()))

        result: list[int] = ray.get([remote_len_partition.remote(self._partitions[pid]) for pid in partition_ids])
        return result

    def num_partitions(self) -> int:
        return len(self._partitions)

    def wait(self) -> None:
        ray.wait([o for o in self._partitions.values()])


class RayRunnerIO(runner_io.RunnerIO[ray.ObjectRef]):
    def glob_paths_details(
        self,
        source_paths: list[str],
        source_info: SourceInfo | None = None,
        fs: fsspec.AbstractFileSystem | None = None,
    ) -> RayPartitionSet:
        partition_refs = ray.get(
            _glob_path_into_details_vpartitions.remote(source_paths, RayRunnerIO.FS_LISTING_SCHEMA, source_info, fs=fs)
        )
        return RayPartitionSet({part_id: part for part_id, part in partition_refs})

    def get_schema_from_first_filepath(
        self,
        listing_details_partitions: PartitionSet[ray.ObjectRef],
        source_info: SourceInfo,
        fs: fsspec.AbstractFileSystem | None,
    ) -> Schema:
        nonempty_partitions: list[ray.ObjectRef] = [
            p
            for p, p_len in zip(listing_details_partitions.values(), listing_details_partitions.len_of_partitions())
            if p_len > 0
        ]
        if len(nonempty_partitions) == 0:
            raise ValueError("No files to get schema from")
        partition: ray.ObjectRef = nonempty_partitions[0]
        return ray.get(
            sample_schema_from_filepath_vpartition.remote(
                partition,
                RayRunnerIO.FS_LISTING_PATH_COLUMN_NAME,
                source_info,
                fs,
            )
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
        return RayPartitionSet(dict(enumerate(daft_vpartitions))), daft_schema

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
        daft_vpartitions, schemas = zip(*map(_make_daft_partition_from_dask_dataframe_partitions.remote, parts))
        schemas = ray.get(list(schemas))
        # Dask shouldn't allow inconsistent schemas across partitions, but we double-check here.
        if not all(schemas[0] == schema for schema in schemas[1:]):
            raise ValueError(
                "Can't convert a Dask DataFrame with inconsistent schemas across partitions to a Daft DataFrame:",
                schemas,
            )
        return RayPartitionSet(dict(enumerate(daft_vpartitions))), schemas[0]


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


def build_partitions(instruction_stack: list[Instruction], *inputs: Table) -> list[list[PartitionMetadata] | Table]:
    partitions = list(inputs)
    for instruction in instruction_stack:
        partitions = instruction.run(partitions)

    metadatas = [PartitionMetadata.from_table(p) for p in partitions]

    return [metadatas, *partitions]


# Give the same function different names to aid in profiling data distribution.


@ray.remote
def single_partition_pipeline(
    instruction_stack: list[Instruction], *inputs: Table
) -> list[list[PartitionMetadata] | Table]:
    return build_partitions(instruction_stack, *inputs)


@ray.remote
def fanout_pipeline(instruction_stack: list[Instruction], *inputs: Table) -> list[list[PartitionMetadata] | Table]:
    return build_partitions(instruction_stack, *inputs)


@ray.remote(scheduling_strategy="SPREAD")
def reduce_pipeline(instruction_stack: list[Instruction], inputs: list) -> list[list[PartitionMetadata] | Table]:
    import ray

    return build_partitions(instruction_stack, *ray.get(inputs))


@ray.remote(scheduling_strategy="SPREAD")
def reduce_and_fanout(instruction_stack: list[Instruction], inputs: list) -> list[list[PartitionMetadata] | Table]:
    import ray

    return build_partitions(instruction_stack, *ray.get(inputs))


@ray.remote
def get_meta(partition: Table) -> PartitionMetadata:
    return PartitionMetadata.from_table(partition)


class Scheduler:
    def __init__(self, max_task_backlog: int | None) -> None:
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

        self.threads_by_df: dict[str, threading.Thread] = dict()
        self.results_by_df: dict[str, Queue] = defaultdict(Queue)

    def next(self, result_uuid: str) -> ray.ObjectRef | StopIteration:

        # Case: thread is terminated and no longer exists.
        # Should only be hit for repeated calls to next() after StopIteration.
        if result_uuid not in self.threads_by_df:
            return StopIteration()

        # Common case: get the next result from the thread.
        result = self.results_by_df[result_uuid].get()

        # If there are no more results, delete the thread.
        if isinstance(result, StopIteration):
            self.threads_by_df[result_uuid].join()
            del self.threads_by_df[result_uuid]

        return result

    def run_plan(
        self,
        plan: logical_plan.LogicalPlan,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
    ) -> None:

        t = threading.Thread(
            target=self._run_plan,
            name=result_uuid,
            kwargs={
                "plan": plan,
                "psets": psets,
                "result_uuid": result_uuid,
            },
        )
        t.start()
        self.threads_by_df[result_uuid] = t

    def _run_plan(
        self,
        plan: logical_plan.LogicalPlan,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
    ) -> None:
        from loguru import logger

        phys_plan = physical_plan_factory.get_materializing_physical_plan(plan, psets)

        # Note: For autoscaling clusters, we will probably want to query cores dynamically.
        # Keep in mind this call takes about 0.3ms.
        cores = int(ray.cluster_resources()["CPU"]) - self.reserved_cores

        max_inflight_tasks = cores + self.max_task_backlog

        inflight_tasks: dict[str, PartitionTask[ray.ObjectRef]] = dict()
        inflight_ref_to_task: dict[ray.ObjectRef, str] = dict()

        start = datetime.now()
        profile_filename = (
            f"profile_RayRunner.run()_"
            f"{datetime.replace(datetime.now(), second=0, microsecond=0).isoformat()[:-3]}.json"
        )
        with profiler(profile_filename):
            try:
                next_step = next(phys_plan)

                while True:  # Loop: Dispatch -> await.
                    while True:  # Loop: Dispatch (get tasks -> batch dispatch).
                        tasks_to_dispatch: list[PartitionTask] = []

                        dispatches_allowed = max_inflight_tasks - len(inflight_tasks)
                        dispatches_allowed = min(cores, dispatches_allowed)

                        # Loop: Get a batch of tasks.
                        while len(tasks_to_dispatch) < dispatches_allowed:
                            if next_step is None:
                                # Blocked on already dispatched tasks; await some tasks.
                                break

                            elif isinstance(next_step, ray.ObjectRef):
                                # A final result.
                                self.results_by_df[result_uuid].put(next_step)
                                next_step = next(phys_plan)

                            # next_step is a task.

                            # If it is a no-op task, just run it locally immediately.
                            elif len(next_step.instructions) == 0:
                                logger.debug(
                                    "Running task synchronously in main thread: {next_step}", next_step=next_step
                                )
                                assert isinstance(next_step, SingleOutputPartitionTask)
                                next_step.set_result(
                                    [RayMaterializedResult(partition) for partition in next_step.inputs]
                                )
                                next_step = next(phys_plan)

                            else:
                                # Add the task to the batch.
                                tasks_to_dispatch.append(next_step)
                                next_step = next(phys_plan)

                        # Dispatch the batch of tasks.
                        logger.debug(
                            f"{(datetime.now() - start).total_seconds()}s: RayRunner dispatching {len(tasks_to_dispatch)} tasks:"
                        )
                        for task in tasks_to_dispatch:
                            results = _build_partitions(task)
                            logger.debug(f"{task} -> {results}")
                            inflight_tasks[task.id()] = task
                            for result in results:
                                inflight_ref_to_task[result] = task.id()

                        if dispatches_allowed == 0 or next_step is None:
                            break

                    # Await a batch of tasks.
                    # (Awaits the next task, and then the next batch of tasks within 10ms.)

                    dispatch = datetime.now()
                    completed_task_ids = []
                    for wait_for in ("next_one", "next_batch"):
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

                                del inflight_tasks[task_id]

                    logger.debug(
                        f"+{(datetime.now() - dispatch).total_seconds()}s to await results from {completed_task_ids}"
                    )

                    if next_step is None:
                        next_step = next(phys_plan)

            except StopIteration as e:
                self.results_by_df[result_uuid].put(e)


@ray.remote(num_cpus=1)
class SchedulerActor(Scheduler):
    def __init__(self, *n, **kw) -> None:
        super().__init__(*n, **kw)
        self.reserved_cores = 1


def _build_partitions(task: PartitionTask[ray.ObjectRef]) -> list[ray.ObjectRef]:
    """Run a PartitionTask and return the resulting list of partitions."""
    ray_options: dict[str, Any] = {
        "num_returns": task.num_results + 1,
    }

    if task.resource_request is not None:
        ray_options = {**ray_options, **_get_ray_task_options(task.resource_request)}

    if isinstance(task.instructions[0], ReduceInstruction):
        build_remote = reduce_and_fanout if isinstance(task.instructions[-1], FanoutInstruction) else reduce_pipeline
        build_remote = build_remote.options(**ray_options)
        [metadatas_ref, *partitions] = build_remote.remote(task.instructions, task.inputs)

    else:
        build_remote = (
            fanout_pipeline if isinstance(task.instructions[-1], FanoutInstruction) else single_partition_pipeline
        )
        build_remote = build_remote.options(**ray_options)
        [metadatas_ref, *partitions] = build_remote.remote(task.instructions, *task.inputs)

    metadatas_accessor = PartitionMetadataAccessor(metadatas_ref)
    task.set_result([RayMaterializedResult(partition, metadatas_accessor, i) for i, partition in enumerate(partitions)])

    return partitions


class RayRunner(Runner[ray.ObjectRef]):
    def __init__(
        self,
        address: str | None,
        max_task_backlog: int | None,
    ) -> None:
        super().__init__()
        if ray.is_initialized():
            logger.warning(f"Ray has already been initialized, Daft will reuse the existing Ray context.")
        self.ray_context = ray.init(address=address, ignore_reinit_error=True)
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

        if isinstance(self.ray_context, ray.client_builder.ClientContext):
            # Run scheduler remotely if the cluster is connected remotely.
            self.scheduler_actor = SchedulerActor.remote(  # type: ignore
                max_task_backlog=max_task_backlog,
            )
        else:
            self.scheduler = Scheduler(
                max_task_backlog=max_task_backlog,
            )

    def run_iter(self, plan: logical_plan.LogicalPlan) -> Iterator[ray.ObjectRef]:
        plan = self.optimize(plan)

        psets = {
            key: entry.value.values()
            for key, entry in self._part_set_cache._uuid_to_partition_set.items()
            if entry.value is not None
        }
        result_uuid = str(uuid.uuid4())
        if isinstance(self.ray_context, ray.client_builder.ClientContext):
            ray.get(
                self.scheduler_actor.run_plan.remote(
                    plan=plan,
                    psets=psets,
                    result_uuid=result_uuid,
                )
            )

        else:
            self.scheduler.run_plan(
                plan=plan,
                psets=psets,
                result_uuid=result_uuid,
            )

        while True:
            if isinstance(self.ray_context, ray.client_builder.ClientContext):
                result = ray.get(self.scheduler_actor.next.remote(result_uuid))
            else:
                result = self.scheduler.next(result_uuid)

            if isinstance(result, StopIteration):
                return
            yield result

    def run_iter_tables(self, plan: logical_plan.LogicalPlan) -> Iterator[Table]:
        for ref in self.run_iter(plan):
            yield ray.get(ref)

    def run(self, plan: logical_plan.LogicalPlan) -> PartitionCacheEntry:
        result_pset = RayPartitionSet({})

        partitions_iter = self.run_iter(plan)

        for i, partition in enumerate(partitions_iter):
            result_pset.set_partition(i, partition)

        pset_entry = self._part_set_cache.put_partition_set(result_pset)

        return pset_entry

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        if isinstance(pset, LocalPartitionSet):
            pset = RayPartitionSet({pid: ray.put(val) for pid, val in pset._partitions.items()})

        return self._part_set_cache.put_partition_set(pset=pset)

    def optimize(self, plan: logical_plan.LogicalPlan) -> logical_plan.LogicalPlan:
        return self._optimizer.optimize(plan)

    def runner_io(self) -> RayRunnerIO:
        return RayRunnerIO()


@dataclass(frozen=True)
class RayMaterializedResult(MaterializedResult[ray.ObjectRef]):
    _partition: ray.ObjectRef
    _metadatas: PartitionMetadataAccessor | None = None
    _metadata_index: int | None = None

    def partition(self) -> ray.ObjectRef:
        return self._partition

    def vpartition(self) -> Table:
        return ray.get(self._partition)

    def metadata(self) -> PartitionMetadata:
        if self._metadatas is not None and self._metadata_index is not None:
            return self._metadatas.get_index(self._metadata_index)
        else:
            return ray.get(get_meta.remote(self._partition))

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
