from __future__ import annotations

import contextlib
import dataclasses
import logging
import os
import threading
import time
import uuid
import warnings
from collections.abc import Generator, Iterable, Iterator
from datetime import datetime
from queue import Full, Queue
from typing import TYPE_CHECKING, Any, Union, cast

# The ray runner is not a top-level module, so we don't need to lazily import pyarrow to minimize
# import times. If this changes, we first need to make the daft.lazy_import.LazyImport class
# serializable before importing pa from daft.dependencies.
import pyarrow as pa  # noqa: TID253
import ray.experimental  # noqa: TID253

from daft.arrow_utils import ensure_array
from daft.context import execution_config_ctx, get_context
from daft.daft import DistributedPhysicalPlan
from daft.daft import PyRecordBatch as _PyRecordBatch
from daft.dependencies import np
from daft.recordbatch import RecordBatch
from daft.runners import ray_tracing
from daft.runners.flotilla import FlotillaRunner
from daft.runners.progress_bar import ProgressBar
from daft.scarf_telemetry import track_runner_on_scarf
from daft.series import Series, item_to_series

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

    import dask
    import dask.dataframe

    import daft


logger = logging.getLogger(__name__)

try:
    import ray
except ImportError:
    logger.error(
        "Error when importing Ray. Please ensure that daft was installed with the Ray extras tag: daft[ray] (https://docs.getdaft.io/en/latest/install)"
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
    ActorPoolProject,
    FanoutInstruction,
    Instruction,
    MultiOutputPartitionTask,
    PartitionTask,
    ReduceInstruction,
    ScanWithTask,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import ActorPoolManager
from daft.expressions import ExpressionsProjection
from daft.filesystem import glob_path_with_stats
from daft.recordbatch import MicroPartition
from daft.runners import runner_io
from daft.runners.partitioning import (
    LocalPartitionSet,
    MaterializedResult,
    PartID,
    PartitionCacheEntry,
    PartitionMetadata,
    PartitionSet,
    PartitionSetCache,
)
from daft.runners.profiler import profiler
from daft.runners.runner import Runner

if TYPE_CHECKING:
    import dask
    import pandas as pd
    from ray.data.block import Block as RayDatasetBlock
    from ray.data.dataset import Dataset as RayDataset

    from daft.logical.builder import LogicalPlanBuilder
    from daft.plan_scheduler import PhysicalPlanScheduler
    from daft.runners.partitioning import PartialPartitionMetadata
    from daft.runners.ray_tracing import RunnerTracer

_RAY_FROM_ARROW_REFS_AVAILABLE = True
try:
    from ray.data import from_arrow_refs
except ImportError:
    _RAY_FROM_ARROW_REFS_AVAILABLE = False

from daft.logical.schema import Schema

RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])

_RAY_DATA_ARROW_TENSOR_TYPE_AVAILABLE = True
try:
    from ray.data.extensions import ArrowTensorArray, ArrowTensorType
except ImportError:
    _RAY_DATA_ARROW_TENSOR_TYPE_AVAILABLE = False

_RAY_DATA_EXTENSIONS_AVAILABLE = True
_TENSOR_EXTENSION_TYPES = []
try:
    import ray
except ImportError:
    _RAY_DATA_EXTENSIONS_AVAILABLE = False
else:
    _RAY_VERSION = tuple(int(s) for s in ray.__version__.split(".")[0:3])
    try:
        # Variable-shaped tensor column support was added in Ray 2.1.0.
        if _RAY_VERSION >= (2, 2, 0):
            from ray.data.extensions import (
                ArrowTensorType,
                ArrowVariableShapedTensorType,
            )

            _TENSOR_EXTENSION_TYPES = [ArrowTensorType, ArrowVariableShapedTensorType]
        else:
            from ray.data.extensions import ArrowTensorType

            _TENSOR_EXTENSION_TYPES = [ArrowTensorType]
    except ImportError:
        _RAY_DATA_EXTENSIONS_AVAILABLE = False


@ray.remote  # type: ignore[misc]
def _glob_path_into_file_infos(
    paths: list[str],
    file_format_config: FileFormatConfig | None,
    io_config: IOConfig | None,
) -> FileInfos:
    file_infos = FileInfos()
    file_format = file_format_config.file_format() if file_format_config is not None else None
    for path in paths:
        path_file_infos = glob_path_with_stats(path, file_format=file_format, io_config=io_config)
        if len(path_file_infos) == 0:
            raise FileNotFoundError(f"No files found at {path}")
        file_infos.extend(path_file_infos)

    return file_infos


@ray.remote  # type: ignore[misc]
def _make_ray_block_from_micropartition(partition: MicroPartition) -> RayDatasetBlock | list[dict[str, Any]]:
    try:
        daft_schema = partition.schema()
        arrow_tbl = partition.to_arrow()

        # Convert arrays to Ray Data's native ArrowTensorType arrays
        new_arrs: dict[int, tuple[str, pa.Array[Any]]] = {}
        for idx, field in enumerate(arrow_tbl.schema):
            if daft_schema[field.name].dtype.is_fixed_shape_tensor():
                assert isinstance(field.type, pa.FixedShapeTensorType)
                new_dtype = ArrowTensorType(tuple(field.type.shape), field.type.value_type)
                arrow_arr = cast("pa.FixedShapeTensorArray", arrow_tbl[field.name].combine_chunks())
                storage_arr = arrow_arr.storage
                list_size = storage_arr.type.list_size
                new_storage_arr = pa.ListArray.from_arrays(
                    pa.array(
                        list(range(0, (len(arrow_arr) + 1) * list_size, list_size)),
                        pa.int32(),
                    ),
                    storage_arr.values,
                )
                new_arrs[idx] = (
                    field.name,
                    pa.ExtensionArray.from_storage(new_dtype, new_storage_arr),
                )
            elif daft_schema[field.name].dtype.is_tensor():
                assert isinstance(field.type, pa.ExtensionType)
                new_arrs[idx] = (
                    field.name,
                    ArrowTensorArray.from_numpy(partition.get_column_by_name(field.name).to_pylist()),
                )
        for idx, (field_name, arr) in new_arrs.items():
            arrow_tbl = arrow_tbl.set_column(idx, pa.field(field_name, arr.type), arr)

        return arrow_tbl
    except pa.ArrowInvalid:
        return partition.to_pylist()


def _series_from_arrow_with_ray_data_extensions(
    array: pa.Array[Any] | pa.ChunkedArray[Any], name: str = "arrow_series"
) -> Series:
    if isinstance(array, pa.Array):
        # TODO(desmond): This might be dead code since `ArrayTensorType`s are `numpy.ndarray` under
        # the hood and are not instances of `pyarrow.Array`. Should follow up and check if this code
        # can be removed.
        array = ensure_array(array)
        if _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(array.type, ArrowTensorType):
            tensor_array = cast("ArrowTensorArray", array)
            storage_series = _series_from_arrow_with_ray_data_extensions(tensor_array.storage, name=name)
            series = storage_series.cast(
                DataType.fixed_size_list(
                    _from_arrow_type_with_ray_data_extensions(tensor_array.type.scalar_type),
                    int(np.prod(array.type.shape)),
                )
            )
            return series.cast(DataType.from_arrow_type(array.type))
        elif _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(array.type, ArrowVariableShapedTensorType):
            return Series.from_numpy(array.to_numpy(zero_copy_only=False), name=name)
    return Series.from_arrow(array, name)


def _micropartition_from_arrow_with_ray_data_extensions(arrow_table: pa.Table) -> MicroPartition:
    assert isinstance(arrow_table, pa.Table)
    non_native_fields = []
    for arrow_field in arrow_table.schema:
        dt = _from_arrow_type_with_ray_data_extensions(arrow_field.type)
        if dt == DataType.python() or dt.is_tensor() or dt.is_fixed_shape_tensor():
            non_native_fields.append(arrow_field.name)
    if non_native_fields:
        # If there are any contained Arrow types that are not natively supported, convert each
        # series while checking for ray data extension types.
        logger.debug("Unsupported Arrow types detected for columns: %s", non_native_fields)
        series_dict = dict()
        for name, column in zip(arrow_table.column_names, arrow_table.columns):
            series = (
                _series_from_arrow_with_ray_data_extensions(column, name)
                if isinstance(column, (pa.Array, pa.ChunkedArray))
                else item_to_series(name, column)
            )
            series_dict[name] = series._series
        return MicroPartition._from_record_batches(
            [RecordBatch._from_pyrecordbatch(_PyRecordBatch.from_pylist_series(series_dict))]
        )
    return MicroPartition.from_arrow(arrow_table)


@ray.remote  # type: ignore[misc]
def _make_daft_partition_from_ray_dataset_blocks(ray_dataset_block: pa.Table, daft_schema: Schema) -> MicroPartition:
    return _micropartition_from_arrow_with_ray_data_extensions(ray_dataset_block)


@ray.remote(num_returns=2)  # type: ignore[misc]
def _make_daft_partition_from_dask_dataframe_partitions(
    dask_df_partition: pd.DataFrame,
) -> tuple[MicroPartition, daft.Schema]:
    vpart = MicroPartition.from_pandas(dask_df_partition)
    return vpart, vpart.schema()


def _to_pandas_ref(df: pd.DataFrame | ray.ObjectRef) -> ray.ObjectRef:
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

    def _get_merged_micropartition(self, schema: Schema) -> MicroPartition:
        ids_and_partitions = self.items()
        if len(ids_and_partitions) > 0:
            assert ids_and_partitions[0][0] == 0
            assert ids_and_partitions[-1][0] + 1 == len(ids_and_partitions)
        all_partitions = ray.get([part.partition() for id, part in ids_and_partitions])
        return MicroPartition.concat_or_empty(all_partitions, schema)

    def _get_preview_micropartitions(self, num_rows: int) -> list[MicroPartition]:
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

        blocks = [
            _make_ray_block_from_micropartition.remote(self._results[k].partition()) for k in self._results.keys()
        ]
        # NOTE: although the Ray method is called `from_arrow_refs`, this method works also when the blocks are List[T] types
        # instead of Arrow tables as the codepath for Dataset creation is the same.
        return from_arrow_refs(blocks)

    def to_dask_dataframe(
        self,
        meta: (pd.DataFrame | pd.Series[Any] | dict[str, Any] | Iterable[Any] | tuple[Any] | None) = None,
    ) -> dask.dataframe.DataFrame:
        import dask
        import dask.dataframe as dd
        from ray.util.dask import ray_dask_get

        dask.config.set(scheduler=ray_dask_get)

        @dask.delayed  # type: ignore[misc]
        def _make_dask_dataframe_partition_from_micropartition(partition: MicroPartition) -> pd.DataFrame:
            return partition.to_pandas()

        ddf_parts = [
            _make_dask_dataframe_partition_from_micropartition(self._results[k].partition())
            for k in self._results.keys()
        ]
        return cast("dd.DataFrame", dd.from_delayed(ddf_parts, meta=meta))

    def get_partition(self, idx: PartID) -> RayMaterializedResult:
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
        ray.wait(list(deduped_object_refs), fetch_local=False, num_returns=len(deduped_object_refs))


def _from_arrow_type_with_ray_data_extensions(arrow_type: pa.DataType) -> DataType:
    if _RAY_DATA_EXTENSIONS_AVAILABLE and isinstance(arrow_type, tuple(_TENSOR_EXTENSION_TYPES)):
        tensor_types = cast("Union[ArrowTensorType, ArrowVariableShapedTensorType]", arrow_type)
        scalar_dtype = _from_arrow_type_with_ray_data_extensions(tensor_types.scalar_type)
        shape = tensor_types.shape if isinstance(tensor_types, ArrowTensorType) else None
        return DataType.tensor(scalar_dtype, shape)
    return DataType.from_arrow_type(arrow_type)


class RayRunnerIO(runner_io.RunnerIO):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> FileInfos:
        # Synchronously fetch the file infos, for now.
        return ray.get(_glob_path_into_file_infos.remote(source_paths, file_format_config, io_config=io_config))

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
            [
                (arrow_field.name, _from_arrow_type_with_ray_data_extensions(arrow_field.type))
                for arrow_field in arrow_schema
            ]
        )
        block_refs = ds.get_internal_block_refs()

        # NOTE: This materializes the entire Ray Dataset - we could make this more intelligent by creating a new RayDatasetScan node
        # which can iterate on Ray Dataset blocks and materialize as-needed
        daft_micropartitions = [
            _make_daft_partition_from_ray_dataset_blocks.remote(block, daft_schema) for block in block_refs
        ]
        pset = RayPartitionSet()

        for i, obj in enumerate(daft_micropartitions):
            pset.set_partition(i, RayMaterializedResult(obj))
        return (
            pset,
            daft_schema,
        )

    def partition_set_from_dask_dataframe(
        self,
        ddf: dask.dataframe.DataFrame,
    ) -> tuple[RayPartitionSet, Schema]:
        from dask.base import persist
        from ray.util.dask import ray_dask_get

        partitions = ddf.to_delayed()
        if not partitions:
            raise ValueError("Can't convert an empty Dask DataFrame (with no partitions) to a Daft DataFrame.")
        persisted_partitions = persist(*partitions, scheduler=ray_dask_get)
        parts = [_to_pandas_ref(next(iter(part.dask.values()))) for part in persisted_partitions]
        daft_micropartitions, schemas = zip(
            *(_make_daft_partition_from_dask_dataframe_partitions.remote(p) for p in parts)
        )
        schemas = ray.get(list(schemas))
        # Dask shouldn't allow inconsistent schemas across partitions, but we double-check here.
        if not all(schemas[0] == schema for schema in schemas[1:]):
            raise ValueError(
                "Can't convert a Dask DataFrame with inconsistent schemas across partitions to a Daft DataFrame:",
                schemas,
            )

        pset = RayPartitionSet()

        for i, obj in enumerate(daft_micropartitions):
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
        execution_config = get_context().daft_execution_config
        min_cpus = execution_config.min_cpu_per_task
        options["num_cpus"] = max(min_cpus, resource_request.num_cpus)
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


@dataclasses.dataclass(frozen=True)
class PartitionTaskContext:
    job_id: str
    task_id: str
    stage_id: int


# Give the same function different names to aid in profiling data distribution.


@ray_tracing.ray_remote_traced
@ray.remote  # type: ignore[misc]
def single_partition_pipeline(
    task_context: PartitionTaskContext,
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    *inputs: MicroPartition,
) -> list[list[PartitionMetadata] | MicroPartition]:
    with (
        execution_config_ctx(
            config=daft_execution_config,
        ),
        ray_tracing.collect_ray_task_metrics(
            task_context.job_id, task_context.task_id, task_context.stage_id, daft_execution_config
        ),
    ):
        return build_partitions(instruction_stack, partial_metadatas, *inputs)


@ray_tracing.ray_remote_traced
@ray.remote  # type: ignore[misc]
def fanout_pipeline(
    task_context: PartitionTaskContext,
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    *inputs: MicroPartition,
) -> list[list[PartitionMetadata] | MicroPartition]:
    with (
        execution_config_ctx(config=daft_execution_config),
        ray_tracing.collect_ray_task_metrics(
            task_context.job_id, task_context.task_id, task_context.stage_id, daft_execution_config
        ),
    ):
        return build_partitions(instruction_stack, partial_metadatas, *inputs)


@ray_tracing.ray_remote_traced
@ray.remote  # type: ignore[misc]
def reduce_pipeline(
    task_context: PartitionTaskContext,
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    inputs: list[ray.ObjectRef],
) -> list[list[PartitionMetadata] | MicroPartition]:
    import ray

    with (
        execution_config_ctx(config=daft_execution_config),
        ray_tracing.collect_ray_task_metrics(
            task_context.job_id, task_context.task_id, task_context.stage_id, daft_execution_config
        ),
    ):
        return build_partitions(instruction_stack, partial_metadatas, *ray.get(inputs))


@ray_tracing.ray_remote_traced
@ray.remote  # type: ignore[misc]
def reduce_and_fanout(
    task_context: PartitionTaskContext,
    daft_execution_config: PyDaftExecutionConfig,
    instruction_stack: list[Instruction],
    partial_metadatas: list[PartitionMetadata],
    inputs: list[ray.ObjectRef],
) -> list[list[PartitionMetadata] | MicroPartition]:
    import ray

    with (
        execution_config_ctx(config=daft_execution_config),
        ray_tracing.collect_ray_task_metrics(
            task_context.job_id, task_context.task_id, task_context.stage_id, daft_execution_config
        ),
    ):
        return build_partitions(instruction_stack, partial_metadatas, *ray.get(inputs))


@ray.remote  # type: ignore[misc]
def get_metas(*partitions: MicroPartition) -> list[PartitionMetadata]:
    return [PartitionMetadata.from_table(partition) for partition in partitions]


def _ray_num_cpus_provider(ttl_seconds: int = 1) -> Generator[int, None, None]:
    """Helper that gets the number of CPUs from Ray.

    Used as a generator as it provides a guard against calling ray.cluster_resources()
    more than once per `ttl_seconds`.

    Examples:
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


class Scheduler(ActorPoolManager):
    def __init__(self, max_task_backlog: int | None, use_ray_tqdm: bool) -> None:
        """max_task_backlog: Max number of inflight tasks waiting for cores."""
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
        self.results_by_df: dict[str, Queue[RayMaterializedResult | StopIteration | Exception]] = {}
        self.active_by_df: dict[str, bool] = dict()
        self.results_buffer_size_by_df: dict[str, int | None] = dict()

        self._actor_pools: dict[str, RayRoundRobinActorPool] = {}

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

        return result  # type: ignore[return-value]

    def start_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        psets: dict[str, MaterializedResult[ray.ObjectRef]],
        result_uuid: str,
        daft_execution_config: PyDaftExecutionConfig,
        results_buffer_size: int | None = None,
    ) -> None:
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
                "daft_execution_config": daft_execution_config,
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

    def get_actor_pool(
        self,
        name: str,
        resource_request: ResourceRequest,
        num_actors: int,
        projection: ExpressionsProjection,
        execution_config: PyDaftExecutionConfig,
    ) -> str:
        actor_pool = RayRoundRobinActorPool(name, num_actors, resource_request, projection, execution_config)
        self._actor_pools[name] = actor_pool
        self._actor_pools[name].setup()
        return name

    def teardown_actor_pool(self, name: str) -> None:
        if name in self._actor_pools:
            self._actor_pools[name].teardown()
            del self._actor_pools[name]

    def _construct_dispatch_batch(
        self,
        execution_id: str,
        tasks: ray_tracing.MaterializedPhysicalPlanWrapper,
        dispatches_allowed: int,
        runner_tracer: RunnerTracer,
    ) -> tuple[list[PartitionTask[ray.ObjectRef]], bool]:
        """Constructs a batch of PartitionTasks that should be dispatched.

        Args:
            execution_id: The ID of the current execution.
            tasks: The iterator over the physical plan.
            dispatches_allowed (int): The maximum number of tasks that can be dispatched in this batch.
            runner_tracer: The tracer to capture information about the dispatch batching process
        Returns:

            tuple[list[PartitionTask], bool]: A tuple containing:
                - A list of PartitionTasks to be dispatched.
                - A pagination boolean indicating whether or not there are more tasks to be had by calling _construct_dispatch_batch again
        """
        with runner_tracer.dispatch_batching():
            tasks_to_dispatch: list[PartitionTask[ray.ObjectRef]] = []

            # Loop until:
            # - Reached the limit of the number of tasks we are allowed to dispatch
            # - Encounter a `None` as the next step (short-circuit and return has_next=False)
            while len(tasks_to_dispatch) < dispatches_allowed and self._is_active(execution_id):
                next_step = next(tasks)

                # CASE: Blocked on already dispatched tasks
                # Early terminate and mark has_next=False
                if next_step is None:
                    return tasks_to_dispatch, False

                # CASE: A final result
                # Place it in the result queue (potentially block on space to be available)
                elif isinstance(next_step, MaterializedResult):
                    self._place_in_queue(execution_id, cast("RayMaterializedResult", next_step))

                # CASE: No-op task
                # Just run it locally immediately.
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
                    next_step.set_done()

                # CASE: Actual task that needs to be dispatched
                else:
                    tasks_to_dispatch.append(next_step)

            return tasks_to_dispatch, True

    def _dispatch_tasks(
        self,
        execution_id: str,
        tasks_to_dispatch: list[PartitionTask[ray.ObjectRef]],
        daft_execution_config_objref: ray.ObjectRef,
        runner_tracer: RunnerTracer,
    ) -> Iterator[tuple[PartitionTask[ray.ObjectRef], list[ray.ObjectRef]]]:
        """Iteratively Dispatches a batch of tasks to the Ray backend."""
        with runner_tracer.dispatching():
            for task in tasks_to_dispatch:
                if task.actor_pool_id is None:
                    results = _build_partitions(execution_id, daft_execution_config_objref, task, runner_tracer)
                else:
                    actor_pool = self._actor_pools.get(task.actor_pool_id)
                    assert actor_pool is not None, "Ray actor pool must live for as long as the tasks."
                    # TODO: Add tracing for submissions to actor pool
                    results = _build_partitions_on_actor_pool(task, actor_pool)
                logger.debug("%s -> %s", task, results)

                yield task, results

    def _await_tasks(
        self,
        inflight_ref_to_task_id: dict[ray.ObjectRef, str],
        inflight_tasks: dict[str, PartitionTask[ray.ObjectRef]],
        runner_tracer: RunnerTracer,
    ) -> list[ray.ObjectRef]:
        """Awaits for tasks to be completed. Returns tasks that are ready.

        NOTE: This method blocks until at least 1 task is ready. Then it will return as many ready tasks as it can.
        """
        if len(inflight_ref_to_task_id) == 0:
            return []

        # Await on (any) task to be ready with an unlimited timeout
        with runner_tracer.awaiting(1, None):
            ray.wait(
                list(inflight_ref_to_task_id.keys()),
                num_returns=1,
                timeout=None,
                fetch_local=False,
            )

        # Now, grab as many ready tasks as possible with a 0.01s timeout
        timeout = 0.01
        num_returns = len(inflight_ref_to_task_id)
        with runner_tracer.awaiting(num_returns, timeout):
            readies, _ = ray.wait(
                list(inflight_ref_to_task_id.keys()),
                num_returns=num_returns,
                timeout=timeout,
                fetch_local=False,
            )

        # Update traces
        for ready in readies:
            if ready in inflight_ref_to_task_id:
                task_id = inflight_ref_to_task_id[ready]
                runner_tracer.task_received_as_ready(task_id, inflight_tasks[task_id].stage_id)

        return readies

    def _is_active(self, execution_id: str) -> bool:
        """Checks if the execution for the provided `execution_id` is still active."""
        return self.active_by_df.get(execution_id, False)

    def _place_in_queue(self, execution_id: str, item: RayMaterializedResult | StopIteration | Exception) -> None:
        """Places a result into the queue for the provided `execution_id.

        NOTE: This will block and poll busily until space is available on the queue
        `
        """
        while self._is_active(execution_id):
            try:
                self.results_by_df[execution_id].put(item, timeout=0.1)
                break
            except Full:
                pass

    def _run_plan(
        self,
        plan_scheduler: PhysicalPlanScheduler,
        psets: dict[str, ray.ObjectRef],
        result_uuid: str,
        daft_execution_config: PyDaftExecutionConfig,
    ) -> None:
        # Put execution config into cluster once to share it amongst all tasks
        daft_execution_config_objref = ray.put(daft_execution_config)

        # Get executable tasks from plan scheduler.
        results_buffer_size = self.results_buffer_size_by_df[result_uuid]

        inflight_tasks: dict[str, PartitionTask[ray.ObjectRef]] = dict()
        inflight_ref_to_task: dict[ray.ObjectRef, str] = dict()
        pbar = ProgressBar(use_ray_tqdm=self.use_ray_tqdm)
        num_cpus_provider = _ray_num_cpus_provider()

        start = datetime.now()
        profile_filename = (
            f"profile_RayRunner.run()_"
            f"{datetime.replace(datetime.now(), second=0, microsecond=0).isoformat()[:-3]}.json"
        )

        with profiler(profile_filename), ray_tracing.ray_tracer(result_uuid, daft_execution_config) as runner_tracer:
            try:
                raw_tasks = plan_scheduler.to_partition_tasks(
                    psets,
                    self,
                    # Attempt to subtract 1 from results_buffer_size because the return Queue size is already 1
                    # If results_buffer_size=1 though, we can't do much and the total buffer size actually has to be >= 2
                    # because we have two buffers (the Queue and the buffer inside the `materialize` generator)
                    None if results_buffer_size is None else max(results_buffer_size - 1, 1),
                )
                tasks = ray_tracing.MaterializedPhysicalPlanWrapper(raw_tasks, runner_tracer)
                ###
                # Scheduling Loop:
                #
                #    DispatchBatching ─► Dispatch
                #    ▲                        │      ───────►  Await
                #    └────────────────────────┘                  │
                #                ▲                               │
                #                └───────────────────────────────┘
                ###
                wave_count = 0
                while self._is_active(result_uuid):
                    ###
                    # Dispatch Loop:
                    #
                    #    DispatchBatching ─► Dispatch
                    #    ▲                        │
                    #    └────────────────────────┘
                    ###
                    wave_count += 1
                    with runner_tracer.dispatch_wave(wave_count):
                        while self._is_active(result_uuid):
                            # Update available cluster resources
                            # TODO: improve control loop code to be more understandable and dynamically adjust backlog
                            cores: int = max(
                                next(num_cpus_provider) - self.reserved_cores, 1
                            )  # assume at least 1 CPU core for bootstrapping clusters that scale from zero
                            max_inflight_tasks = cores + self.max_task_backlog
                            dispatches_allowed = max_inflight_tasks - len(inflight_tasks)
                            dispatches_allowed = min(cores, dispatches_allowed)

                            # Dispatch Batching
                            tasks_to_dispatch, has_next = self._construct_dispatch_batch(
                                result_uuid,
                                tasks,
                                dispatches_allowed,
                                runner_tracer,
                            )

                            logger.debug(
                                "%ss: RayRunner dispatching %s tasks",
                                (datetime.now() - start).total_seconds(),
                                len(tasks_to_dispatch),
                            )

                            if not self._is_active(result_uuid):
                                break

                            # Dispatch
                            for task, result_obj_refs in self._dispatch_tasks(
                                result_uuid,
                                tasks_to_dispatch,
                                daft_execution_config_objref,
                                runner_tracer,
                            ):
                                inflight_tasks[task.id()] = task
                                for result in result_obj_refs:
                                    inflight_ref_to_task[result] = task.id()

                                pbar.make_bar_or_update_total(task.stage_id, task.name())

                            # Break the dispatch batching/dispatch loop if no more dispatches allowed, or physical plan
                            # needs work for forward progress
                            if dispatches_allowed == 0 or not has_next:
                                break

                        ###
                        # Await:
                        # Wait for some work to be completed from the current wave's dispatch
                        # Then we perform the necessary record-keeping on tasks that were retrieved as ready.
                        ###
                        readies = self._await_tasks(
                            inflight_ref_to_task,
                            inflight_tasks,
                            runner_tracer,
                        )
                        for ready in readies:
                            if ready in inflight_ref_to_task:
                                task_id = inflight_ref_to_task[ready]

                                # Mark the entire task associated with the result as done.
                                task = inflight_tasks[task_id]
                                task.set_done()

                                if isinstance(task, SingleOutputPartitionTask):
                                    del inflight_ref_to_task[ready]
                                elif isinstance(task, MultiOutputPartitionTask):
                                    for partition in task.partitions():
                                        del inflight_ref_to_task[partition]

                                pbar.update_bar(task.stage_id)
                                del inflight_tasks[task_id]

            except StopIteration as e:
                self._place_in_queue(result_uuid, e)

            # Ensure that all Exceptions are correctly propagated to the consumer before reraising to kill thread
            except Exception as e:
                self._place_in_queue(result_uuid, e)
                pbar.close()
                raise

        pbar.close()

    @contextlib.contextmanager
    def actor_pool_context(
        self,
        name: str,
        actor_resource_request: ResourceRequest,
        task_resource_request: ResourceRequest,
        num_actors: PartID,
        projection: ExpressionsProjection,
    ) -> Iterator[str]:
        # Ray runs actor methods serially, so the resource request for an actor should be both the actor's resources and the task's resources
        resource_request = actor_resource_request + task_resource_request

        execution_config = get_context().daft_execution_config
        try:
            yield self.get_actor_pool(name, resource_request, num_actors, projection, execution_config)
        finally:
            self.teardown_actor_pool(name)


SCHEDULER_ACTOR_NAME = "scheduler"
SCHEDULER_ACTOR_NAMESPACE = "daft"


@ray.remote(num_cpus=1)
class SchedulerActor(Scheduler):
    def __init__(self, *n: Any, **kw: Any) -> None:
        super().__init__(*n, **kw)
        self.reserved_cores = 1


def _build_partitions(
    job_id: str,
    daft_execution_config_objref: ray.ObjectRef,
    task: PartitionTask[ray.ObjectRef],
    runner_tracer: RunnerTracer,
) -> list[ray.ObjectRef]:
    """Run a PartitionTask and return the resulting list of partitions."""
    ray_options: dict[str, Any] = {"num_returns": task.num_results + 1, "name": task.name()}

    if task.resource_request is not None:
        ray_options = {**ray_options, **_get_ray_task_options(task.resource_request)}

    metadatas_ref: ray.ObjectRef
    partitions: list[list[PartitionMetadata] | MicroPartition]

    if isinstance(task.instructions[0], ReduceInstruction):
        build_remote = (
            reduce_and_fanout
            if task.instructions and isinstance(task.instructions[-1], FanoutInstruction)
            else reduce_pipeline
        )
        if task.node_id is not None:
            ray_options["scheduling_strategy"] = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                task.node_id, soft=True
            )
        else:
            ray_options["scheduling_strategy"] = "SPREAD"
        build_remote_runner = build_remote.options(**ray_options).with_tracing(runner_tracer, task)
        [metadatas_ref, *partitions] = build_remote_runner.remote(
            PartitionTaskContext(job_id=job_id, task_id=task.id(), stage_id=task.stage_id),
            daft_execution_config_objref,
            task.instructions,
            task.partial_metadatas,
            task.inputs,
        )

    else:
        build_remote = (
            fanout_pipeline
            if task.instructions and isinstance(task.instructions[-1], FanoutInstruction)
            else single_partition_pipeline
        )
        if task.instructions and isinstance(task.instructions[0], ScanWithTask):
            ray_options["scheduling_strategy"] = "SPREAD"
        build_remote_runner = build_remote.options(**ray_options).with_tracing(runner_tracer, task)
        [metadatas_ref, *partitions] = build_remote_runner.remote(
            PartitionTaskContext(job_id=job_id, task_id=task.id(), stage_id=task.stage_id),
            daft_execution_config_objref,
            task.instructions,
            task.partial_metadatas,
            *task.inputs,
        )

    task.inputs.clear()
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


def _build_partitions_on_actor_pool(
    task: PartitionTask[ray.ObjectRef],
    actor_pool: RayRoundRobinActorPool,
) -> list[ray.ObjectRef]:
    """Run a PartitionTask on an actor pool and return the resulting list of partitions."""
    assert len(task.instructions) == 1, "Actor pool can only handle single ActorPoolProject instructions"
    assert isinstance(task.instructions[0], ActorPoolProject)

    [metadatas_ref, *partitions] = actor_pool.submit(task.partial_metadatas, task.inputs)
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


@ray.remote
class DaftRayActor:
    def __init__(
        self, daft_execution_config: PyDaftExecutionConfig, uninitialized_projection: ExpressionsProjection
    ) -> None:
        from daft.daft import get_udf_names

        self.daft_execution_config = daft_execution_config

        logger.info(
            "Initializing stateful UDFs: %s",
            ", ".join(name for expr in uninitialized_projection for name in get_udf_names(expr._expr)),
        )
        self.initialized_projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    @ray.method(num_returns=2)  # type: ignore[misc]
    def run(
        self,
        partial_metadatas: list[PartitionMetadata],
        *inputs: MicroPartition,
    ) -> list[list[PartitionMetadata] | MicroPartition]:
        with execution_config_ctx(config=self.daft_execution_config):
            assert len(inputs) == 1, "DaftRayActor can only process single partitions"
            assert len(partial_metadatas) == 1, "DaftRayActor can only process single partitions (and single metadata)"
            part = inputs[0]
            partial = partial_metadatas[0]

            new_part = part.eval_expression_list(self.initialized_projection)

            return [
                [PartitionMetadata.from_table(new_part).merge_with_partial(partial)],
                new_part,
            ]


class RayRoundRobinActorPool:
    """Naive implementation of an ActorPool that performs round-robin task submission to the actors."""

    def __init__(
        self,
        pool_id: str,
        num_actors: int,
        resource_request: ResourceRequest,
        projection: ExpressionsProjection,
        execution_config: PyDaftExecutionConfig,
    ) -> None:
        self._actors: list[DaftRayActor] | None = None
        self._task_idx = 0

        self._execution_config = execution_config
        self._num_actors = num_actors
        self._resource_request_per_actor = resource_request
        self._id = pool_id
        self._projection = projection

    def setup(self) -> None:
        ray_options = _get_ray_task_options(self._resource_request_per_actor)

        self._actors = [
            DaftRayActor.options(name=f"rank={rank}-{self._id}", **ray_options).remote(  # type: ignore
                self._execution_config, self._projection
            )
            for rank in range(self._num_actors)
        ]

    def teardown(self) -> None:
        assert self._actors is not None, "Must have active Ray actors on teardown"

        # Delete the actors in the old pool so Ray can tear them down
        old_actors = self._actors
        self._actors = None
        del old_actors

    def submit(
        self, partial_metadatas: list[PartialPartitionMetadata], inputs: list[ray.ObjectRef]
    ) -> list[ray.ObjectRef]:
        assert self._actors is not None, "Must have active Ray actors during submission"

        # Determine which actor to schedule on in a round-robin fashion
        idx = self._task_idx % self._num_actors
        self._task_idx += 1
        actor = self._actors[idx]

        return actor.run.remote(partial_metadatas, *inputs)


class RayRunner(Runner[ray.ObjectRef]):
    name = "ray"

    def __init__(
        self,
        address: str | None,
        max_task_backlog: int | None,
        force_client_mode: bool = False,
    ) -> None:
        super().__init__()

        self.ray_address = address

        if ray.is_initialized():
            if address is not None:
                logger.warning(
                    "Ray has already been initialized, Daft will reuse the existing Ray context and ignore the "
                    "supplied address: %s",
                    address,
                )
        else:
            if address is not None:
                if not address.endswith("10001"):
                    warnings.warn(
                        f"The address to a Ray client server is typically at port :10001, but instead we found: {address}"
                    )
                if not address.startswith("ray://"):
                    warnings.warn(
                        f"Expected Ray address to start with 'ray://' protocol but found: {address}. Automatically prefixing your address with the protocol to make a Ray connection: ray://{address}"
                    )
                    address = "ray://" + address
            ray.init(address=address)

        # Check if Ray is running in "client mode" (connected to a Ray cluster via a Ray client)
        self.ray_client_mode: bool = force_client_mode or ray.util.client.ray.get_context().is_connected()

        if self.ray_client_mode:
            # Run scheduler remotely if the cluster is connected remotely.
            self.scheduler_actor = SchedulerActor.options(  # type: ignore
                name=SCHEDULER_ACTOR_NAME,
                namespace=SCHEDULER_ACTOR_NAMESPACE,
                get_if_exists=True,
            ).remote(
                max_task_backlog=max_task_backlog,
                use_ray_tqdm=True,
            )
        else:
            self.scheduler = Scheduler(
                max_task_backlog=max_task_backlog,
                use_ray_tqdm=False,
            )
        self.flotilla_plan_runner: FlotillaRunner | None = None

    def initialize_partition_set_cache(self) -> PartitionSetCache:
        return PartitionSetCache()

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
                psets=psets,  # type: ignore[arg-type]
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
        track_runner_on_scarf(runner=self.name)

        # Grab and freeze the current DaftExecutionConfig
        daft_execution_config = get_context().daft_execution_config

        # Optimize the logical plan.
        builder = builder.optimize()

        if daft_execution_config.enable_aqe:
            adaptive_planner = builder.to_adaptive_physical_plan_scheduler(daft_execution_config)
            while not adaptive_planner.is_done():
                stage_id, plan_scheduler = adaptive_planner.next()
                start_time = time.time()
                # don't store partition sets in variable to avoid reference
                result_uuid = self._start_plan(
                    plan_scheduler, daft_execution_config, results_buffer_size=results_buffer_size
                )
                del plan_scheduler
                results_iter = self._stream_plan(result_uuid)
                # if stage_id is None that means this is the final stage
                if stage_id is None:
                    num_rows_processed = 0
                    bytes_processed = 0

                    for result in results_iter:
                        num_rows_processed += result.metadata().num_rows
                        size_bytes = result.metadata().size_bytes
                        if size_bytes is not None:
                            bytes_processed += size_bytes
                        yield result
                    adaptive_planner.update_stats(
                        time.time() - start_time, bytes_processed, num_rows_processed, stage_id
                    )
                else:
                    cache_entry = self._collect_into_cache(results_iter)
                    adaptive_planner.update_stats(
                        time.time() - start_time, cache_entry.size_bytes(), cache_entry.num_rows(), stage_id
                    )
                    adaptive_planner.update(stage_id, cache_entry)
                    del cache_entry

            enable_explain_analyze = os.getenv("DAFT_DEV_ENABLE_EXPLAIN_ANALYZE")
            ray_logs_location = ray_tracing.get_log_location()
            should_explain_analyze = (
                ray_logs_location.exists()
                and enable_explain_analyze is not None
                and enable_explain_analyze in ["1", "true"]
            )
            if should_explain_analyze:
                explain_analyze_dir = ray_tracing.get_daft_trace_location(ray_logs_location)
                explain_analyze_dir.mkdir(exist_ok=True, parents=True)
                adaptive_planner.explain_analyze(str(explain_analyze_dir))
        elif daft_execution_config.use_experimental_distributed_engine:
            try:
                distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(
                    builder._builder, daft_execution_config
                )
            except Exception as e:
                logger.error("Failed to build distributed plan, falling back to regular execution. Error: %s", str(e))
                # Fallback to regular execution
                yield from self._execute_plan(builder, daft_execution_config, results_buffer_size)
            else:
                if self.flotilla_plan_runner is None:
                    self.flotilla_plan_runner = FlotillaRunner(self.ray_client_mode)
                yield from self.flotilla_plan_runner.stream_plan(
                    distributed_plan, self._part_set_cache.get_all_partition_sets()
                )

        else:
            yield from self._execute_plan(builder, daft_execution_config, results_buffer_size)

    def _execute_plan(
        self, builder: LogicalPlanBuilder, daft_execution_config: PyDaftExecutionConfig, results_buffer_size: int | None
    ) -> Iterator[RayMaterializedResult]:
        # Finalize the logical plan and get a physical plan scheduler for translating the
        # physical plan to executable tasks.
        plan_scheduler = builder.to_physical_plan_scheduler(daft_execution_config)
        result_uuid = self._start_plan(plan_scheduler, daft_execution_config, results_buffer_size=results_buffer_size)
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

    def put_partition_set_into_cache(self, pset: PartitionSet[ray.ObjectRef]) -> PartitionCacheEntry:
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
        partition: ray.ObjectRef[Any],
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

    def micropartition(self) -> MicroPartition:
        return ray.get(self._partition)

    def metadata(self) -> PartitionMetadata:
        assert self._metadata_idx is not None
        return self._metadatas.get_index(self._metadata_idx)

    def cancel(self) -> None:
        return ray.cancel(self._partition)

    def _noop(self, _: ray.ObjectRef) -> None:
        return None


class PartitionMetadataAccessor:
    """Wrapper class around Remote[List[PartitionMetadata]] to memoize lookups."""

    def __init__(self, ref: ray.ObjectRef) -> None:
        self._ref = ref
        self._metadatas: None | list[PartitionMetadata] = None

    def _get_metadatas(self) -> list[PartitionMetadata]:
        if self._metadatas is None:
            self._metadatas = ray.get(self._ref)
        return self._metadatas

    def get_index(self, key: int) -> PartitionMetadata:
        return self._get_metadatas()[key]

    @classmethod
    def from_metadata_list(cls, meta: list[PartitionMetadata]) -> PartitionMetadataAccessor:
        ref = ray.put(meta)
        accessor = cls(ref)
        accessor._metadatas = meta
        return accessor
