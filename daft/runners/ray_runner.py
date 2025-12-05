from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Generator, Iterable, Iterator
from typing import TYPE_CHECKING, Any, Union, cast

# The ray runner is not a top-level module, so we don't need to lazily import pyarrow to minimize
# import times. If this changes, we first need to make the daft.lazy_import.LazyImport class
# serializable before importing pa from daft.dependencies.
import pyarrow as pa  # noqa: TID253
import ray.experimental  # noqa: TID253

from daft.arrow_utils import ensure_array
from daft.context import get_context
from daft.daft import DistributedPhysicalPlan
from daft.daft import PyRecordBatch as _PyRecordBatch
from daft.dependencies import np
from daft.recordbatch import RecordBatch
from daft.runners.flotilla import FlotillaRunner
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
        "Error when importing Ray. Please ensure that daft was installed with the Ray extras tag: daft[ray] (https://docs.daft.ai/en/latest/install)"
    )
    raise

from daft.daft import (
    FileFormatConfig,
    FileInfos,
    IOConfig,
)
from daft.datatype import DataType
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
from daft.runners.runner import Runner

if TYPE_CHECKING:
    import dask
    import pandas as pd
    from ray.data.block import Block as RayDatasetBlock
    from ray.data.dataset import Dataset as RayDataset

    from daft.logical.builder import LogicalPlanBuilder

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

            # ArrowTensorTypeV2 was introduced in later Ray versions
            try:
                from ray.data.extensions import ArrowTensorTypeV2

                _TENSOR_EXTENSION_TYPES = [ArrowTensorType, ArrowTensorTypeV2, ArrowVariableShapedTensorType]
            except ImportError:
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
    for path in set(paths):
        try:
            path_file_infos = glob_path_with_stats(path, file_format=file_format, io_config=io_config)
            file_infos.merge(path_file_infos)
        except FileNotFoundError:
            logger.debug("%s is not found.", path)

    if len(file_infos) == 0:
        raise FileNotFoundError(f"No files found at {','.join(paths)}")

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
    # Handle ChunkedArrays by checking the type of the first chunk
    if isinstance(array, pa.ChunkedArray) and len(array.chunks) > 0:
        array_type = array.chunks[0].type
    elif isinstance(array, pa.Array):
        array_type = array.type
    else:
        array_type = None

    if (
        _RAY_DATA_EXTENSIONS_AVAILABLE
        and array_type is not None
        and isinstance(array_type, tuple(_TENSOR_EXTENSION_TYPES))
    ):
        # Check if it's ArrowTensorTypeV2 (imported if available)
        try:
            from ray.data.extensions import ArrowTensorTypeV2

            is_arrow_tensor_v2 = isinstance(array_type, ArrowTensorTypeV2)
        except ImportError:
            is_arrow_tensor_v2 = False

        # ArrowTensorTypeV2 and ArrowVariableShapedTensorType should be converted via numpy
        if isinstance(array_type, ArrowVariableShapedTensorType) or is_arrow_tensor_v2:
            if isinstance(array, pa.ChunkedArray):
                # Convert each chunk and concatenate
                numpy_arrays = [chunk.to_numpy(zero_copy_only=False) for chunk in array.chunks]
                numpy_data = np.concatenate(numpy_arrays, axis=0)
            else:
                numpy_data = array.to_numpy(zero_copy_only=False)
            return Series.from_numpy(numpy_data, name=name)
        elif isinstance(array, pa.Array):
            # Handle ArrowTensorType (has storage attribute)
            array = ensure_array(array)
            if hasattr(array.type, "shape") and array.type.shape is not None and hasattr(array, "storage"):
                tensor_array = cast("ArrowTensorArray", array)
                storage_series = _series_from_arrow_with_ray_data_extensions(tensor_array.storage, name=name)
                series = storage_series.cast(
                    DataType.fixed_size_list(
                        _from_arrow_type_with_ray_data_extensions(tensor_array.type.scalar_type),
                        int(np.prod(array.type.shape)),
                    )
                )
                return series.cast(DataType.from_arrow_type(array.type))
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
        raise ValueError(f"Expected a Ray object ref or a Pandas DataFrame, got {type(df)}")


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
        # Both ArrowTensorType and ArrowTensorTypeV2 have a shape attribute
        # ArrowVariableShapedTensorType does not
        shape = getattr(tensor_types, "shape", None)
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


# Give the same function different names to aid in profiling data distribution.


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
            if address is not None and address.startswith("ray://"):
                logger.warning(
                    "Specifying a Ray address with the 'ray://' prefix uses the Ray Client, which may impact performance. If this is running in a Ray job, you may not need to specify the address at all."
                )

            ray.init(address=address)

        self.flotilla_plan_runner: FlotillaRunner | None = None

    def initialize_partition_set_cache(self) -> PartitionSetCache:
        return PartitionSetCache()

    def run_iter(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[RayMaterializedResult]:
        track_runner_on_scarf(runner=self.name)

        # Grab and freeze the current context
        ctx = get_context()
        query_id = str(uuid.uuid4())
        daft_execution_config = ctx.daft_execution_config

        # Optimize the logical plan.
        builder = builder.optimize(daft_execution_config)

        distributed_plan = DistributedPhysicalPlan.from_logical_plan_builder(
            builder._builder, query_id, daft_execution_config
        )
        if self.flotilla_plan_runner is None:
            self.flotilla_plan_runner = FlotillaRunner()

        yield from self.flotilla_plan_runner.stream_plan(
            distributed_plan, self._part_set_cache.get_all_partition_sets()
        )

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
