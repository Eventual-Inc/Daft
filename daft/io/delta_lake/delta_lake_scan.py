from __future__ import annotations

import logging
from numbers import Integral
from typing import TYPE_CHECKING
from urllib.error import HTTPError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

import deltalake
from deltalake.exceptions import TableNotFoundError
from deltalake.schema import ArrayType, MapType, StructType
from deltalake.schema import Schema as DeltaSchema
from deltalake.table import DeltaTable
from packaging.version import parse

import daft
import daft.exceptions
from daft import DataType
from daft.daft import (
    CountMode,
    ParquetSourceConfig,
    PyField,
    S3Config,
    StorageConfig,
)
from daft.dependencies import pa, pc
from daft.expressions import ExpressionsProjection
from daft.io.delta_lake._deltalake import delta_schema_to_pyarrow
from daft.io.delta_lake._visitors import convert_filter_to_stats_predicate
from daft.io.delta_lake.utils import construct_delta_file_path
from daft.io.object_store_options import io_config_to_storage_options
from daft.io.partitioning import PartitionField
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from datetime import datetime

    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)

_CM_ID_KEY = "delta.columnMapping.id"
_CM_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName"


def _delta_field_to_pyfield(field: deltalake.schema.Field) -> PyField:
    """Convert a Delta `Field` to a Daft `PyField` carrying its logical name and real dtype."""
    pa_field = delta_schema_to_pyarrow(DeltaSchema([field])).field(0)
    return PyField.create(field.name, DataType.from_arrow_type(pa_field.type)._dtype)


def _iter_mapped_fields(schema: deltalake.Schema) -> Iterator[deltalake.schema.Field]:
    """Yield every Delta `Field` in the schema that carries column-mapping metadata.

    Per Delta protocol, list elements / map keys / map values are anonymous (no
    `columnMapping.id`), but their *element type* may still contain mapped struct
    fields (e.g. `array<struct<...>>`). We recurse through container types but only
    yield fields that actually carry the mapping metadata.
    """

    def walk_type(t: object) -> Iterator[deltalake.schema.Field]:
        if isinstance(t, StructType):
            for sub in t.fields:
                if (sub.metadata or {}).get(_CM_ID_KEY) is not None:
                    yield sub
                yield from walk_type(sub.type)
        elif isinstance(t, ArrayType):
            yield from walk_type(t.element_type)
        elif isinstance(t, MapType):
            yield from walk_type(t.key_type)
            yield from walk_type(t.value_type)

    for field in schema.fields:
        if (field.metadata or {}).get(_CM_ID_KEY) is not None:
            yield field
        yield from walk_type(field.type)


def _column_mapping_maps(
    schema: deltalake.Schema,
) -> tuple[dict[int, PyField], dict[str, str]]:
    """Build column-mapping lookups for a column-mapped Delta schema.

    Returns:
    - `field_id -> PyField(logical_name, dtype)` covering every mapped field including
      nested, for the Parquet field-id rename path.
    - `physical_name -> logical_name` for top-level fields only, used to rename min/max
      stats columns (delta-rs exposes those keyed by physical name).
    """
    field_ids = {
        int(field.metadata[_CM_ID_KEY]): _delta_field_to_pyfield(field) for field in _iter_mapped_fields(schema)
    }
    phys_to_logical = {
        meta[_CM_PHYSICAL_NAME_KEY]: field.name
        for field in schema.fields
        if (meta := field.metadata or {}).get(_CM_PHYSICAL_NAME_KEY) is not None
    }
    return field_ids, phys_to_logical


def get_s3_bucket_region(bucket_name: str) -> str | None:
    # When making a https request to https://{bucket_name}.s3.amazonaws.com/, aws returns either a 200 response, 403 response, or 404 response.
    # In the header of the 200 and 403 responses, there is an `x-amz-bucket-region` field from which we can extract the bucket's region.
    url = f"https://{bucket_name}.s3.amazonaws.com"

    try:
        req = Request(url, method="HEAD")
        with urlopen(req) as response:
            return response.headers.get("x-amz-bucket-region")
    except HTTPError as e:
        bucket_region = e.headers.get("x-amz-bucket-region")
        if bucket_region is None:
            logger.warning(
                "Failed to get the S3 bucket region using the given S3 uri. HTTPError error: %s",
                e,
            )
        return bucket_region
    except Exception as e:
        logger.warning(
            "Failed to get the S3 bucket region using the given S3 uri. Error: %s",
            e,
        )
        return None


class _DeltaCountTask(DataSourceTask):
    """Metadata-only count result produced for count pushdown."""

    def __init__(self, total_count: int, field_name: str) -> None:
        self._total_count = total_count
        self._field_name = field_name
        self._arrow_schema = pa.schema([pa.field(field_name, pa.uint64())])
        self._schema = Schema.from_pyarrow_schema(self._arrow_schema)

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        batch = pa.RecordBatch.from_arrays([pa.array([self._total_count], type=pa.uint64())], [self._field_name])
        yield RecordBatch.from_arrow_record_batches([batch], self._arrow_schema)


class DeltaLakeDataSource(DataSource):
    def __init__(
        self,
        table_uri: str,
        storage_config: StorageConfig,
        version: int | str | datetime | None = None,
        ignore_deletion_vectors: bool = False,
    ) -> None:
        # Unfortunately delta-rs doesn't do very good inference of credentials for S3. Thus the current Daft behavior of passing
        # in `None` for credentials will cause issues when instantiating the DeltaTable without credentials.
        #
        # Thus, if we don't detect any credentials being available, we attempt to detect it from the environment using our Daft credentials chain.
        #
        # See: https://github.com/delta-io/delta-rs/issues/2117
        deltalake_sdk_io_config = storage_config.io_config
        scheme = urlparse(table_uri).scheme
        if scheme == "s3" or scheme == "s3a":
            # Try to get the bucket's region.
            if deltalake_sdk_io_config.s3.region_name is None:
                bucket_name = urlparse(table_uri).netloc
                region = get_s3_bucket_region(bucket_name)
                if region is not None:
                    deltalake_sdk_io_config = deltalake_sdk_io_config.replace(
                        s3=deltalake_sdk_io_config.s3.replace(region_name=region)
                    )

            # Try to get config from the environment
            if any(
                [
                    deltalake_sdk_io_config.s3.key_id is None,
                    deltalake_sdk_io_config.s3.region_name is None,
                ]
            ):
                try:
                    s3_config_from_env = S3Config.from_env()
                # Sometimes S3Config.from_env throws an error, for example on CI machines with weird metadata servers.
                except daft.exceptions.DaftCoreException:
                    pass
                else:
                    if (
                        deltalake_sdk_io_config.s3.key_id is None
                        and deltalake_sdk_io_config.s3.access_key is None
                        and deltalake_sdk_io_config.s3.session_token is None
                    ):
                        deltalake_sdk_io_config = deltalake_sdk_io_config.replace(
                            s3=deltalake_sdk_io_config.s3.replace(
                                key_id=s3_config_from_env.key_id,
                                access_key=s3_config_from_env.access_key,
                                session_token=s3_config_from_env.session_token,
                            )
                        )
                    if deltalake_sdk_io_config.s3.region_name is None:
                        deltalake_sdk_io_config = deltalake_sdk_io_config.replace(
                            s3=deltalake_sdk_io_config.s3.replace(
                                region_name=s3_config_from_env.region_name,
                            )
                        )
        elif scheme == "gcs" or scheme == "gs" or scheme == "az" or scheme == "abfs" or scheme == "abfss":
            # TO-DO: Handle any key-value replacements in `io_config` if there are missing elements
            pass

        self._table = DeltaTable(
            table_uri,
            storage_options=io_config_to_storage_options(deltalake_sdk_io_config, table_uri),
        )

        if version is not None:
            if isinstance(version, Integral) and not isinstance(version, bool):
                requested_version = int(version)
                latest_version = self._table.version()
                if requested_version < 0 or requested_version > latest_version:
                    raise TableNotFoundError(
                        f"Requested Delta Lake table version {requested_version} does not exist. "
                        f"Latest available version is {latest_version}."
                    )
                self._table.load_as_version(requested_version)
            else:
                self._table.load_as_version(version)

        self._storage_config = storage_config

        delta_schema = self._table.schema()
        self._schema = Schema.from_pyarrow_schema(delta_schema_to_pyarrow(delta_schema))

        cm_mode = self._table.metadata().configuration.get("delta.columnMapping.mode", "none").lower()
        if cm_mode not in ("none", "id", "name"):
            raise NotImplementedError(f"Unsupported Delta Lake column mapping mode: {cm_mode!r}")
        # delta-rs validates that every mapped field carries `delta.columnMapping.id` at
        # `DeltaTable()` construction (raises "Kernel error: Invalid column mapping mode: …
        # lacks the delta.columnMapping.id annotation"), so we can build the mapping
        # without re-checking that here.
        if cm_mode == "none":
            field_id_mapping: dict[int, PyField] | None = None
            self._stats_physical_to_logical: dict[str, str] = {}
        else:
            field_id_mapping, self._stats_physical_to_logical = _column_mapping_maps(delta_schema)
        self._parquet_config = ParquetSourceConfig(field_id_mapping=field_id_mapping)

        partition_columns = set(self._table.metadata().partition_columns)
        self._partition_fields = [
            PartitionField.create(field) for field in self._schema if field.name in partition_columns
        ]
        self._ignore_deletion_vectors = ignore_deletion_vectors

    @property
    def schema(self) -> Schema:
        return self._schema

    @property
    def name(self) -> str:
        return f"DeltaLakeDataSource({self._table.metadata().name})"

    def get_partition_fields(self) -> list[PartitionField]:
        return self._partition_fields

    def supports_count_pushdown(self) -> bool:
        # Record counts include rows a deletion vector removes.
        return not self._deletion_vectors_enabled()

    def supported_count_modes(self) -> list[CountMode]:
        return [CountMode.All]

    def _deletion_vectors_enabled(self) -> bool:
        return self._table.metadata().configuration.get("delta.enableDeletionVectors", "false").lower() == "true"

    def _stats_columns(self, add_actions: pa.Table) -> dict[str, tuple[str, pa.DataType]]:
        """Map each logical column name to its physical stats name and stats type."""
        if "min" not in add_actions.schema.names or "max" not in add_actions.schema.names:
            return {}
        min_type = add_actions.schema.field("min").type
        return {
            self._stats_physical_to_logical.get(f.name, f.name): (f.name, f.type)
            for f in (min_type.field(i) for i in range(min_type.num_fields))
        }

    def _skip_files_by_stats(self, add_actions: pa.Table, pushdowns: Pushdowns) -> pa.Table:
        if pushdowns.filters is None:
            return add_actions
        predicate = convert_filter_to_stats_predicate(pushdowns.filters, self._stats_columns(add_actions))
        if predicate is None:
            return add_actions
        try:
            return add_actions.filter(predicate)
        except Exception as e:
            logger.warning("Could not apply Delta stats predicate, skipping file pruning: %s", e)
            return add_actions

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        py_pushdowns = pushdowns._to_pypushdowns()
        if (
            py_pushdowns.aggregation is not None
            and py_pushdowns.aggregation_count_mode() in self.supported_count_modes()
            and py_pushdowns.aggregation_required_column_names()
            and pushdowns.filters is None
            and not self._deletion_vectors_enabled()
        ):
            total_count = self._count_from_metadata(pushdowns)
            if total_count is not None:
                yield _DeltaCountTask(total_count, py_pushdowns.aggregation_required_column_names()[0])
                return

        async for task in self._get_scan_tasks(pushdowns):
            yield task

    def _count_from_metadata(self, pushdowns: Pushdowns) -> int | None:
        """Sum record counts of files matching the partition filter, or None if any file lacks a count."""
        add_actions = pa.table(self._table.get_add_actions())
        if "num_records" not in add_actions.schema.names:
            return None
        columns = {"__num_records": add_actions["num_records"]}
        if pushdowns.partition_filters is not None:
            if "partition" not in add_actions.schema.names:
                return None
            partition = add_actions["partition"]
            for i, field in enumerate(partition.type):
                columns[field.name] = pc.struct_field(partition, i)
        files = RecordBatch.from_pydict({name: daft.Series.from_arrow(arr, name) for name, arr in columns.items()})
        if pushdowns.partition_filters is not None:
            # Exact: every row of a file whose partition values match also matches.
            files = files.filter(ExpressionsProjection([pushdowns.partition_filters]))
        counts = files.to_arrow_table()["__num_records"]
        if counts.null_count > 0:
            return None
        return int(pc.sum(counts).as_py() or 0)

    async def _get_scan_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        metadata = self._table.metadata()
        deletion_vectors_enabled = metadata.configuration.get("delta.enableDeletionVectors", "false").lower() == "true"
        # If deletion vectors are enabled and the deltalake library does not support propagation
        # raise an error if ignore_deletion_vectors is not set.
        if deletion_vectors_enabled and _missing_deletion_vectors_propagation():
            if not self._ignore_deletion_vectors:
                raise NotImplementedError(
                    "Delta Lake deletion vectors are not yet supported; please let the Daft team know if you'd like to see this feature!\n"
                    "Note that for Delta Lake versions 1.2.0 and newer, deletion vectors are not included in add actions.\n"
                    "Deletion records can be dropped from this table to allow it to be read with Daft: https://docs.delta.io/latest/delta-drop-feature.html\n"
                    "Alternatively, you can set ignore_deletion_vectors=True to skip checking for deletion vectors."
                )

        add_actions = self._skip_files_by_stats(pa.table(self._table.get_add_actions()), pushdowns)
        if self._partition_fields and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partitioning keys = %s, but no partition filter was specified. This will result in a full table scan.",
                self.name,
                self._partition_fields,
            )

        # TODO(Clark): Add support for deletion vectors.
        # Issue: https://github.com/Eventual-Inc/Daft/issues/1954
        if not self._ignore_deletion_vectors and "deletionVector" in add_actions.schema.names:
            raise NotImplementedError(
                "Delta Lake deletion vectors are not yet supported; please let the Daft team know if you'd like to see this feature!\n"
                "Deletion records can be dropped from this table to allow it to be read with Daft: https://docs.delta.io/latest/delta-drop-feature.html\n"
                "Alternatively, you can set ignore_deletion_vectors=True to skip checking for deletion vectors."
            )

        limit_files = pushdowns.limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None
        rows_left = pushdowns.limit if pushdowns.limit is not None else 0

        # Determine which partition field name is used in the schema
        # Deltalake versions <1.2.0 use "partition_values", >=1.2.0 use "partition"
        is_deltalake_v1_2_or_above = parse(deltalake.__version__) >= parse("1.2.0")

        partition_field_name = "partition" if is_deltalake_v1_2_or_above else "partition_values"
        is_partitioned = (
            partition_field_name in add_actions.schema.names
            and add_actions.schema.field(partition_field_name).type.num_fields > 0
        )
        for task_idx in range(add_actions.num_rows):
            if limit_files and rows_left <= 0:
                break

            # NOTE: The paths in the transaction log consist of the post-table-uri suffix.
            # Workaround for deltalake >= 1.2.0: paths are double-encoded in the log but single-encoded on disk.
            scheme = urlparse(self._table.table_uri).scheme
            raw_path = add_actions["path"][task_idx].as_py()
            if is_deltalake_v1_2_or_above:
                # For Delta Lake >= 1.2.0, decode double-encoded paths
                unquoted = unquote(raw_path)
                decoded_path = unquoted if unquoted != raw_path else raw_path
            else:
                # For Delta Lake < 1.2.0, use path as-is
                decoded_path = raw_path
            path = construct_delta_file_path(scheme, self._table.table_uri, decoded_path)

            try:
                record_count = add_actions["num_records"][task_idx].as_py()
            except KeyError:
                record_count = None

            try:
                size_bytes = add_actions["size_bytes"][task_idx].as_py()
            except KeyError:
                size_bytes = None

            if is_partitioned:
                dtype = add_actions.schema.field(partition_field_name).type
                part_values = add_actions[partition_field_name][task_idx]
                arrays = {}
                for field_idx in range(dtype.num_fields):
                    field_name = dtype.field(field_idx).name
                    try:
                        arrow_arr = pa.array([part_values[field_name]], type=dtype.field(field_idx).type)
                    except (
                        pa.ArrowInvalid,
                        pa.ArrowTypeError,
                        pa.ArrowNotImplementedError,
                    ):
                        # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                        arrow_arr = pa.array(
                            [part_values[field_name].as_py()],
                            type=dtype.field(field_idx).type,
                        )
                    arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
                partition_values = daft.recordbatch.RecordBatch.from_pydict(arrays)
            else:
                partition_values = None

            if partition_values is not None and pushdowns.partition_filters is not None:
                filtered = partition_values.filter(ExpressionsProjection([pushdowns.partition_filters]))
                if len(filtered) == 0:
                    continue

            # Populate scan task with column-wise stats.
            schema_names = add_actions.schema.names
            if "min" in schema_names and "max" in schema_names:
                dtype = add_actions.schema.field("min").type
                min_values = add_actions["min"][task_idx]
                max_values = add_actions["max"][task_idx]
                # TODO(Clark): Add support for tracking null counts in column stats.
                # null_counts = add_actions["null_count"][task_idx]
                arrays = {}
                for field_idx in range(dtype.num_fields):
                    field_name = dtype.field(field_idx).name
                    # delta-rs exposes stats under physical names when column mapping is on.
                    logical_name = self._stats_physical_to_logical.get(field_name, field_name)
                    try:
                        arrow_arr = pa.array(
                            [min_values[field_name], max_values[field_name]],
                            type=dtype.field(field_idx).type,
                        )
                    except (
                        pa.ArrowInvalid,
                        pa.ArrowTypeError,
                        pa.ArrowNotImplementedError,
                    ):
                        # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                        arrow_arr = pa.array(
                            [
                                min_values[field_name].as_py(),
                                max_values[field_name].as_py(),
                            ],
                            type=dtype.field(field_idx).type,
                        )
                    arrays[logical_name] = daft.Series.from_arrow(arrow_arr, logical_name)
                stats = daft.recordbatch.RecordBatch.from_pydict(arrays)
            else:
                stats = None
            task = DataSourceTask.parquet(
                path=path,
                schema=self._schema,
                parquet_config=self._parquet_config,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                partition_values=partition_values,
                stats=stats,
            )
            if record_count is not None:
                rows_left -= record_count
            yield task


# Returns True if the Delta Lake library does not return the deletion vectors in the add_actions.
# These were removed in deltalake>=1.2.0.
def _missing_deletion_vectors_propagation() -> bool:
    return parse(deltalake.__version__) >= parse("1.2.0")
