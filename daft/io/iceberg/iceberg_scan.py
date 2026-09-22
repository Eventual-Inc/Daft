from __future__ import annotations

import logging
import struct
import warnings
from typing import TYPE_CHECKING

from pyiceberg.conversions import from_bytes
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.manifest import ManifestContent
from pyiceberg.schema import visit
from pyiceberg.transforms import IdentityTransform, VoidTransform
from pyiceberg.types import PrimitiveType

import daft
from daft.daft import (
    CountMode,
    ParquetSourceConfig,
    StorageConfig,
)
from daft.dependencies import pa
from daft.expressions import ExpressionsProjection
from daft.expressions.visitor import _ColumnVisitor
from daft.io.iceberg._expressions import convert_row_filter
from daft.io.iceberg._metadata import (
    convert_iceberg_data_type,
    convert_iceberg_schema,
    convert_iceberg_transform,
)
from daft.io.iceberg.schema_field_id_mapping_visitor import SchemaFieldIdMappingVisitor
from daft.io.partitioning import PartitionField
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Field, Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from pyiceberg.manifest import DataFile
    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import Table
    from pyiceberg.typedef import Record

    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)

# Errors that can occur during Iceberg stats decoding and are safe to treat
# as "stats unavailable for this field" rather than fatal errors.
_EXPECTED_STATS_DECODE_ERRORS = (
    pa.ArrowInvalid,
    pa.ArrowTypeError,
    pa.ArrowNotImplementedError,
    TypeError,
    ValueError,
    NotImplementedError,
    struct.error,
)


def _iceberg_count_result_function(total_count: int, field_name: str) -> Iterator[RecordBatch]:
    """Construct Iceberg count query result."""
    try:
        arrow_schema = pa.schema([pa.field(field_name, pa.uint64())])
        arrow_array = pa.array([total_count], type=pa.uint64())
        arrow_batch = pa.RecordBatch.from_arrays([arrow_array], [field_name])

        logger.debug("Generated Iceberg count result: %s=%d", field_name, total_count)

        yield RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema)
    except Exception as e:
        logger.error("Failed to construct Iceberg count result: %s", e)
        raise


class _IcebergCountTask(DataSourceTask):
    """Metadata-only count result task produced for count pushdown."""

    def __init__(self, total_count: int, field_name: str, schema: Schema) -> None:
        self._total_count = total_count
        self._field_name = field_name
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        for batch in _iceberg_count_result_function(self._total_count, self._field_name):
            yield batch


def _iceberg_partition_field_to_daft_partition_field(
    iceberg_schema: IcebergSchema, pfield: IcebergPartitionField
) -> PartitionField | None:
    # In v1, removing a partition field replaces its transform with void. Its
    # null values neither settle predicates nor represent the source column.
    if isinstance(pfield.transform, VoidTransform):
        return None
    source_id = pfield.source_id
    try:
        source_field = iceberg_schema.find_field(source_id)
    except ValueError:
        # The current spec may reference a column absent from a historical snapshot.
        return None
    source_name = source_field.name
    if iceberg_schema.find_column_name(source_id) != source_name:
        # PartitionField source names address top-level columns, not nested leaves.
        return None
    is_identity = isinstance(pfield.transform, IdentityTransform)
    if not is_identity and pfield.name in iceberg_schema.column_names:
        # A current partition name may collide with a historical data column.
        # Never let derived values masquerade as that column's identity values.
        return None
    source_type = convert_iceberg_data_type(source_field.field_type)
    daft_field = Field.create(source_name, source_type)
    try:
        partition_transform, result_type = convert_iceberg_transform(pfield.transform, source_type)
    except NotImplementedError:
        warnings.warn(f"{pfield.transform} not implemented, Please make an issue!")
        return None
    # Identity values are source values, regardless of the partition field's
    # metadata name. Use the query's column name so aliases retain count pushdown.
    field_name = source_name if is_identity else pfield.name
    result_field = Field.create(field_name, result_type)
    return PartitionField.create(result_field, daft_field, transform=partition_transform)


def _partition_field_key(field: IcebergPartitionField) -> tuple[int, str]:
    # The serialized transform includes its parameters (e.g. bucket[4]). Names
    # can change without changing values, and v1 partition IDs can be reused.
    return field.source_id, str(field.transform)


def _build_iceberg_data_source_task_stats(
    file: DataFile,
    top_level_fields: list[tuple[int, str, object, bool, pa.DataType]],
    task_schema: Schema,
) -> RecordBatch | None:
    """Decode Iceberg DataFile column bounds into a 2-row stats RecordBatch.

    Returns a RecordBatch with row 0 = min values, row 1 = max values,
    matching the column names, order, and dtypes of ``task_schema`` exactly.

    Columns whose bounds are missing or fail to decode are filled with typed
    nulls. If no column produces usable bounds, returns ``None``.

    Args:
        file: PyIceberg DataFile with optional ``lower_bounds`` / ``upper_bounds`` dicts.
        top_level_fields: Precomputed list of ``(field_id, name, iceberg_type,
            is_primitive, arrow_type)`` in schema order.
        task_schema: Daft Schema the returned RecordBatch must match.
    """
    lower = file.lower_bounds or {}
    upper = file.upper_bounds or {}

    if not lower and not upper:
        return None

    arrays: dict[str, daft.Series] = {}
    has_decoded_bounds = False

    for field_id, field_name, field_type, is_primitive, arrow_type in top_level_fields:
        # Require both lower and upper bounds. Single-sided bounds
        # (e.g. key-only metrics with only one direction populated)
        # cannot be expressed by ColumnRangeStatistics — it requires
        # paired min/max — so the field is conservatively treated as
        # unknown rather than partially used.
        if is_primitive and field_id in lower and field_id in upper:
            assert isinstance(field_type, PrimitiveType)  # narrow object→PrimitiveType for mypy
            try:
                values = [
                    from_bytes(field_type, lower[field_id]),
                    from_bytes(field_type, upper[field_id]),
                ]
                arrays[field_name] = daft.Series.from_arrow(
                    pa.array(values, type=arrow_type),
                    name=field_name,
                    dtype=task_schema[field_name].dtype,
                )
                has_decoded_bounds = True
                continue
            except _EXPECTED_STATS_DECODE_ERRORS:
                logger.debug(
                    "Failed to decode stats for field %s (id=%d, type=%s)",
                    field_name,
                    field_id,
                    field_type,
                )

        arrays[field_name] = daft.Series.from_arrow(
            pa.array([None, None], type=arrow_type),
            name=field_name,
            dtype=task_schema[field_name].dtype,
        )

    if not has_decoded_bounds:
        return None

    stats = RecordBatch.from_pydict(arrays)
    assert stats.schema() == task_schema, (
        f"stats schema {stats.schema().column_names()} != task schema {task_schema.column_names()}"
    )
    return stats


class IcebergDataSource(DataSource):
    """DataSource for Apache Iceberg tables.

    Uses pyiceberg for catalog metadata and scan planning (file listing,
    partition pruning, statistics-based file skipping), then yields
    DataSourceTask objects executed by Daft's native Parquet reader.
    Positional delete files are passed through to the native reader.

    For count aggregation pushdowns on tables without delete files, a
    metadata-only _IcebergCountTask is yielded instead of scanning data files.
    """

    def __init__(
        self,
        iceberg_table: Table,
        snapshot_id: int | None,
        storage_config: StorageConfig,
        ignore_corrupt_files: bool = False,
    ) -> None:
        iceberg_schema = (
            iceberg_table.schema() if snapshot_id is None else iceberg_table.scan(snapshot_id=snapshot_id).projection()
        )
        self._iceberg_table = iceberg_table
        self._iceberg_schema = iceberg_schema
        self._snapshot_id = snapshot_id
        self._storage_config = storage_config

        field_id_mapping = visit(iceberg_schema, SchemaFieldIdMappingVisitor())
        self._parquet_config = ParquetSourceConfig(
            field_id_mapping=field_id_mapping,
            ignore_corrupt_files=ignore_corrupt_files,
        )

        self._schema = convert_iceberg_schema(iceberg_schema)
        self._partition_fields = {
            _partition_field_key(field): converted
            for field in self._iceberg_table.spec().fields
            if (converted := _iceberg_partition_field_to_daft_partition_field(iceberg_schema, field)) is not None
        }
        self._settleable_partition_fields: list[PartitionField] | None = None

        # Precompute ordered field metadata for per-file stats decoding.  The
        # order must match self._schema — downstream consumers (TableStatistics,
        # MicroPartition) index columns by position and require exact equality.
        self._top_level_fields: list[tuple[int, str, object, bool, pa.DataType]] = [
            (
                field.field_id,
                field.name,
                field.field_type,
                isinstance(field.field_type, PrimitiveType),
                schema_to_pyarrow(field.field_type),
            )
            for field in iceberg_schema.fields
        ]

    @property
    def name(self) -> str:
        return f"IcebergDataSource({'.'.join(self._iceberg_table.name())})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        """Partition fields the optimizer may treat as settling a predicate on their own.

        A predicate over an identity-partitioned field is dropped from the row-level filter
        on the assumption that pruning covers it. Partition evolution breaks that: a file
        written before a field was added carries no value for it and holds a mix of matching
        and non-matching rows. Reporting only the fields every live file carries keeps such
        predicates as ordinary row filters instead.
        """
        if self._settleable_partition_fields is None:
            if not self._partition_fields:
                self._settleable_partition_fields = []
                return self._settleable_partition_fields
            live = self._live_partition_field_keys()
            if live is None:
                # A later call can retry a failed manifest-list read. Successful
                # empty intersections, unlike failures, are safe to cache.
                return []
            self._settleable_partition_fields = [field for key, field in self._partition_fields.items() if key in live]
        return self._settleable_partition_fields

    def _live_partition_field_keys(self) -> set[tuple[int, str]] | None:
        """Partition semantics carried by every live data file in the selected snapshot.

        Manifests record the spec they were written with, so this reads the manifest list
        rather than planning files. Returns None when that cannot be determined, which
        callers treat as "settle nothing".
        """
        try:
            snapshot = (
                self._iceberg_table.snapshot_by_id(self._snapshot_id)
                if self._snapshot_id is not None
                else self._iceberg_table.current_snapshot()
            )
            live = set(self._partition_fields)
            if snapshot is None:
                return live
            specs = self._iceberg_table.specs()
            spec_ids = {
                manifest.partition_spec_id
                for manifest in snapshot.manifests(self._iceberg_table.io)
                if manifest.content == ManifestContent.DATA
                and (manifest.has_added_files() or manifest.has_existing_files())
            }
            for spec_id in spec_ids:
                spec = specs.get(spec_id)
                if spec is None:
                    return None
                live.intersection_update(_partition_field_key(field) for field in spec.fields)
            return live
        except Exception as e:
            logger.warning("Could not determine partition specs in use: %s, disabling partition pruning", e)
            return None

    def _iceberg_record_to_partition_values(
        self, spec: IcebergPartitionSpec, record: Record
    ) -> tuple[RecordBatch | None, RecordBatch | None]:
        """Return pruning values and identity constants safe to fill into data columns."""
        pruning_values = {}
        identity_values = {}
        assert len(record) == len(spec.fields)
        # Record positions belong to the original spec, including removed/void fields.
        for idx, iceberg_field in enumerate(spec.fields):
            candidate = self._partition_fields.get(_partition_field_key(iceberg_field))
            is_identity = isinstance(iceberg_field.transform, IdentityTransform)
            pfield = candidate
            if pfield is None and is_identity:
                pfield = _iceberg_partition_field_to_daft_partition_field(self._iceberg_schema, iceberg_field)
            if pfield is None:
                continue
            field = pfield.field
            field_name = field.name
            field_dtype = field.dtype
            arrow_type = field_dtype.to_arrow_dtype()
            value = daft.Series.from_arrow(pa.array([record[idx]], type=arrow_type), name=field_name).cast(field_dtype)
            if candidate is not None:
                pruning_values[field_name] = value
            if is_identity:
                # Native readers may skip these columns and fill them with constants.
                # Derived partition values (especially void's null) cannot be used here.
                identity_values[field_name] = value
        return (
            RecordBatch.from_pydict(pruning_values) if pruning_values else None,
            RecordBatch.from_pydict(identity_values) if identity_values else None,
        )

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        # Check if there is a count aggregation pushdown
        py_pushdowns = pushdowns._to_pypushdowns()
        if (
            py_pushdowns.aggregation is not None
            and py_pushdowns.aggregation_count_mode() is not None
            and py_pushdowns.aggregation_required_column_names()
        ):
            count_mode = py_pushdowns.aggregation_count_mode()
            fields = py_pushdowns.aggregation_required_column_names()

            if count_mode in self.supported_count_modes():
                logger.info(
                    "Using Iceberg count pushdown optimization for count mode: %s",
                    count_mode,
                )
                for task in self._create_count_tasks(pushdowns, fields[0]):
                    yield task
                return
            else:
                logger.warning(
                    "Count mode %s is not supported for pushdown, falling back to regular scan",
                    count_mode,
                )

        # Regular scan without count pushdown
        for task in self._create_regular_tasks(pushdowns):
            yield task

    def _create_regular_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        """Create regular tasks without count pushdown."""
        limit = pushdowns.limit
        row_filter = convert_row_filter(pushdowns._to_pypushdowns(), self._iceberg_schema)

        iceberg_tasks = self._iceberg_table.scan(
            row_filter=row_filter,
            limit=limit,
            snapshot_id=self._snapshot_id,
        ).plan_files()

        # Fields a file's partition record must carry for the predicate to be evaluable.
        required_partition_fields = (
            _ColumnVisitor().visit(pushdowns.partition_filters) if pushdowns.partition_filters is not None else set()
        )

        should_limit_files = limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None

        if len(self._partition_fields) > 0 and pushdowns.partition_filters is None:
            logger.warning(
                "%s has Partitioning Keys: %s but no partition filter was specified. This will result in a full table scan.",
                self.name,
                list(self._partition_fields.values()),
            )

        if limit is not None:
            rows_left = limit
        else:
            rows_left = 0
        for task in iceberg_tasks:
            if should_limit_files and (rows_left <= 0):
                break
            file = task.file
            path = file.file_path
            record_count = file.record_count
            file_format = file.file_format
            if file_format != "PARQUET":
                # TODO: Support ORC and AVRO when we can read it
                raise NotImplementedError(f"{file_format} for iceberg not implemented!")

            iceberg_delete_files = [f.file_path for f in task.delete_files]

            pspec, partition_values = self._iceberg_record_to_partition_values(
                self._iceberg_table.specs()[file.spec_id], file.partition
            )

            # Partition pruning is the DataSource's responsibility in the DataSource model.
            # Only prune on a record that carries every field the predicate references;
            # get_partition_fields keeps such predicates out of `partition_filters` in the
            # first place. Keep this check before evaluating synthetic partition columns.
            if (
                pspec is not None
                and pushdowns.partition_filters is not None
                and required_partition_fields.issubset(pspec.schema().column_names())
            ):
                filtered = pspec.filter(ExpressionsProjection([pushdowns.partition_filters]))
                if len(filtered) == 0:
                    continue

            stats = _build_iceberg_data_source_task_stats(
                file=file,
                top_level_fields=self._top_level_fields,
                task_schema=self._schema,
            )

            yield DataSourceTask.parquet(
                path=path,
                schema=self._schema,
                parquet_config=self._parquet_config,
                pushdowns=pushdowns,
                num_rows=record_count,
                size_bytes=file.file_size_in_bytes,
                partition_values=partition_values,
                stats=stats,
                storage_config=self._storage_config,
                iceberg_delete_files=iceberg_delete_files if iceberg_delete_files else None,
            )
            rows_left -= record_count

    def _create_count_tasks(self, pushdowns: Pushdowns, field_name: str) -> Iterator[DataSourceTask]:
        """Create count pushdown task using Iceberg metadata."""
        try:
            if pushdowns.filters is not None:
                # A row-level predicate cannot be answered from record counts: a file
                # surviving metadata pruning may still hold rows that do not match.
                yield from self._create_regular_tasks(pushdowns)
                return

            # Forwarding the predicate lets PyIceberg drop whole manifests from its partition
            # summaries instead of materializing every data file entry for us to filter.
            # It only ever returns a superset of the matching files, and degrades to no
            # pruning when the predicate cannot be converted, so the per-file check below
            # remains the authority on what is counted.
            row_filter = convert_row_filter(pushdowns._to_pypushdowns(), self._iceberg_schema)
            iceberg_tasks = self._iceberg_table.scan(
                row_filter=row_filter, limit=None, snapshot_id=self._snapshot_id
            ).plan_files()
            total_count = 0

            # Aggregate row counts from all data files. `partition_filters` holds the
            # predicates the optimizer resolved against partition values alone and then
            # dropped from `filters`, so applying them here is what keeps the count honest.
            # Partition records are stored whole, unlike the truncated column bounds
            # PyIceberg prunes on, so this check is exact: every row of a surviving file
            # matches the predicate.
            required_fields = (
                _ColumnVisitor().visit(pushdowns.partition_filters)
                if pushdowns.partition_filters is not None
                else set()
            )

            for task in iceberg_tasks:
                data_file = task.file
                if pushdowns.partition_filters is not None:
                    pspec, _ = self._iceberg_record_to_partition_values(
                        self._iceberg_table.specs()[data_file.spec_id], data_file.partition
                    )
                    # A file written under an older spec need not carry the fields the
                    # predicate partitions on: partition evolution can add a field long
                    # after data was written without it. Such a file mixes matching and
                    # non-matching rows, so its rows have to be read to be counted.
                    if pspec is None or not required_fields.issubset(pspec.schema().column_names()):
                        yield from self._create_regular_tasks(pushdowns)
                        return
                    if len(pspec.filter(ExpressionsProjection([pushdowns.partition_filters]))) == 0:
                        continue
                total_count += data_file.record_count

            result_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(field_name, pa.uint64())]))

            logger.info(
                "Created Iceberg count pushdown task with total_count=%d for field=%s",
                total_count,
                field_name,
            )
            yield _IcebergCountTask(total_count, field_name, result_schema)
        except Exception as e:
            logger.error(
                "Failed to create Iceberg count pushdown task: %s, now falling back to regular scan",
                e,
            )
            yield from self._create_regular_tasks(pushdowns)

    def _has_delete_files(self) -> bool:
        """Check if the table has any delete files.

        This method quickly scans the table to determine if there are any delete files
        present. If delete files are found, count pushdown should be disabled to avoid
        complex delete file processing logic.

        Returns:
            True if the table has delete files, False otherwise
        """
        try:
            # Get a limited scan to check for delete files
            iceberg_tasks = self._iceberg_table.scan(
                limit=1,  # Only need to check if any delete files exist
                snapshot_id=self._snapshot_id,
            ).plan_files()

            # Check if any task has delete files
            for task in iceberg_tasks:
                if task.delete_files and len(task.delete_files) > 0:
                    logger.debug("Found delete files in table, count pushdown will be disabled")
                    return True
            return False

        except Exception as e:
            logger.warning(
                "Error checking for delete files: %s, disabling count pushdown as precaution",
                e,
            )
            return True

    def supports_count_pushdown(self) -> bool:
        return not self._has_delete_files()

    def supported_count_modes(self) -> list[CountMode]:
        return [CountMode.All]
