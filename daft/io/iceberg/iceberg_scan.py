from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING

from pyiceberg.schema import visit

import daft
from daft.daft import (
    CountMode,
    ParquetSourceConfig,
    StorageConfig,
)
from daft.dependencies import pa
from daft.expressions import ExpressionsProjection
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

    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import Table
    from pyiceberg.typedef import Record

    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)


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
) -> PartitionField:
    source_id = pfield.source_id
    source_field = iceberg_schema.find_field(source_id)
    source_name = source_field.name
    source_type = convert_iceberg_data_type(source_field.field_type)
    daft_field = Field.create(source_name, source_type)
    try:
        partition_transform, result_type = convert_iceberg_transform(pfield.transform, source_type)
    except NotImplementedError:
        warnings.warn(f"{pfield.transform} not implemented, Please make an issue!")
        partition_transform = None
        result_type = source_type
    result_field = Field.create(pfield.name, result_type)
    return PartitionField.create(result_field, daft_field, transform=partition_transform)


def iceberg_partition_spec_to_fields(iceberg_schema: IcebergSchema, spec: IcebergPartitionSpec) -> list[PartitionField]:
    return [_iceberg_partition_field_to_daft_partition_field(iceberg_schema, field) for field in spec.fields]


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
        self._partition_fields = iceberg_partition_spec_to_fields(iceberg_schema, self._iceberg_table.spec())

    @property
    def name(self) -> str:
        return f"IcebergDataSource({'.'.join(self._iceberg_table.name())})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        return self._partition_fields

    def _iceberg_record_to_partition_spec(
        self, spec: IcebergPartitionSpec, record: Record
    ) -> daft.recordbatch.RecordBatch | None:
        partition_fields = iceberg_partition_spec_to_fields(self._iceberg_table.schema(), spec)
        arrays = dict()
        assert len(record) == len(partition_fields)
        for idx, pfield in enumerate(partition_fields):
            field = pfield.field
            field_name = field.name
            field_dtype = field.dtype
            arrow_type = field_dtype.to_arrow_dtype()
            arrays[field_name] = daft.Series.from_arrow(pa.array([record[idx]], type=arrow_type), name=field_name).cast(
                field_dtype
            )
        if len(arrays) > 0:
            return daft.recordbatch.RecordBatch.from_pydict(arrays)
        else:
            return None

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

        should_limit_files = limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None

        if len(self._partition_fields) > 0 and pushdowns.partition_filters is None:
            logger.warning(
                "%s has Partitioning Keys: %s but no partition filter was specified. This will result in a full table scan.",
                self.name,
                self._partition_fields,
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

            # TODO: Thread in Statistics to each task: P2
            pspec = self._iceberg_record_to_partition_spec(self._iceberg_table.specs()[file.spec_id], file.partition)

            # Partition pruning is the DataSource's responsibility in the DataSource model.
            if pspec is not None and pushdowns.partition_filters is not None:
                filtered = pspec.filter(ExpressionsProjection([pushdowns.partition_filters]))
                if len(filtered) == 0:
                    continue

            yield DataSourceTask.parquet(
                path=path,
                schema=self._schema,
                parquet_config=self._parquet_config,
                pushdowns=pushdowns,
                num_rows=record_count,
                size_bytes=file.file_size_in_bytes,
                partition_values=pspec,
                storage_config=self._storage_config,
                iceberg_delete_files=iceberg_delete_files if iceberg_delete_files else None,
            )
            rows_left -= record_count

    def _create_count_tasks(self, pushdowns: Pushdowns, field_name: str) -> Iterator[DataSourceTask]:
        """Create count pushdown task using Iceberg metadata."""
        try:
            iceberg_tasks = self._iceberg_table.scan(limit=None, snapshot_id=self._snapshot_id).plan_files()
            total_count = 0

            # Aggregate row counts from all data files
            for task in iceberg_tasks:
                data_file = task.file
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
