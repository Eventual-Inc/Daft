from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING

from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.schema import visit

import daft
from daft.daft import (
    CountMode,
    FileFormatConfig,
    ParquetSourceConfig,
    PyPartitionField,
    PyPartitionTransform,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
    StorageConfig,
)
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io.iceberg.schema_field_id_mapping_visitor import SchemaFieldIdMappingVisitor
from daft.io.scan import ScanOperator, make_partition_field
from daft.logical.schema import Field, Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyiceberg.partitioning import PartitionField as IcebergPartitionField
    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.table import Table
    from pyiceberg.typedef import Record

logger = logging.getLogger(__name__)


def _iceberg_count_result_function(total_count: int, field_name: str) -> Iterator[PyRecordBatch]:
    """Construct Iceberg count query result."""
    try:
        arrow_schema = pa.schema([pa.field(field_name, pa.uint64())])
        arrow_array = pa.array([total_count], type=pa.uint64())
        arrow_batch = pa.RecordBatch.from_arrays([arrow_array], [field_name])

        logger.debug("Generated Iceberg count result: %s=%d", field_name, total_count)

        yield RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema)._recordbatch
    except Exception as e:
        logger.error("Failed to construct Iceberg count result: %s", e)
        raise


def _iceberg_partition_field_to_daft_partition_field(
    iceberg_schema: IcebergSchema, pfield: IcebergPartitionField
) -> PyPartitionField:
    name = pfield.name
    source_id = pfield.source_id
    source_field = iceberg_schema.find_field(source_id)
    source_name = source_field.name
    daft_field = Field.create(
        source_name, DataType.from_arrow_type(schema_to_pyarrow(iceberg_schema.find_type(source_name)))
    )
    transform = pfield.transform
    source_type = DataType.from_arrow_type(schema_to_pyarrow(source_field.field_type))

    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        IdentityTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )

    tfm = None
    if isinstance(transform, IdentityTransform):
        tfm = PyPartitionTransform.identity()
        result_type = source_type
    elif isinstance(transform, YearTransform):
        tfm = PyPartitionTransform.year()
        result_type = DataType.int32()
    elif isinstance(transform, MonthTransform):
        tfm = PyPartitionTransform.month()
        result_type = DataType.int32()
    elif isinstance(transform, DayTransform):
        tfm = PyPartitionTransform.day()
        # pyiceberg uses date as the result type of a day transform, which is incorrect
        # so we cannot use transform.result_type() here
        result_type = DataType.date()
    elif isinstance(transform, HourTransform):
        tfm = PyPartitionTransform.hour()
        result_type = DataType.int32()
    elif isinstance(transform, BucketTransform):
        n = transform.num_buckets
        tfm = PyPartitionTransform.iceberg_bucket(n)
        result_type = DataType.int32()
    elif isinstance(transform, TruncateTransform):
        w = transform.width
        tfm = PyPartitionTransform.iceberg_truncate(w)
        result_type = source_type
    else:
        warnings.warn(f"{transform} not implemented, Please make an issue!")
        result_type = source_type
    result_field = Field.create(name, result_type)
    return make_partition_field(result_field, daft_field, transform=tfm)


def iceberg_partition_spec_to_fields(
    iceberg_schema: IcebergSchema, spec: IcebergPartitionSpec
) -> list[PyPartitionField]:
    return [_iceberg_partition_field_to_daft_partition_field(iceberg_schema, field) for field in spec.fields]


class IcebergScanOperator(ScanOperator):
    def __init__(self, iceberg_table: Table, snapshot_id: int | None, storage_config: StorageConfig) -> None:
        super().__init__()
        self._table = iceberg_table
        self._snapshot_id = snapshot_id
        self._storage_config = storage_config

        iceberg_schema = (
            iceberg_table.schema()
            if self._snapshot_id is None
            else self._table.scan(snapshot_id=self._snapshot_id).projection()
        )
        arrow_schema = schema_to_pyarrow(iceberg_schema)
        self._field_id_mapping = visit(iceberg_schema, SchemaFieldIdMappingVisitor())
        self._schema = Schema.from_pyarrow_schema(arrow_schema)

        self._partition_keys = iceberg_partition_spec_to_fields(iceberg_schema, self._table.spec())

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "IcebergScanOperator"

    def display_name(self) -> str:
        return f"IcebergScanOperator({'.'.join(self._table.name())})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partition_keys

    def _iceberg_record_to_partition_spec(
        self, spec: IcebergPartitionSpec, record: Record
    ) -> daft.recordbatch.RecordBatch | None:
        partition_fields = iceberg_partition_spec_to_fields(self._table.schema(), spec)
        arrays = dict()
        assert len(record) == len(partition_fields)
        for idx, pfield in enumerate(partition_fields):
            field = Field._from_pyfield(pfield.field)
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

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys}",
            # TODO(Clark): Improve repr of storage config here.
            f"Storage config = {self._storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        # Check if there is a count aggregation pushdown
        if (
            pushdowns.aggregation is not None
            and pushdowns.aggregation_count_mode() is not None
            and pushdowns.aggregation_required_column_names()
        ):
            count_mode = pushdowns.aggregation_count_mode()
            fields = pushdowns.aggregation_required_column_names()

            if count_mode in self.supported_count_modes():
                logger.info(
                    "Using Iceberg count pushdown optimization for count mode: %s",
                    count_mode,
                )
                yield from self._create_count_scan_task(pushdowns, fields[0])
                return
            else:
                logger.warning(
                    "Count mode %s is not supported for pushdown, falling back to regular scan",
                    count_mode,
                )

        # Regular scan without count pushdown
        yield from self._create_regular_scan_tasks(pushdowns)

    def _create_regular_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        """Create regular scan tasks without count pushdown."""
        limit = pushdowns.limit
        iceberg_tasks = self._table.scan(limit=limit, snapshot_id=self._snapshot_id).plan_files()

        limit_files = limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None

        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logger.warning(
                "%s has Partitioning Keys: %s but no partition filter was specified. This will result in a full table scan.",
                self.display_name(),
                self.partitioning_keys(),
            )
        scan_tasks = []

        if limit is not None:
            rows_left = limit
        else:
            rows_left = 0
        for task in iceberg_tasks:
            if limit_files and (rows_left <= 0):
                break
            file = task.file
            path = file.file_path
            record_count = file.record_count
            file_format = file.file_format
            if file_format == "PARQUET":
                file_format_config = FileFormatConfig.from_parquet_config(
                    ParquetSourceConfig(field_id_mapping=self._field_id_mapping)
                )
            else:
                # TODO: Support ORC and AVRO when we can read it
                raise NotImplementedError(f"{file_format} for iceberg not implemented!")

            iceberg_delete_files = [f.file_path for f in task.delete_files]

            # TODO: Thread in Statistics to each ScanTask: P2
            pspec = self._iceberg_record_to_partition_spec(self._table.specs()[file.spec_id], file.partition)
            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                num_rows=record_count,
                storage_config=self._storage_config,
                size_bytes=file.file_size_in_bytes,
                iceberg_delete_files=iceberg_delete_files,
                pushdowns=pushdowns,
                partition_values=pspec._recordbatch if pspec is not None else None,
                stats=None,
            )
            if st is None:
                continue
            rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def _create_count_scan_task(self, pushdowns: PyPushdowns, field_name: str) -> Iterator[ScanTask]:
        """Create count pushdown scan task using Iceberg metadata."""
        try:
            total_count = self._calculate_total_rows_from_metadata()
            result_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(field_name, pa.uint64())]))

            logger.info("Created Iceberg count pushdown task with total_count=%d for field=%s", total_count, field_name)
            yield ScanTask.python_factory_func_scan_task(
                module=_iceberg_count_result_function.__module__,
                func_name=_iceberg_count_result_function.__name__,
                func_args=(total_count, field_name),
                schema=result_schema._schema,
                num_rows=1,
                size_bytes=8,
                pushdowns=pushdowns,
                stats=None,
            )
        except Exception as e:
            logger.error("Failed to create Iceberg count pushdown task: %s, now falling back to regular scan", e)
            yield from self._create_regular_scan_tasks(pushdowns)

    def _calculate_total_rows_from_metadata(self) -> int:
        """Calculate total row count from Iceberg manifest metadata.

        This method reads the manifest files to aggregate record_count information
        from all data files without accessing the actual data.
        """
        try:
            iceberg_tasks = self._table.scan(limit=None, snapshot_id=self._snapshot_id).plan_files()

            total_rows = 0
            total_deleted = 0

            # Aggregate row counts from all data files
            for task in iceberg_tasks:
                data_file = task.file
                total_rows += data_file.record_count

                # Handle delete files (for Iceberg MOR - Merge-on-Read)
                for delete_file in task.delete_files:
                    # For now, we'll use a simple estimation for delete files
                    # In a production implementation, this could be more sophisticated
                    total_deleted += delete_file.record_count

            final_count = max(0, total_rows - total_deleted)
            logger.info(
                "Calculated Iceberg count from metadata: total_rows=%d, deleted_rows=%d, final_count=%d",
                total_rows,
                total_deleted,
                final_count,
            )
            return final_count
        except Exception as e:
            logger.error("Failed to calculate total rows from Iceberg metadata: %s", e)
            raise

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True

    def supports_count_pushdown(self) -> bool:
        return True

    def supported_count_modes(self) -> list[CountMode]:
        return [CountMode.All]
