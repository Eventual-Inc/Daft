from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING

from pyiceberg.schema import visit
from pyiceberg.table import ALWAYS_TRUE

import daft
from daft.daft import (
    CountMode,
    FileFormatConfig,
    ParquetSourceConfig,
    PyPartitionField,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
    StorageConfig,
)
from daft.dependencies import pa
from daft.io.iceberg._expressions import convert_expression_to_iceberg
from daft.io.iceberg._metadata import convert_iceberg_partition_spec, convert_iceberg_schema, resolve_iceberg_schema
from daft.io.iceberg.schema_field_id_mapping_visitor import SchemaFieldIdMappingVisitor
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.table import Table
    from pyiceberg.typedef import Record

logger = logging.getLogger(__name__)

# Thread-local storage for planning-phase scan metrics.
# Populated by _create_regular_scan_tasks() / _create_count_scan_task().
_tls: threading.local = threading.local()


def reset_iceberg_scan_metrics() -> None:
    """Clear planning-phase metrics accumulated since the last reset."""
    _tls.last_scan_metrics = {}


def get_iceberg_scan_metrics() -> dict[str, int | float]:
    """Return a copy of the planning-phase metrics from the most recent scan.

    Keys:
        files_listed_by_catalog  – files returned by plan_files() before pruning
        tasks_after_pruning      – ScanTasks created after catalog_scan_task() filtering
        planning_time_ms         – wall-clock ms spent in plan_files() + task creation
        bytes_scheduled          – sum of file_size_in_bytes across surviving tasks
        count_pushdown_used      – 1 if the metadata-only count path fired, else 0
    """
    return dict(getattr(_tls, "last_scan_metrics", {}))


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


class IcebergScanOperator(ScanOperator):
    def __init__(self, iceberg_table: Table, snapshot_id: int | None, storage_config: StorageConfig) -> None:
        super().__init__()
        self._table = iceberg_table
        self._snapshot_id = snapshot_id
        self._storage_config = storage_config

        iceberg_schema = resolve_iceberg_schema(iceberg_table.metadata, snapshot_id)
        self._field_id_mapping = visit(iceberg_schema, SchemaFieldIdMappingVisitor())
        self._schema = convert_iceberg_schema(iceberg_schema)

        partition_fields = convert_iceberg_partition_spec(iceberg_schema, self._table.spec())
        self._partition_keys = [pf._partition_field for pf in partition_fields]

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
        partition_fields = convert_iceberg_partition_spec(self._table.schema(), spec)
        arrays = dict()
        assert len(record) == len(partition_fields)
        for idx, pf in enumerate(partition_fields):
            field = pf.field
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

        # TODO: Use the correct Iceberg expression for the row filter
        # row_filter = ALWAYS_TRUE
        # if pushdowns.filters is not None:
        #     row_filter = convert_expression_to_iceberg(pushdowns.filters)

        _t0 = time.perf_counter()
        iceberg_tasks = list(self._table.scan(limit=limit, snapshot_id=self._snapshot_id).plan_files())
        _files_listed = len(iceberg_tasks)

        limit_files = limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None

        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logger.warning(
                "%s has Partitioning Keys: %s but no partition filter was specified. This will result in a full table scan.",
                self.display_name(),
                self.partitioning_keys(),
            )
        scan_tasks = []
        _bytes_scheduled = 0

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
            _bytes_scheduled += file.file_size_in_bytes or 0
            scan_tasks.append(st)

        _tls.last_scan_metrics = {
            "files_listed_by_catalog": _files_listed,
            "tasks_after_pruning": len(scan_tasks),
            "planning_time_ms": (time.perf_counter() - _t0) * 1000.0,
            "bytes_scheduled": _bytes_scheduled,
            "count_pushdown_used": 0,
        }
        return iter(scan_tasks)

    def _create_count_scan_task(self, pushdowns: PyPushdowns, field_name: str) -> Iterator[ScanTask]:
        """Create count pushdown scan task using Iceberg metadata."""
        try:
            iceberg_tasks = self._table.scan(limit=None, snapshot_id=self._snapshot_id).plan_files()
            total_count = 0

            # Aggregate row counts from all data files
            for task in iceberg_tasks:
                data_file = task.file
                total_count += data_file.record_count

            result_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(field_name, pa.uint64())]))

            logger.info("Created Iceberg count pushdown task with total_count=%d for field=%s", total_count, field_name)
            _tls.last_scan_metrics = {
                "files_listed_by_catalog": 0,
                "tasks_after_pruning": 0,
                "planning_time_ms": 0.0,
                "bytes_scheduled": 0,
                "count_pushdown_used": 1,
            }
            yield ScanTask.python_factory_func_scan_task(
                module=_iceberg_count_result_function.__module__,
                func_name=_iceberg_count_result_function.__name__,
                func_args=(total_count, field_name),
                schema=result_schema._schema,
                num_rows=1,
                size_bytes=8,
                pushdowns=pushdowns,
                stats=None,
                source_name=self.display_name(),
            )
        except Exception as e:
            logger.error("Failed to create Iceberg count pushdown task: %s, now falling back to regular scan", e)
            yield from self._create_regular_scan_tasks(pushdowns)

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
            iceberg_tasks = self._table.scan(
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
            logger.warning("Error checking for delete files: %s, disabling count pushdown as precaution", e)
            return True

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True

    def supports_count_pushdown(self) -> bool:
        return not self._has_delete_files()

    def supported_count_modes(self) -> list[CountMode]:
        return [CountMode.All]
