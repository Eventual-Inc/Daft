from __future__ import annotations

import logging
from collections.abc import Iterator

import daft
from daft.daft import (
    FileFormatConfig,
    ParquetSourceConfig,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.filesystem import _resolve_paths_and_filesystem, join_path
from daft.hudi.pyhudi.table import HUDI_METAFIELD_PARTITION_PATH, HudiTable, HudiTableMetadata
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema

logger = logging.getLogger(__name__)


class HudiScanOperator(ScanOperator):
    def __init__(self, table_uri: str, storage_config: StorageConfig) -> None:
        super().__init__()
        resolved_path, self._resolved_fs = _resolve_paths_and_filesystem(table_uri, storage_config.config.io_config)
        self._table = HudiTable(table_uri, self._resolved_fs, resolved_path[0])
        self._storage_config = storage_config
        self._schema = Schema.from_pyarrow_schema(self._table.schema)
        partition_fields = set(self._table.props.partition_fields)
        self._partition_keys = [
            PartitionField(field._field) for field in self._schema if field.name in partition_fields
        ]

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"HudiScanOperator({self._table.props.name})"

    def partitioning_keys(self) -> list[PartitionField]:
        return self._partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self.partitioning_keys()}",
            f"Storage config = {self._storage_config}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        import pyarrow as pa

        hudi_table_metadata: HudiTableMetadata = self._table.latest_table_metadata()
        files_metadata = hudi_table_metadata.files_metadata

        if len(self.partitioning_keys()) > 0 and pushdowns.partition_filters is None:
            logging.warning(
                "%s has partitioning keys = %s, but no partition filter was specified. This will result in a full table scan.",
                self.display_name(),
                self.partitioning_keys(),
            )

        limit_files = pushdowns.limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None
        rows_left = pushdowns.limit if pushdowns.limit is not None else 0
        scan_tasks = []
        for task_idx in range(files_metadata.num_rows):
            if limit_files and rows_left <= 0:
                break

            path = join_path(self._resolved_fs, self._table.table_uri, files_metadata["path"][task_idx].as_py())
            record_count = files_metadata["num_records"][task_idx].as_py()
            try:
                size_bytes = files_metadata["size_bytes"][task_idx].as_py()
            except KeyError:
                size_bytes = None
            file_format_config = FileFormatConfig.from_parquet_config(ParquetSourceConfig())

            if self._table.supports_partition_values:
                try:
                    arrow_arr = pa.array([files_metadata["partition_path"][task_idx]], type=pa.string())
                except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                    # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                    arrow_arr = pa.array([files_metadata["partition_path"][task_idx].as_py()], type=pa.string())
                partition_paths = {
                    HUDI_METAFIELD_PARTITION_PATH: daft.Series.from_arrow(arrow_arr, HUDI_METAFIELD_PARTITION_PATH),
                }
                partition_values = daft.table.Table.from_pydict(partition_paths)._table
            else:
                partition_values = None

            # Populate scan task with column-wise stats.
            # TODO: nested fields are skipped
            colstats_schema = hudi_table_metadata.colstats_min_values.schema
            min_values = hudi_table_metadata.colstats_min_values
            max_values = hudi_table_metadata.colstats_max_values
            arrays = {}
            for field_idx in range(len(colstats_schema)):
                field_name = colstats_schema.field(field_idx).name
                field_type = colstats_schema.field(field_idx).type
                try:
                    arrow_arr = pa.array(
                        [min_values[field_name][task_idx], max_values[field_name][task_idx]], type=field_type
                    )
                except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                    # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                    arrow_arr = pa.array(
                        [min_values[field_name][task_idx].as_py(), max_values[field_name][task_idx].as_py()],
                        type=field_type,
                    )
                arrays[field_name] = daft.Series.from_arrow(arrow_arr, field_name)
            stats = daft.table.Table.from_pydict(arrays)._table

            st = ScanTask.catalog_scan_task(
                file=path,
                file_format=file_format_config,
                schema=self._schema._schema,
                storage_config=self._storage_config,
                num_rows=record_count,
                size_bytes=size_bytes,
                iceberg_delete_files=None,
                pushdowns=pushdowns,
                partition_values=partition_values,
                stats=stats,
            )
            if st is None:
                continue
            rows_left -= record_count
            scan_tasks.append(st)
        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True
