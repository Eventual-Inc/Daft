from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import daft
from daft.expressions import ExpressionsProjection
from daft.filesystem import _resolve_paths_and_filesystem, join_path
from daft.io.hudi.pyhudi.table import HUDI_METAFIELD_PARTITION_PATH, HudiTable, HudiTableMetadata
from daft.io.partitioning import PartitionField
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft.daft import StorageConfig
    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)


class HudiDataSource(DataSource):
    def __init__(self, table_uri: str, storage_config: StorageConfig) -> None:
        resolved_path, self._resolved_fs = _resolve_paths_and_filesystem(table_uri, storage_config.io_config)
        self._table = HudiTable(table_uri, self._resolved_fs, resolved_path[0])
        self._storage_config = storage_config
        self._schema = Schema.from_pyarrow_schema(self._table.schema)
        partition_fields = set(self._table.props.partition_fields)
        self._partition_fields = [
            PartitionField.create(field) for field in self._schema if field.name in partition_fields
        ]

    @property
    def name(self) -> str:
        return f"HudiDataSource({self._table.props.name})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        return self._partition_fields

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        import pyarrow as pa

        hudi_table_metadata: HudiTableMetadata = self._table.latest_table_metadata()
        files_metadata = hudi_table_metadata.files_metadata

        if self._partition_fields and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partitioning keys = %s, but no partition filter was specified. This will result in a full table scan.",
                self.name,
                self._partition_fields,
            )

        limit_files = pushdowns.limit is not None and pushdowns.filters is None and pushdowns.partition_filters is None
        rows_left = pushdowns.limit if pushdowns.limit is not None else 0
        for task_idx in range(files_metadata.num_rows):
            if limit_files and rows_left <= 0:
                break

            path = join_path(self._resolved_fs, self._table.table_uri, files_metadata["path"][task_idx].as_py())
            record_count = files_metadata["num_records"][task_idx].as_py()
            try:
                size_bytes = files_metadata["size_bytes"][task_idx].as_py()
            except KeyError:
                size_bytes = None

            if self._table.supports_partition_values:
                try:
                    arrow_arr = pa.array([files_metadata["partition_path"][task_idx]], type=pa.string())
                except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                    # pyarrow < 13.0.0 doesn't accept pyarrow scalars in the array constructor.
                    arrow_arr = pa.array([files_metadata["partition_path"][task_idx].as_py()], type=pa.string())
                partition_paths = {
                    HUDI_METAFIELD_PARTITION_PATH: daft.Series.from_arrow(arrow_arr, HUDI_METAFIELD_PARTITION_PATH),
                }
                partition_values = daft.recordbatch.RecordBatch.from_pydict(partition_paths)
            else:
                partition_values = None

            # Partition pruning is the DataSource's responsibility: skip files whose
            # partition values do not match the pushed-down partition filters.
            if partition_values is not None and pushdowns.partition_filters is not None:
                filtered = partition_values.filter(ExpressionsProjection([pushdowns.partition_filters]))
                if len(filtered) == 0:
                    continue

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
            stats = daft.recordbatch.RecordBatch.from_pydict(arrays)

            task = DataSourceTask.parquet(
                path=path,
                schema=self._schema,
                pushdowns=pushdowns,
                num_rows=record_count,
                size_bytes=size_bytes,
                partition_values=partition_values,
                stats=stats,
                storage_config=self._storage_config,
            )
            rows_left -= record_count
            yield task
