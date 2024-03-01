from __future__ import annotations

import math
from collections.abc import Iterator

from daft.context import get_context
from daft.daft import (
    DatabaseSourceConfig,
    FileFormatConfig,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema
from daft.sql.sql_reader import SQLReader


class SQLScanOperator(ScanOperator):
    def __init__(
        self,
        sql: str,
        url: str,
        storage_config: StorageConfig,
        num_partitions: int | None = None,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.url = url
        self.storage_config = storage_config
        self._num_partitions = num_partitions
        self._initialize_schema_and_features()

    def _initialize_schema_and_features(self) -> None:
        self._schema, self._limit_and_offset_supported, self._apply_limit_before_offset = self._attempt_schema_read()

    def _attempt_schema_read(self) -> tuple[Schema, bool, bool]:
        for apply_limit_before_offset in [True, False]:
            try:
                pa_table = SQLReader(
                    self.sql, self.url, limit=1, offset=0, apply_limit_before_offset=apply_limit_before_offset
                ).read()
                schema = Schema.from_pyarrow_schema(pa_table.schema)
                return schema, True, apply_limit_before_offset
            except Exception:
                continue

        # If both attempts fail, read without limit and offset
        pa_table = SQLReader(self.sql, self.url).read()
        schema = Schema.from_pyarrow_schema(pa_table.schema)
        return schema, False, False

    def _get_num_rows(self) -> int:
        pa_table = SQLReader(
            self.sql,
            self.url,
            projection=["COUNT(*)"],
        ).read()
        return pa_table.column(0)[0].as_py()

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"SQLScanOperator(sql={self.sql}, url={self.url})"

    def partitioning_keys(self) -> list[PartitionField]:
        return []

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        total_rows = self._get_num_rows()
        estimate_row_size_bytes = self.schema().estimate_row_size_bytes()
        total_size = total_rows * estimate_row_size_bytes

        if not self._limit_and_offset_supported:
            # If limit and offset are not supported, then we can't parallelize the scan, so we just return a single scan task
            file_format_config = FileFormatConfig.from_database_config(DatabaseSourceConfig(self.sql))
            return iter(
                [
                    ScanTask.sql_scan_task(
                        url=self.url,
                        file_format=file_format_config,
                        schema=self._schema._schema,
                        num_rows=total_rows,
                        storage_config=self.storage_config,
                        size_bytes=math.ceil(total_size),
                        pushdowns=pushdowns,
                    )
                ]
            )

        num_scan_tasks = (
            math.ceil(total_size / get_context().daft_execution_config.read_sql_partition_size_bytes)
            if self._num_partitions is None
            else self._num_partitions
        )
        num_rows_per_scan_task = total_rows // num_scan_tasks
        num_scan_tasks_with_extra_row = total_rows % num_scan_tasks

        scan_tasks = []
        offset = 0
        for i in range(num_scan_tasks):
            limit = num_rows_per_scan_task
            if i < num_scan_tasks_with_extra_row:
                limit += 1
            file_format_config = FileFormatConfig.from_database_config(
                DatabaseSourceConfig(
                    self.sql, limit=limit, offset=offset, apply_limit_before_offset=self._apply_limit_before_offset
                )
            )
            scan_tasks.append(
                ScanTask.sql_scan_task(
                    url=self.url,
                    file_format=file_format_config,
                    schema=self._schema._schema,
                    num_rows=limit,
                    storage_config=self.storage_config,
                    size_bytes=math.ceil(limit * estimate_row_size_bytes),
                    pushdowns=pushdowns,
                )
            )
            offset += limit

        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def can_partition_read(self) -> bool:
        return self._limit_and_offset_supported
