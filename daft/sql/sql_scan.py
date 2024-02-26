from __future__ import annotations

import math
from collections.abc import Iterator

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
    MIN_ROWS_PER_SCAN_TASK = 50  # TODO: Would be better to have a memory limit instead of a row limit

    def __init__(
        self,
        sql: str,
        url: str,
        storage_config: StorageConfig,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.url = url
        self.storage_config = storage_config
        self._limit_and_offset_supported, self._limit_before_offset = self._check_limit_and_offset_supported()
        self._schema = self._get_schema()

    def _check_limit_and_offset_supported(self) -> tuple[bool, bool]:
        try:
            # Try to read 1 row with limit before offset
            SQLReader(self.sql, self.url, limit=1, offset=0, limit_before_offset=True).read()
            return (True, True)
        except Exception:
            try:
                # Try to read 1 row with limit after offset
                SQLReader(self.sql, self.url, offset=0, limit=1, limit_before_offset=False).read()
                return (True, False)
            except Exception:
                # If both fail, then limit and offset are not supported
                return (False, False)

    def _get_schema(self) -> Schema:
        if self._limit_and_offset_supported:
            pa_table = SQLReader(
                self.sql, self.url, limit=1, offset=0, limit_before_offset=self._limit_before_offset
            ).read()
        else:
            pa_table = SQLReader(self.sql, self.url).read()
        return Schema.from_pyarrow_schema(pa_table.schema)

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
        if not self._limit_and_offset_supported:
            # If limit and offset are not supported, then we can't parallelize the scan, so we just return a single scan task
            file_format_config = FileFormatConfig.from_database_config(DatabaseSourceConfig(self.sql))
            return iter(
                [
                    ScanTask.sql_scan_task(
                        url=self.url,
                        file_format=file_format_config,
                        schema=self._schema._schema,
                        storage_config=self.storage_config,
                        pushdowns=pushdowns,
                    )
                ]
            )

        total_rows = SQLReader(self.sql, self.url).get_num_rows()
        num_scan_tasks = math.ceil(total_rows / self.MIN_ROWS_PER_SCAN_TASK)
        num_rows_per_scan_task = total_rows // num_scan_tasks

        scan_tasks = []
        offset = 0
        for _ in range(num_scan_tasks):
            limit = max(num_rows_per_scan_task, total_rows - offset)
            file_format_config = FileFormatConfig.from_database_config(
                DatabaseSourceConfig(
                    self.sql, limit=limit, offset=offset, limit_before_offset=self._limit_before_offset
                )
            )
            scan_tasks.append(
                ScanTask.sql_scan_task(
                    url=self.url,
                    file_format=file_format_config,
                    schema=self._schema._schema,
                    storage_config=self.storage_config,
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
