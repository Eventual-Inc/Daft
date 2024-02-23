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
from daft.utils import execute_sql_query_to_pyarrow


class SQLScanOperator(ScanOperator):
    MIN_ROWS_PER_SCAN_TASK = 50  # Would be better to have a memory limit instead of a row limit

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
        self._limit_supported = self._check_limit_supported()
        self._schema = self._get_schema()

    def _check_limit_supported(self) -> bool:
        try:
            execute_sql_query_to_pyarrow(f"SELECT * FROM ({self.sql}) AS subquery LIMIT 1 OFFSET 0", self.url)
            return True
        except Exception:
            return False

    def _get_schema(self) -> Schema:
        sql = f"SELECT * FROM ({self.sql}) AS subquery"
        if self._limit_supported:
            sql += " LIMIT 1 OFFSET 0"

        pa_table = execute_sql_query_to_pyarrow(sql, self.url)
        return Schema.from_pyarrow_schema(pa_table.schema)

    def _get_num_rows(self) -> int:
        sql = f"SELECT COUNT(*) FROM ({self.sql}) AS subquery"
        pa_table = execute_sql_query_to_pyarrow(sql, self.url)
        return pa_table.column(0)[0].as_py()

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"SQLScanOperator({self.sql})"

    def partitioning_keys(self) -> list[PartitionField]:
        return []

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        if not self._limit_supported:
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

        total_rows = self._get_num_rows()
        num_scan_tasks = math.ceil(total_rows / self.MIN_ROWS_PER_SCAN_TASK)
        num_rows_per_scan_task = total_rows // num_scan_tasks

        scan_tasks = []
        offset = 0
        for _ in range(num_scan_tasks):
            limit = max(num_rows_per_scan_task, total_rows - offset)
            file_format_config = FileFormatConfig.from_database_config(
                DatabaseSourceConfig(self.sql, limit=limit, offset=offset)
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
        return True
