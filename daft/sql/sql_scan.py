from __future__ import annotations

import logging
import math
from collections.abc import Iterator

import pyarrow as pa
from sqlalchemy import create_engine, text

from daft.daft import (
    DatabaseSourceConfig,
    FileFormatConfig,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema

logger = logging.getLogger(__name__)


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
            with create_engine(self.url).connect() as connection:
                connection.execute(text(f"SELECT * FROM ({self.sql}) AS subquery LIMIT 1 OFFSET 0"))

            return True
        except Exception:
            return False

    def _get_schema(self) -> Schema:
        with create_engine(self.url).connect() as connection:
            sql = f"SELECT * FROM ({self.sql}) AS subquery"
            if self._limit_supported:
                sql += " LIMIT 1 OFFSET 0"

            result = connection.execute(text(sql))

            # Fetch the cursor from the result proxy to access column descriptions
            cursor = result.cursor

            rows = cursor.fetchall()
            columns = [column_description[0] for column_description in cursor.description]

            pydict = {column: [row[i] for row in rows] for i, column in enumerate(columns)}
            pa_table = pa.Table.from_pydict(pydict)

            return Schema.from_pyarrow_schema(pa_table.schema)

    def _get_num_rows(self) -> int:
        with create_engine(self.url).connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM ({self.sql}) AS subquery"))
            cursor = result.cursor
            return cursor.fetchone()[0]

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
            limit = min(num_rows_per_scan_task, total_rows - offset)
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
