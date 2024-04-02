from __future__ import annotations

import logging
import math
import warnings
from collections.abc import Iterator
from enum import Enum, auto
from typing import Any, Callable

from sqlalchemy.engine import Connection

from daft.context import get_context
from daft.daft import (
    DatabaseSourceConfig,
    FileFormatConfig,
    Pushdowns,
    ScanTask,
    StorageConfig,
)
from daft.expressions.expressions import lit
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema
from daft.sql.sql_reader import SQLReader, get_db_scheme_from_url
from daft.table import Table

logger = logging.getLogger(__name__)


class PartitionBoundStrategy(Enum):
    PERCENTILE = auto()
    MIN_MAX = auto()


class SQLScanOperator(ScanOperator):
    def __init__(
        self,
        sql: str,
        url: str,
        storage_config: StorageConfig,
        sql_alchemy_conn: Callable[[], Connection] | None,
        partition_col: str | None = None,
        num_partitions: int | None = None,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.url = url
        self.storage_config = storage_config
        self._sql_alchemy_conn = sql_alchemy_conn
        self._partition_col = partition_col
        self._num_partitions = num_partitions
        self._schema = self._attempt_schema_read()

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
        num_scan_tasks = (
            math.ceil(total_size / get_context().daft_execution_config.read_sql_partition_size_bytes)
            if self._num_partitions is None
            else self._num_partitions
        )

        if num_scan_tasks == 1 or self._partition_col is None:
            return self._single_scan_task(pushdowns, total_rows, total_size)

        partition_bounds, strategy = self._get_partition_bounds_and_strategy(num_scan_tasks)
        partition_bounds_sql = [lit(bound)._to_sql(get_db_scheme_from_url(self.url)) for bound in partition_bounds]

        if any(bound is None for bound in partition_bounds_sql):
            warnings.warn("Unable to partion the data using the specified column. Falling back to a single scan task.")
            return self._single_scan_task(pushdowns, total_rows, total_size)

        size_bytes = math.ceil(total_size / num_scan_tasks) if strategy == PartitionBoundStrategy.PERCENTILE else None
        scan_tasks = []
        for i in range(num_scan_tasks):
            left_clause = f"{self._partition_col} >= {partition_bounds_sql[i]}"
            right_clause = (
                f"{self._partition_col} {'<' if i < num_scan_tasks - 1 else '<='} {partition_bounds_sql[i + 1]}"
            )
            sql = f"SELECT * FROM ({self.sql}) AS subquery WHERE {left_clause} AND {right_clause}"
            stats = Table.from_pydict({self._partition_col: [partition_bounds[i], partition_bounds[i + 1]]})
            file_format_config = FileFormatConfig.from_database_config(
                DatabaseSourceConfig(sql=sql, sql_alchemy_conn=self._sql_alchemy_conn)
            )

            scan_tasks.append(
                ScanTask.sql_scan_task(
                    url=self.url,
                    file_format=file_format_config,
                    schema=self._schema._schema,
                    num_rows=None,
                    storage_config=self.storage_config,
                    size_bytes=size_bytes,
                    pushdowns=pushdowns,
                    stats=stats._table,
                )
            )

        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def _attempt_schema_read(self) -> Schema:
        pa_table = SQLReader(self.sql, self.url, self._sql_alchemy_conn, limit=1).read()
        schema = Schema.from_pyarrow_schema(pa_table.schema)
        return schema

    def _get_num_rows(self) -> int:
        pa_table = SQLReader(
            self.sql,
            self.url,
            self._sql_alchemy_conn,
            projection=["COUNT(*)"],
        ).read()

        if pa_table.num_rows != 1:
            raise RuntimeError(
                "Failed to get the number of rows: COUNT(*) query returned an unexpected number of rows."
            )
        if pa_table.num_columns != 1:
            raise RuntimeError(
                "Failed to get the number of rows: COUNT(*) query returned an unexpected number of columns."
            )

        return pa_table.column(0)[0].as_py()

    def _attempt_partition_bounds_read(self, num_scan_tasks: int) -> tuple[Any, PartitionBoundStrategy]:
        try:
            # Try to get percentiles using percentile_cont
            percentiles = [i / num_scan_tasks for i in range(num_scan_tasks + 1)]
            pa_table = SQLReader(
                self.sql,
                self.url,
                self._sql_alchemy_conn,
                projection=[
                    f"percentile_cont({percentile}) WITHIN GROUP (ORDER BY {self._partition_col}) AS bound_{i}"
                    for i, percentile in enumerate(percentiles)
                ],
            ).read()
            return pa_table, PartitionBoundStrategy.PERCENTILE

        except RuntimeError as e:
            # If percentiles fails, use the min and max of the partition column
            logger.info("Failed to get percentiles using percentile_cont, falling back to min and max. Error: %s", e)

            pa_table = SQLReader(
                self.sql,
                self.url,
                self._sql_alchemy_conn,
                projection=[f"MIN({self._partition_col}) AS min", f"MAX({self._partition_col}) AS max"],
            ).read()
            return pa_table, PartitionBoundStrategy.MIN_MAX

    def _get_partition_bounds_and_strategy(self, num_scan_tasks: int) -> tuple[list[Any], PartitionBoundStrategy]:
        if self._partition_col is None:
            raise ValueError("Failed to get partition bounds: partition_col must be specified to partition the data.")

        if not (
            self._schema[self._partition_col].dtype._is_temporal_type()
            or self._schema[self._partition_col].dtype._is_numeric_type()
        ):
            raise ValueError(
                f"Failed to get partition bounds: {self._partition_col} is not a numeric or temporal type, and cannot be used for partitioning."
            )

        pa_table, strategy = self._attempt_partition_bounds_read(num_scan_tasks)

        if pa_table.num_rows != 1:
            raise RuntimeError(f"Failed to get partition bounds: expected 1 row, but got {pa_table.num_rows}.")

        if strategy == PartitionBoundStrategy.PERCENTILE:
            if pa_table.num_columns != num_scan_tasks + 1:
                raise RuntimeError(
                    f"Failed to get partition bounds: expected {num_scan_tasks + 1} percentiles, but got {pa_table.num_columns}."
                )

            pydict = Table.from_arrow(pa_table).to_pydict()
            assert pydict.keys() == {f"bound_{i}" for i in range(num_scan_tasks + 1)}
            bounds = [pydict[f"bound_{i}"][0] for i in range(num_scan_tasks + 1)]

        elif strategy == PartitionBoundStrategy.MIN_MAX:
            if pa_table.num_columns != 2:
                raise RuntimeError(
                    f"Failed to get partition bounds: expected 2 columns, but got {pa_table.num_columns}."
                )

            pydict = Table.from_arrow(pa_table).to_pydict()
            assert pydict.keys() == {"min", "max"}
            min_val = pydict["min"][0]
            max_val = pydict["max"][0]
            range_size = (max_val - min_val) / num_scan_tasks
            bounds = [min_val + range_size * i for i in range(num_scan_tasks)] + [max_val]

        return bounds, strategy

    def _single_scan_task(self, pushdowns: Pushdowns, total_rows: int | None, total_size: float) -> Iterator[ScanTask]:
        file_format_config = FileFormatConfig.from_database_config(
            DatabaseSourceConfig(self.sql, sql_alchemy_conn=self._sql_alchemy_conn)
        )
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
                    stats=None,
                )
            ]
        )
