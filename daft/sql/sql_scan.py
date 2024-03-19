from __future__ import annotations

import logging
import math
import warnings
from collections.abc import Iterator
from enum import Enum, auto
from typing import Any

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
from daft.sql.sql_reader import SQLReader

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
        partition_col: str | None = None,
        num_partitions: int | None = None,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.url = url
        self.storage_config = storage_config
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
        partition_bounds = [lit(bound)._to_sql() for bound in partition_bounds]

        if any(bound is None for bound in partition_bounds):
            warnings.warn("Unable to partion the data using the specified column. Falling back to a single scan task.")
            return self._single_scan_task(pushdowns, total_rows, total_size)

        size_bytes = math.ceil(total_size / num_scan_tasks) if strategy == PartitionBoundStrategy.PERCENTILE else None
        scan_tasks = []
        for i in range(num_scan_tasks):
            if i == 0:
                sql = f"SELECT * FROM ({self.sql}) AS subquery WHERE {self._partition_col} <= {partition_bounds[i]}"
            elif i == num_scan_tasks - 1:
                sql = f"SELECT * FROM ({self.sql}) AS subquery WHERE {self._partition_col} > {partition_bounds[i - 1]}"
            else:
                sql = f"SELECT * FROM ({self.sql}) AS subquery WHERE {self._partition_col} > {partition_bounds[i - 1]} AND {self._partition_col} <= {partition_bounds[i]}"

            file_format_config = FileFormatConfig.from_database_config(DatabaseSourceConfig(sql=sql))

            scan_tasks.append(
                ScanTask.sql_scan_task(
                    url=self.url,
                    file_format=file_format_config,
                    schema=self._schema._schema,
                    num_rows=None,
                    storage_config=self.storage_config,
                    size_bytes=size_bytes,
                    pushdowns=pushdowns,
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
        pa_table = SQLReader(self.sql, self.url, limit=1).read()
        schema = Schema.from_pyarrow_schema(pa_table.schema)
        return schema

    def _get_num_rows(self) -> int:
        pa_table = SQLReader(
            self.sql,
            self.url,
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
            percentiles = [i / num_scan_tasks for i in range(1, num_scan_tasks)]
            pa_table = SQLReader(
                self.sql,
                self.url,
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
                projection=[f"MIN({self._partition_col})", f"MAX({self._partition_col})"],
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
            if pa_table.num_columns != num_scan_tasks - 1:
                raise RuntimeError(
                    f"Failed to get partition bounds: expected {num_scan_tasks - 1} percentiles, but got {pa_table.num_columns}."
                )

            bounds = [pa_table.column(i)[0].as_py() for i in range(num_scan_tasks - 1)]

        elif strategy == PartitionBoundStrategy.MIN_MAX:
            if pa_table.num_columns != 2:
                raise RuntimeError(
                    f"Failed to get partition bounds: expected 2 columns, but got {pa_table.num_columns}."
                )

            min_val = pa_table.column(0)[0].as_py()
            max_val = pa_table.column(1)[0].as_py()
            range_size = (max_val - min_val) / num_scan_tasks
            bounds = [min_val + range_size * i for i in range(1, num_scan_tasks)]

        return bounds, strategy

    def _single_scan_task(self, pushdowns: Pushdowns, total_rows: int | None, total_size: float) -> Iterator[ScanTask]:
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
