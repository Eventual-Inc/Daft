from __future__ import annotations

import logging
import math
import warnings
from enum import Enum
from typing import TYPE_CHECKING, Any

from daft.context import get_context
from daft.daft import (
    DatabaseSourceConfig,
    FileFormatConfig,
    PyPartitionField,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
    StorageConfig,
)
from daft.expressions.expressions import lit
from daft.io.common import _get_schema_from_dict
from daft.io.scan import ScanOperator
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.datatype import DataType
    from daft.logical.schema import Schema
    from daft.sql.sql_connection import SQLConnection

logger = logging.getLogger(__name__)


class PartitionBoundStrategy(Enum):
    PERCENTILE = "percentile"
    MIN_MAX = "min-max"

    @classmethod
    def from_str(cls, value: str) -> PartitionBoundStrategy:
        try:
            return cls(value.lower())
        except ValueError:
            raise ValueError(f"Invalid PartitionBoundStrategy: {value}, must be either 'percentile' or 'min-max'")


class SQLScanOperator(ScanOperator):
    def __init__(
        self,
        sql: str,
        conn: SQLConnection,
        storage_config: StorageConfig,
        disable_pushdowns_to_sql: bool,
        infer_schema: bool,
        infer_schema_length: int,
        schema: dict[str, DataType] | None,
        partition_col: str | None = None,
        num_partitions: int | None = None,
        partition_bound_strategy: PartitionBoundStrategy | None = None,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.conn = conn
        self.storage_config = storage_config
        self._disable_pushdowns_to_sql = disable_pushdowns_to_sql
        self._partition_col = partition_col
        self._num_partitions = num_partitions
        self._partition_bound_strategy = partition_bound_strategy
        self._schema = self._attempt_schema_read(infer_schema, infer_schema_length, schema)

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "SQLScanOperator"

    def display_name(self) -> str:
        return f"SQLScanOperator(sql={self.sql}, conn={self.conn})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        total_rows, total_size, num_scan_tasks = self._get_size_estimates()
        if num_scan_tasks == 0:
            return iter(())
        if num_scan_tasks == 1 or self._partition_col is None:
            return self._single_scan_task(pushdowns, total_rows, total_size)

        partition_bounds = self._get_partition_bounds(num_scan_tasks)
        partition_bounds_sql = [lit(bound)._to_sql() for bound in partition_bounds]

        if any(bound is None for bound in partition_bounds_sql):
            warnings.warn(
                "Unable to partition the data using the specified column. Falling back to a single scan task."
            )
            return self._single_scan_task(pushdowns, total_rows, total_size)

        size_bytes = (
            math.ceil(total_size / num_scan_tasks)
            if self._partition_bound_strategy == PartitionBoundStrategy.PERCENTILE
            else None
        )
        scan_tasks = []
        for i in range(num_scan_tasks):
            left_clause = f"{self._partition_col} >= {partition_bounds_sql[i]}"
            right_clause = (
                f"{self._partition_col} {'<' if i < num_scan_tasks - 1 else '<='} {partition_bounds_sql[i + 1]}"
            )
            stats = RecordBatch.from_pydict({self._partition_col: [partition_bounds[i], partition_bounds[i + 1]]})
            scan_task = self._construct_scan_task(
                pushdowns,
                num_rows=None,
                size_bytes=size_bytes,
                partition_bounds=(left_clause, right_clause),
                stats=stats._recordbatch,
            )
            scan_tasks.append(scan_task)

        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def _attempt_schema_read(
        self,
        infer_schema: bool,
        infer_schema_length: int,
        schema: dict[str, DataType] | None,
    ) -> Schema:
        # If schema is provided and user turned off schema inference, use the provided schema
        if schema is not None and not infer_schema:
            return _get_schema_from_dict(schema)

        # Else, attempt schema inference then apply the schema hint if provided
        inferred_schema = self.conn.read_schema(self.sql, infer_schema_length)
        if schema is not None:
            return inferred_schema.apply_hints(_get_schema_from_dict(schema))
        return inferred_schema

    def _get_size_estimates(self) -> tuple[int, float, int]:
        total_rows = self._get_num_rows()
        estimate_row_size_bytes = self.schema().estimate_row_size_bytes()
        total_size = total_rows * estimate_row_size_bytes
        num_scan_tasks = (
            math.ceil(total_size / get_context().daft_execution_config.read_sql_partition_size_bytes)
            if self._num_partitions is None
            else self._num_partitions
        )
        num_scan_tasks = min(num_scan_tasks, total_rows)
        return total_rows, total_size, num_scan_tasks

    def _get_num_rows(self) -> int:
        num_rows_sql = self.conn.construct_sql_query(self.sql, projection=["COUNT(*)"])
        pa_table = self.conn.execute_sql_query(num_rows_sql)

        if pa_table.num_rows != 1:
            raise RuntimeError(
                "Failed to get the number of rows: COUNT(*) query returned an unexpected number of rows."
            )
        if pa_table.num_columns != 1:
            raise RuntimeError(
                "Failed to get the number of rows: COUNT(*) query returned an unexpected number of columns."
            )

        return pa_table.column(0)[0].as_py()

    def _get_partition_bounds(self, num_scan_tasks: int) -> list[Any]:
        if self._partition_col is None:
            raise ValueError("Failed to get partition bounds: partition_col must be specified to partition the data.")

        if not (
            self._schema[self._partition_col].dtype.is_temporal()
            or self._schema[self._partition_col].dtype.is_numeric()
        ):
            raise ValueError(
                f"Failed to get partition bounds: {self._partition_col} is not a numeric or temporal type, and cannot be used for partitioning."
            )

        if self._partition_bound_strategy == PartitionBoundStrategy.PERCENTILE:
            try:
                # Try to get percentiles using percentile_disc.
                # Favor percentile_disc over percentile_cont because we want exact values to do <= and >= comparisons.
                percentiles = [i / num_scan_tasks for i in range(num_scan_tasks + 1)]
                # Use the OVER clause for SQL Server dialects
                over_clause = "OVER ()" if self.conn.dialect in ["mssql", "tsql"] else ""
                percentile_sql = self.conn.construct_sql_query(
                    self.sql,
                    projection=[
                        f"percentile_disc({percentile}) WITHIN GROUP (ORDER BY {self._partition_col}) {over_clause} AS bound_{i}"
                        for i, percentile in enumerate(percentiles)
                    ],
                    limit=1,
                )
                pa_table = self.conn.execute_sql_query(percentile_sql)

                if pa_table.num_rows != 1:
                    raise RuntimeError(f"Expected 1 row, but got {pa_table.num_rows}.")

                if pa_table.num_columns != num_scan_tasks + 1:
                    raise RuntimeError(f"Expected {num_scan_tasks + 1} percentiles, but got {pa_table.num_columns}.")

                pydict = RecordBatch.from_arrow_table(pa_table).to_pydict()
                assert pydict.keys() == {f"bound_{i}" for i in range(num_scan_tasks + 1)}
                return [pydict[f"bound_{i}"][0] for i in range(num_scan_tasks + 1)]

            except Exception as e:
                warnings.warn(
                    f"Failed to calculate partition bounds for read_sql using percentile strategy: {e!s}. "
                    "Falling back to MIN_MAX strategy."
                )
                self._partition_bound_strategy = PartitionBoundStrategy.MIN_MAX

        # Either MIN_MAX was explicitly specified or percentile calculation failed
        min_max_sql = self.conn.construct_sql_query(
            self.sql, projection=[f"MIN({self._partition_col}) as min", f"MAX({self._partition_col}) as max"]
        )
        pa_table = self.conn.execute_sql_query(min_max_sql)

        if pa_table.num_rows != 1:
            raise RuntimeError(f"Failed to get partition bounds: expected 1 row, but got {pa_table.num_rows}.")
        if pa_table.num_columns != 2:
            raise RuntimeError(f"Failed to get partition bounds: expected 2 columns, but got {pa_table.num_columns}.")

        pydict = RecordBatch.from_arrow_table(pa_table).to_pydict()
        assert pydict.keys() == {"min", "max"}
        min_val = pydict["min"][0]
        max_val = pydict["max"][0]
        range_size = (max_val - min_val) / num_scan_tasks
        return [min_val + range_size * i for i in range(num_scan_tasks)] + [max_val]

    def _single_scan_task(
        self, pushdowns: PyPushdowns, total_rows: int | None, total_size: float
    ) -> Iterator[ScanTask]:
        return iter([self._construct_scan_task(pushdowns, num_rows=total_rows, size_bytes=math.ceil(total_size))])

    def _construct_scan_task(
        self,
        pushdowns: PyPushdowns,
        num_rows: int | None = None,
        size_bytes: int | None = None,
        partition_bounds: tuple[str, str] | None = None,
        stats: PyRecordBatch | None = None,
    ) -> ScanTask:
        predicate_sql = pushdowns.filters.to_sql() if pushdowns.filters is not None else None
        apply_pushdowns_to_sql = not self._disable_pushdowns_to_sql and (
            pushdowns.filters is None or predicate_sql is not None
        )

        if apply_pushdowns_to_sql:
            sql = self.conn.construct_sql_query(
                self.sql,
                projection=pushdowns.columns,
                predicate=predicate_sql,
                limit=pushdowns.limit,
                partition_bounds=partition_bounds,
            )
        else:
            sql = self.conn.construct_sql_query(self.sql, partition_bounds=partition_bounds)

        file_format_config = FileFormatConfig.from_database_config(DatabaseSourceConfig(sql, self.conn))

        # keep column pushdowns because they are used for deriving the materialized schema
        remaining_pushdowns = pushdowns if not apply_pushdowns_to_sql else PyPushdowns(columns=pushdowns.columns)

        return ScanTask.sql_scan_task(
            url=self.conn.url,
            file_format=file_format_config,
            schema=self._schema._schema,
            storage_config=self.storage_config,
            num_rows=num_rows,
            size_bytes=size_bytes,
            pushdowns=remaining_pushdowns,
            stats=stats,
        )
