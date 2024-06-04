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
    PyTable,
    ScanTask,
    StorageConfig,
)
from daft.expressions.expressions import lit
from daft.io.scan import PartitionField, ScanOperator
from daft.logical.schema import Schema
from daft.sql.sql_connection import SQLConnection
from daft.table import Table

logger = logging.getLogger(__name__)


class PartitionBoundStrategy(Enum):
    PERCENTILE = auto()
    MIN_MAX = auto()


class SQLScanOperator(ScanOperator):
    def __init__(
        self,
        sql: str,
        conn: SQLConnection,
        storage_config: StorageConfig,
        disable_pushdowns_to_sql: bool,
        partition_col: str | None = None,
        num_partitions: int | None = None,
    ) -> None:
        super().__init__()
        self.sql = sql
        self.conn = conn
        self.storage_config = storage_config
        self._disable_pushdowns_to_sql = disable_pushdowns_to_sql
        self._partition_col = partition_col
        self._num_partitions = num_partitions
        self._schema = self._attempt_schema_read()

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"SQLScanOperator(sql={self.sql}, conn={self.conn})"

    def partitioning_keys(self) -> list[PartitionField]:
        return []

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
        ]

    def to_scan_tasks(self, pushdowns: Pushdowns) -> Iterator[ScanTask]:
        total_rows, total_size, num_scan_tasks = self._get_size_estimates()
        if num_scan_tasks == 1 or self._partition_col is None:
            return self._single_scan_task(pushdowns, total_rows, total_size)

        partition_bounds, strategy = self._get_partition_bounds_and_strategy(num_scan_tasks)
        partition_bounds_sql = [lit(bound)._to_sql() for bound in partition_bounds]

        if any(bound is None for bound in partition_bounds_sql):
            warnings.warn(
                "Unable to partition the data using the specified column. Falling back to a single scan task."
            )
            return self._single_scan_task(pushdowns, total_rows, total_size)

        size_bytes = math.ceil(total_size / num_scan_tasks) if strategy == PartitionBoundStrategy.PERCENTILE else None
        scan_tasks = []
        for i in range(num_scan_tasks):
            left_clause = f"{self._partition_col} >= {partition_bounds_sql[i]}"
            right_clause = (
                f"{self._partition_col} {'<' if i < num_scan_tasks - 1 else '<='} {partition_bounds_sql[i + 1]}"
            )
            stats = Table.from_pydict({self._partition_col: [partition_bounds[i], partition_bounds[i + 1]]})
            scan_task = self._construct_scan_task(
                pushdowns,
                num_rows=None,
                size_bytes=size_bytes,
                partition_bounds=(left_clause, right_clause),
                stats=stats._table,
            )
            scan_tasks.append(scan_task)

        return iter(scan_tasks)

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def _attempt_schema_read(self) -> Schema:
        sql = self._construct_sql_query(limit=1)
        pa_table = self.conn.read(sql)
        schema = Schema.from_pyarrow_schema(pa_table.schema)
        return schema

    def _get_size_estimates(self) -> tuple[int, float, int]:
        total_rows = self._get_num_rows()
        estimate_row_size_bytes = self.schema().estimate_row_size_bytes()
        total_size = total_rows * estimate_row_size_bytes
        num_scan_tasks = (
            math.ceil(total_size / get_context().daft_execution_config.read_sql_partition_size_bytes)
            if self._num_partitions is None
            else self._num_partitions
        )
        return total_rows, total_size, num_scan_tasks

    def _get_num_rows(self) -> int:
        sql = self._construct_sql_query(projection=["COUNT(*)"])
        pa_table = self.conn.read(sql)

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
            sql = self._construct_sql_query(
                projection=[
                    f"percentile_disc({percentile}) WITHIN GROUP (ORDER BY {self._partition_col}) AS bound_{i}"
                    for i, percentile in enumerate(percentiles)
                ]
            )
            pa_table = self.conn.read(sql)
            return pa_table, PartitionBoundStrategy.PERCENTILE

        except RuntimeError as e:
            # If percentiles fails, use the min and max of the partition column
            logger.info(
                "Failed to get percentiles using percentile_cont, falling back to min and max. Error: %s",
                e,
            )

            sql = self._construct_sql_query(
                projection=[
                    f"MIN({self._partition_col}) AS min",
                    f"MAX({self._partition_col}) AS max",
                ]
            )
            pa_table = self.conn.read(sql)
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
        return iter([self._construct_scan_task(pushdowns, num_rows=total_rows, size_bytes=math.ceil(total_size))])

    def _construct_scan_task(
        self,
        pushdowns: Pushdowns,
        num_rows: int | None = None,
        size_bytes: int | None = None,
        partition_bounds: tuple[str, str] | None = None,
        stats: PyTable | None = None,
    ) -> ScanTask:
        predicate_sql = pushdowns.filters.to_sql() if pushdowns.filters is not None else None
        apply_pushdowns_to_sql = not self._disable_pushdowns_to_sql and (
            pushdowns.filters is None or predicate_sql is not None
        )

        if apply_pushdowns_to_sql:
            sql = self._construct_sql_query(
                projection=pushdowns.columns,
                predicate=predicate_sql,
                limit=pushdowns.limit,
                partition_bounds=partition_bounds,
            )
        else:
            sql = self._construct_sql_query(partition_bounds=partition_bounds)

        file_format_config = FileFormatConfig.from_database_config(DatabaseSourceConfig(sql, self.conn))
        return ScanTask.sql_scan_task(
            url=self.conn.url,
            file_format=file_format_config,
            schema=self._schema._schema,
            num_rows=num_rows,
            storage_config=self.storage_config,
            size_bytes=size_bytes,
            pushdowns=pushdowns if not apply_pushdowns_to_sql else None,
            stats=stats,
        )

    def _construct_sql_query(
        self,
        projection: list[str] | None = None,
        predicate: str | None = None,
        limit: int | None = None,
        partition_bounds: tuple[str, str] | None = None,
    ) -> str:
        import sqlglot

        target_dialect = self.conn.dialect
        # sqlglot does not support "postgresql" dialect, it only supports "postgres"
        if target_dialect == "postgresql":
            target_dialect = "postgres"
        # sqlglot does not recognize "mssql" as a dialect, it instead recognizes "tsql", which is the SQL dialect for Microsoft SQL Server
        elif target_dialect == "mssql":
            target_dialect = "tsql"

        if not any(target_dialect == supported_dialect.value for supported_dialect in sqlglot.Dialects):
            raise ValueError(
                f"Unsupported dialect: {target_dialect}, please refer to the documentation for supported dialects."
            )

        query = sqlglot.subquery(self.sql, "subquery")

        if projection is not None:
            query = query.select(*projection)
        else:
            query = query.select("*")

        if predicate is not None:
            query = query.where(predicate)

        if partition_bounds is not None:
            query = query.where(partition_bounds[0]).where(partition_bounds[1])

        if limit is not None:
            query = query.limit(limit)

        return query.sql(dialect=target_dialect)
