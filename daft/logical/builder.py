from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

from daft.daft import (
    CountMode,
    FileFormat,
    FileFormatConfig,
    FileInfos,
    IOConfig,
    JoinType,
)
from daft.daft import LogicalPlanBuilder as _LogicalPlanBuilder
from daft.daft import (
    PartitionScheme,
    ResourceRequest,
    ScanOperatorHandle,
    StorageConfig,
)
from daft.expressions import Expression, col
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionCacheEntry

if TYPE_CHECKING:
    from daft.plan_scheduler.physical_plan_scheduler import PhysicalPlanScheduler


class LogicalPlanBuilder:
    """
    A logical plan builder for the Daft DataFrame.
    """

    def __init__(self, builder: _LogicalPlanBuilder) -> None:
        self._builder = builder

    def to_physical_plan_scheduler(self) -> PhysicalPlanScheduler:
        """
        Convert the underlying logical plan to a physical plan scheduler, which is
        used to generate executable tasks for the physical plan.

        This should be called after triggering optimization with self.optimize().
        """
        from daft.plan_scheduler.physical_plan_scheduler import PhysicalPlanScheduler

        return PhysicalPlanScheduler(self._builder.to_physical_plan_scheduler())

    def schema(self) -> Schema:
        """
        The schema of the current logical plan.
        """
        pyschema = self._builder.schema()
        return Schema._from_pyschema(pyschema)

    def pretty_print(self, simple: bool = False) -> str:
        """
        Pretty prints the current underlying logical plan.
        """
        if simple:
            return self._builder.repr_ascii(simple=True)
        else:
            return repr(self)

    def __repr__(self) -> str:
        return self._builder.repr_ascii(simple=False)

    def optimize(self) -> LogicalPlanBuilder:
        """
        Optimize the underlying logical plan.
        """
        builder = self._builder.optimize()
        return LogicalPlanBuilder(builder)

    @classmethod
    def from_in_memory_scan(
        cls, partition: PartitionCacheEntry, schema: Schema, num_partitions: int
    ) -> LogicalPlanBuilder:
        builder = _LogicalPlanBuilder.in_memory_scan(partition.key, partition, schema._schema, num_partitions)
        return cls(builder)

    @classmethod
    def from_tabular_scan_with_scan_operator(
        cls,
        *,
        scan_operator: ScanOperatorHandle,
        schema_hint: Schema | None,
    ) -> LogicalPlanBuilder:
        pyschema = schema_hint._schema if schema_hint is not None else None
        builder = _LogicalPlanBuilder.table_scan_with_scan_operator(scan_operator, pyschema)
        return cls(builder)

    @classmethod
    def from_tabular_scan(
        cls,
        *,
        file_infos: FileInfos,
        schema: Schema,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
    ) -> LogicalPlanBuilder:
        builder = _LogicalPlanBuilder.table_scan(file_infos, schema._schema, file_format_config, storage_config)
        return cls(builder)

    def project(
        self,
        projection: list[Expression],
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> LogicalPlanBuilder:
        projection_pyexprs = [expr._expr for expr in projection]
        builder = self._builder.project(projection_pyexprs, custom_resource_request)
        return LogicalPlanBuilder(builder)

    def filter(self, predicate: Expression) -> LogicalPlanBuilder:
        builder = self._builder.filter(predicate._expr)
        return LogicalPlanBuilder(builder)

    def limit(self, num_rows: int, eager: bool) -> LogicalPlanBuilder:
        builder = self._builder.limit(num_rows, eager)
        return LogicalPlanBuilder(builder)

    def explode(self, explode_expressions: list[Expression]) -> LogicalPlanBuilder:
        explode_pyexprs = [expr._expr for expr in explode_expressions]
        builder = self._builder.explode(explode_pyexprs)
        return LogicalPlanBuilder(builder)

    def count(self) -> LogicalPlanBuilder:
        # TODO(Clark): Add dedicated logical/physical ops when introducing metadata-based count optimizations.
        first_col = col(self.schema().column_names()[0])
        builder = self._builder.aggregate([first_col._count(CountMode.All)._expr], [])
        builder = builder.project([first_col.alias("count")._expr], ResourceRequest())
        return LogicalPlanBuilder(builder)

    def distinct(self) -> LogicalPlanBuilder:
        builder = self._builder.distinct()
        return LogicalPlanBuilder(builder)

    def sort(self, sort_by: list[Expression], descending: list[bool] | bool = False) -> LogicalPlanBuilder:
        sort_by_pyexprs = [expr._expr for expr in sort_by]
        if not isinstance(descending, list):
            descending = [descending] * len(sort_by_pyexprs)
        builder = self._builder.sort(sort_by_pyexprs, descending)
        return LogicalPlanBuilder(builder)

    def repartition(
        self, num_partitions: int | None, partition_by: list[Expression], scheme: PartitionScheme
    ) -> LogicalPlanBuilder:
        partition_by_pyexprs = [expr._expr for expr in partition_by]
        builder = self._builder.repartition(partition_by_pyexprs, scheme, num_partitions=num_partitions)
        return LogicalPlanBuilder(builder)

    def agg(
        self,
        to_agg: list[tuple[Expression, str]],
        group_by: list[Expression] | None,
    ) -> LogicalPlanBuilder:
        exprs = []
        for expr, op in to_agg:
            if op == "sum":
                exprs.append(expr._sum())
            elif op == "count":
                exprs.append(expr._count())
            elif op == "min":
                exprs.append(expr._min())
            elif op == "max":
                exprs.append(expr._max())
            elif op == "mean":
                exprs.append(expr._mean())
            elif op == "list":
                exprs.append(expr._agg_list())
            elif op == "concat":
                exprs.append(expr._agg_concat())
            else:
                raise NotImplementedError(f"Aggregation {op} is not implemented.")

        group_by_pyexprs = [expr._expr for expr in group_by] if group_by is not None else []
        builder = self._builder.aggregate([expr._expr for expr in exprs], group_by_pyexprs)
        return LogicalPlanBuilder(builder)

    def join(  # type: ignore[override]
        self,
        right: LogicalPlanBuilder,
        left_on: list[Expression],
        right_on: list[Expression],
        how: JoinType = JoinType.Inner,
    ) -> LogicalPlanBuilder:
        if how == JoinType.Left:
            raise NotImplementedError("Left join not implemented.")
        elif how == JoinType.Right:
            raise NotImplementedError("Right join not implemented.")
        elif how == JoinType.Inner:
            builder = self._builder.join(
                right._builder,
                [expr._expr for expr in left_on],
                [expr._expr for expr in right_on],
                how,
            )
            return LogicalPlanBuilder(builder)
        else:
            raise NotImplementedError(f"{how} join not implemented.")

    def concat(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:  # type: ignore[override]
        builder = self._builder.concat(other._builder)
        return LogicalPlanBuilder(builder)

    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: list[Expression] | None = None,
        compression: str | None = None,
        io_config: IOConfig | None = None,
    ) -> LogicalPlanBuilder:
        if file_format != FileFormat.Csv and file_format != FileFormat.Parquet:
            raise ValueError(f"Writing is only supported for Parquet and CSV file formats, but got: {file_format}")
        part_cols_pyexprs = [expr._expr for expr in partition_cols] if partition_cols is not None else None
        builder = self._builder.table_write(str(root_dir), file_format, part_cols_pyexprs, compression, io_config)
        return LogicalPlanBuilder(builder)
