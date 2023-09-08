from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

from daft import col
from daft.daft import CountMode, FileFormat, FileFormatConfig, FileInfos, JoinType
from daft.daft import LogicalPlanBuilder as _LogicalPlanBuilder
from daft.daft import PartitionScheme, PartitionSpec, ResourceRequest, StorageConfig
from daft.expressions.expressions import Expression
from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionCacheEntry

if TYPE_CHECKING:
    from daft.planner.rust_planner import RustPhysicalPlanScheduler


class RustLogicalPlanBuilder(LogicalPlanBuilder):
    """Wrapper class for the new LogicalPlanBuilder in Rust."""

    def __init__(self, builder: _LogicalPlanBuilder) -> None:
        self._builder = builder

    def to_physical_plan_scheduler(self) -> RustPhysicalPlanScheduler:
        from daft.planner.rust_planner import RustPhysicalPlanScheduler

        return RustPhysicalPlanScheduler(self._builder.to_physical_plan_scheduler())

    def schema(self) -> Schema:
        pyschema = self._builder.schema()
        return Schema._from_pyschema(pyschema)

    def partition_spec(self) -> PartitionSpec:
        # TODO(Clark): Push PartitionSpec into planner.
        return self._builder.partition_spec()

    def pretty_print(self, simple: bool = False) -> str:
        if simple:
            return self._builder.repr_ascii(simple=True)
        else:
            return repr(self)

    def __repr__(self) -> str:
        return self._builder.repr_ascii(simple=False)

    def optimize(self) -> RustLogicalPlanBuilder:
        builder = self._builder.optimize()
        return RustLogicalPlanBuilder(builder)

    @classmethod
    def from_in_memory_scan(
        cls, partition: PartitionCacheEntry, schema: Schema, partition_spec: PartitionSpec | None = None
    ) -> RustLogicalPlanBuilder:
        if partition_spec is None:
            partition_spec = PartitionSpec(scheme=PartitionScheme.Unknown, num_partitions=1)
        builder = _LogicalPlanBuilder.in_memory_scan(partition.key, partition, schema._schema, partition_spec)
        return cls(builder)

    @classmethod
    def from_tabular_scan(
        cls,
        *,
        file_infos: FileInfos,
        schema: Schema,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
    ) -> RustLogicalPlanBuilder:
        builder = _LogicalPlanBuilder.table_scan(file_infos, schema._schema, file_format_config, storage_config)
        return cls(builder)

    def project(
        self,
        projection: list[Expression],
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> RustLogicalPlanBuilder:
        projection_pyexprs = [expr._expr for expr in projection]
        builder = self._builder.project(projection_pyexprs, custom_resource_request)
        return RustLogicalPlanBuilder(builder)

    def filter(self, predicate: Expression) -> RustLogicalPlanBuilder:
        builder = self._builder.filter(predicate._expr)
        return RustLogicalPlanBuilder(builder)

    def limit(self, num_rows: int) -> RustLogicalPlanBuilder:
        builder = self._builder.limit(num_rows)
        return RustLogicalPlanBuilder(builder)

    def explode(self, explode_expressions: list[Expression]) -> RustLogicalPlanBuilder:
        explode_pyexprs = [expr._expr for expr in explode_expressions]
        builder = self._builder.explode(explode_pyexprs)
        return RustLogicalPlanBuilder(builder)

    def count(self) -> RustLogicalPlanBuilder:
        # TODO(Clark): Add dedicated logical/physical ops when introducing metadata-based count optimizations.
        first_col = col(self.schema().column_names()[0])
        builder = self._builder.aggregate([first_col._count(CountMode.All)._expr], [])
        builder = builder.project([first_col.alias("count")._expr], ResourceRequest())
        return RustLogicalPlanBuilder(builder)

    def distinct(self) -> RustLogicalPlanBuilder:
        builder = self._builder.distinct()
        return RustLogicalPlanBuilder(builder)

    def sort(self, sort_by: list[Expression], descending: list[bool] | bool = False) -> RustLogicalPlanBuilder:
        sort_by_pyexprs = [expr._expr for expr in sort_by]
        if not isinstance(descending, list):
            descending = [descending] * len(sort_by_pyexprs)
        builder = self._builder.sort(sort_by_pyexprs, descending)
        return RustLogicalPlanBuilder(builder)

    def repartition(
        self, num_partitions: int, partition_by: list[Expression], scheme: PartitionScheme
    ) -> RustLogicalPlanBuilder:
        partition_by_pyexprs = [expr._expr for expr in partition_by]
        builder = self._builder.repartition(num_partitions, partition_by_pyexprs, scheme)
        return RustLogicalPlanBuilder(builder)

    def coalesce(self, num_partitions: int) -> RustLogicalPlanBuilder:
        if num_partitions > self.num_partitions():
            raise ValueError(
                f"Coalesce can only reduce the number of partitions: {num_partitions} vs {self.num_partitions}"
            )
        builder = self._builder.coalesce(num_partitions)
        return RustLogicalPlanBuilder(builder)

    def agg(
        self,
        to_agg: list[tuple[Expression, str]],
        group_by: list[Expression] | None,
    ) -> RustLogicalPlanBuilder:
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
        return RustLogicalPlanBuilder(builder)

    def join(  # type: ignore[override]
        self,
        right: RustLogicalPlanBuilder,
        left_on: list[Expression],
        right_on: list[Expression],
        how: JoinType = JoinType.Inner,
    ) -> RustLogicalPlanBuilder:
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
            return RustLogicalPlanBuilder(builder)
        else:
            raise NotImplementedError(f"{how} join not implemented.")

    def concat(self, other: RustLogicalPlanBuilder) -> RustLogicalPlanBuilder:  # type: ignore[override]
        builder = self._builder.concat(other._builder)
        return RustLogicalPlanBuilder(builder)

    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: list[Expression] | None = None,
        compression: str | None = None,
    ) -> RustLogicalPlanBuilder:
        if file_format != FileFormat.Csv and file_format != FileFormat.Parquet:
            raise ValueError(f"Writing is only supported for Parquet and CSV file formats, but got: {file_format}")
        part_cols_pyexprs = [expr._expr for expr in partition_cols] if partition_cols is not None else None
        builder = self._builder.table_write(str(root_dir), file_format, part_cols_pyexprs, compression)
        return RustLogicalPlanBuilder(builder)
