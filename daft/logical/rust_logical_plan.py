from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import fsspec

from daft import DataType
from daft.context import get_context
from daft.daft import FileFormat, FileFormatConfig
from daft.daft import LogicalPlanBuilder as _LogicalPlanBuilder
from daft.daft import PartitionScheme, PartitionSpec
from daft.errors import ExpressionTypeError
from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.logical.builder import JoinType, LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.resource_request import ResourceRequest
from daft.runners.partitioning import PartitionCacheEntry

if TYPE_CHECKING:
    from daft.planner.rust_planner import RustQueryPlanner


class RustLogicalPlanBuilder(LogicalPlanBuilder):
    """Wrapper class for the new LogicalPlanBuilder in Rust."""

    def __init__(self, builder: _LogicalPlanBuilder) -> None:
        self._builder = builder

    def to_planner(self) -> RustQueryPlanner:
        from daft.planner.rust_planner import RustQueryPlanner

        return RustQueryPlanner(self._builder)

    def schema(self) -> Schema:
        pyschema = self._builder.schema()
        return Schema._from_pyschema(pyschema)

    def partition_spec(self) -> PartitionSpec:
        # TODO(Clark): Push PartitionSpec into planner.
        return self._builder.partition_spec()

    def resource_request(self) -> ResourceRequest:
        # TODO(Clark): Expose resource request via builder, or push it into the planner.
        return ResourceRequest()

    def pretty_print(self) -> str:
        return repr(self)

    def __repr__(self) -> str:
        return self._builder.repr_ascii()

    def optimize(self) -> RustLogicalPlanBuilder:
        # TODO(Clark): Add optimization framework.
        return self

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
        paths: list[str],
        file_format_config: FileFormatConfig,
        schema_hint: Schema | None,
        fs: fsspec.AbstractFileSystem | None,
    ) -> RustLogicalPlanBuilder:
        if fs is not None:
            raise ValueError("fsspec filesystems not supported for Rust query planner.")
        # Glob the path using the Runner
        runner_io = get_context().runner().runner_io()
        file_info_partition_set = runner_io.glob_paths_details(paths, file_format_config, fs)

        # Infer schema if no hints provided
        inferred_or_provided_schema = (
            schema_hint
            if schema_hint is not None
            else runner_io.get_schema_from_first_filepath(file_info_partition_set, file_format_config, fs)
        )
        paths_details = file_info_partition_set.to_pydict()
        filepaths = paths_details[runner_io.FS_LISTING_PATH_COLUMN_NAME]
        rs_schema = inferred_or_provided_schema._schema
        builder = _LogicalPlanBuilder.table_scan(filepaths, rs_schema, file_format_config)
        return cls(builder)

    def project(
        self,
        projection: ExpressionsProjection,
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> RustLogicalPlanBuilder:
        if custom_resource_request != ResourceRequest():
            raise NotImplementedError("ResourceRequests not supported for new query planner")
        schema = projection.resolve_schema(self.schema())
        exprs = [expr._expr for expr in projection]
        builder = self._builder.project(exprs, schema._schema)
        return RustLogicalPlanBuilder(builder)

    def filter(self, predicate: Expression) -> RustLogicalPlanBuilder:
        # TODO(Clark): Move this logic to Rust side after we've ported ExpressionsProjection.
        predicate_expr_proj = ExpressionsProjection([predicate])
        predicate_schema = predicate_expr_proj.resolve_schema(self.schema())
        for resolved_field, predicate_expr in zip(predicate_schema, predicate_expr_proj):
            resolved_type = resolved_field.dtype
            if resolved_type != DataType.bool():
                raise ValueError(
                    f"Expected expression {predicate_expr} to resolve to type Boolean, but received: {resolved_type}"
                )
        builder = self._builder.filter(predicate._expr)
        return RustLogicalPlanBuilder(builder)

    def limit(self, num_rows: int) -> RustLogicalPlanBuilder:
        builder = self._builder.limit(num_rows)
        return RustLogicalPlanBuilder(builder)

    def explode(self, explode_expressions: ExpressionsProjection) -> RustLogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def count(self) -> RustLogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def distinct(self) -> RustLogicalPlanBuilder:
        builder = self._builder.distinct()
        return RustLogicalPlanBuilder(builder)

    def sort(self, sort_by: ExpressionsProjection, descending: list[bool] | bool = False) -> RustLogicalPlanBuilder:
        # Disallow sorting by null, binary, and boolean columns.
        # TODO(Clark): This is a port of an existing constraint, we should look at relaxing this.
        resolved_sort_by_schema = sort_by.resolve_schema(self.schema())
        for f, sort_by_expr in zip(resolved_sort_by_schema, sort_by):
            if f.dtype == DataType.null() or f.dtype == DataType.binary() or f.dtype == DataType.bool():
                raise ExpressionTypeError(f"Cannot sort on expression {sort_by_expr} with type: {f.dtype}")

        sort_by_exprs = [expr._expr for expr in sort_by]
        if not isinstance(descending, list):
            descending = [descending] * len(sort_by_exprs)
        builder = self._builder.sort(sort_by_exprs, descending)
        return RustLogicalPlanBuilder(builder)

    def repartition(
        self, num_partitions: int, partition_by: ExpressionsProjection, scheme: PartitionScheme
    ) -> RustLogicalPlanBuilder:
        partition_by_exprs = [expr._expr for expr in partition_by]
        builder = self._builder.repartition(num_partitions, partition_by_exprs, scheme)
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
        group_by: ExpressionsProjection | None,
    ) -> RustLogicalPlanBuilder:
        exprs = []
        for expr, op in to_agg:
            if op == "sum":
                exprs.append(expr._sum())
            else:
                raise NotImplementedError()

        builder = self._builder.aggregate([expr._expr for expr in exprs])
        return RustLogicalPlanBuilder(builder)

    def join(  # type: ignore[override]
        self,
        right: RustLogicalPlanBuilder,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.INNER,
    ) -> RustLogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def concat(self, other: RustLogicalPlanBuilder) -> RustLogicalPlanBuilder:  # type: ignore[override]
        builder = self._builder.concat(other._builder)
        return RustLogicalPlanBuilder(builder)

    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: ExpressionsProjection | None = None,
        compression: str | None = None,
    ) -> RustLogicalPlanBuilder:
        if file_format != FileFormat.Csv and file_format != FileFormat.Parquet:
            raise ValueError(f"Writing is only supported for Parquet and CSV file formats, but got: {file_format}")
        part_cols_pyexprs = [expr._expr for expr in partition_cols] if partition_cols is not None else None
        builder = self._builder.table_write(str(root_dir), file_format, part_cols_pyexprs, compression)
        return RustLogicalPlanBuilder(builder)
