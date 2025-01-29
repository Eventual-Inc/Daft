from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Callable

from daft.context import get_context
from daft.daft import (
    CountMode,
    FileFormat,
    IOConfig,
    JoinStrategy,
    JoinType,
    PyDaftExecutionConfig,
    ScanOperatorHandle,
    logical_plan_table_scan,
)
from daft.daft import LogicalPlanBuilder as _LogicalPlanBuilder
from daft.expressions import Expression, col
from daft.logical.schema import Schema

if TYPE_CHECKING:
    import pathlib

    from pyiceberg.table import Table as IcebergTable

    from daft.plan_scheduler.physical_plan_scheduler import (
        AdaptivePhysicalPlanScheduler,
        PhysicalPlanScheduler,
    )
    from daft.runners.partitioning import PartitionCacheEntry


def _apply_daft_planning_config_to_initializer(classmethod_func: Callable[..., LogicalPlanBuilder]):
    """Decorator to be applied to any @classmethod instantiation method on LogicalPlanBuilder.

    This decorator ensures that the current DaftPlanningConfig is applied to the instantiated LogicalPlanBuilder
    """

    @functools.wraps(classmethod_func)
    def wrapper(cls: type[LogicalPlanBuilder], *args, **kwargs):
        instantiated_logical_plan_builder = classmethod_func(cls, *args, **kwargs)

        # Parametrize the builder with the current DaftPlanningConfig
        inner = instantiated_logical_plan_builder._builder
        inner = inner.with_planning_config(get_context().daft_planning_config)

        return cls(inner)

    return wrapper


class LogicalPlanBuilder:
    """A logical plan builder for the Daft DataFrame."""

    def __init__(self, builder: _LogicalPlanBuilder) -> None:
        self._builder = builder

    def to_physical_plan_scheduler(self, daft_execution_config: PyDaftExecutionConfig) -> PhysicalPlanScheduler:
        """Convert the underlying logical plan to a physical plan scheduler.

        physical plan scheduler is used to generate executable tasks for the physical plan.

        This should be called after triggering optimization with self.optimize().

        **Warning**: This function is not part of the stable API and may change
        without notice. It is intended for internal or experimental use only.
        """
        from daft.plan_scheduler.physical_plan_scheduler import PhysicalPlanScheduler

        return PhysicalPlanScheduler.from_logical_plan_builder(
            self,
            daft_execution_config,
        )

    def to_adaptive_physical_plan_scheduler(
        self, daft_execution_config: PyDaftExecutionConfig
    ) -> AdaptivePhysicalPlanScheduler:
        from daft.plan_scheduler.physical_plan_scheduler import (
            AdaptivePhysicalPlanScheduler,
        )

        return AdaptivePhysicalPlanScheduler.from_logical_plan_builder(
            self,
            daft_execution_config,
        )

    def schema(self) -> Schema:
        """The schema of the current logical plan."""
        pyschema = self._builder.schema()
        return Schema._from_pyschema(pyschema)

    def describe(self) -> LogicalPlanBuilder:
        builder = self._builder.describe()
        return LogicalPlanBuilder(builder)

    def pretty_print(self, simple: bool = False, format: str = "ascii") -> str:
        """Pretty prints the current underlying logical plan."""
        from daft.dataframe.display import MermaidOptions

        if format == "ascii":
            return self._builder.repr_ascii(simple)
        elif format == "mermaid":
            return self._builder.repr_mermaid(MermaidOptions(simple))
        else:
            raise ValueError(f"Unknown format: {format}")

    def __repr__(self) -> str:
        return self._builder.repr_ascii(simple=False)

    def optimize(self) -> LogicalPlanBuilder:
        """Optimize the underlying logical plan."""
        builder = self._builder.optimize()
        return LogicalPlanBuilder(builder)

    @classmethod
    @_apply_daft_planning_config_to_initializer
    def from_in_memory_scan(
        cls,
        partition: PartitionCacheEntry,
        schema: Schema,
        num_partitions: int,
        size_bytes: int,
        num_rows: int,
    ) -> LogicalPlanBuilder:
        builder = _LogicalPlanBuilder.in_memory_scan(
            partition.key,
            partition,
            schema._schema,
            num_partitions,
            size_bytes,
            num_rows,
        )
        return cls(builder)

    @classmethod
    @_apply_daft_planning_config_to_initializer
    def from_tabular_scan(
        cls,
        *,
        scan_operator: ScanOperatorHandle,
    ) -> LogicalPlanBuilder:
        builder = logical_plan_table_scan(scan_operator)
        return cls(builder)

    def select(
        self,
        to_select: list[Expression],
    ) -> LogicalPlanBuilder:
        to_select_pyexprs = [expr._expr for expr in to_select]
        builder = self._builder.select(to_select_pyexprs)
        return LogicalPlanBuilder(builder)

    def with_columns(self, columns: list[Expression]) -> LogicalPlanBuilder:
        column_pyexprs = [expr._expr for expr in columns]
        builder = self._builder.with_columns(column_pyexprs)
        return LogicalPlanBuilder(builder)

    def with_column_renamed(self, existing: str, new: str) -> LogicalPlanBuilder:
        cols_map = {existing: new}
        builder = self._builder.with_columns_renamed(cols_map)
        return LogicalPlanBuilder(builder)

    def with_columns_renamed(self, cols_map: dict[str, str]) -> LogicalPlanBuilder:
        builder = self._builder.with_columns_renamed(cols_map)
        return LogicalPlanBuilder(builder)

    def exclude(self, to_exclude: list[str]) -> LogicalPlanBuilder:
        builder = self._builder.exclude(to_exclude)
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

    def unpivot(
        self,
        ids: list[Expression],
        values: list[Expression],
        variable_name: str,
        value_name: str,
    ) -> LogicalPlanBuilder:
        ids_pyexprs = [expr._expr for expr in ids]
        values_pyexprs = [expr._expr for expr in values]
        builder = self._builder.unpivot(ids_pyexprs, values_pyexprs, variable_name, value_name)
        return LogicalPlanBuilder(builder)

    def count(self) -> LogicalPlanBuilder:
        # TODO(Clark): Add dedicated logical/physical ops when introducing metadata-based count optimizations.
        first_col = col(self.schema().column_names()[0])
        builder = self._builder.aggregate([first_col.count(CountMode.All)._expr], [])
        builder = builder.select([first_col.alias("count")._expr])
        return LogicalPlanBuilder(builder)

    def distinct(self) -> LogicalPlanBuilder:
        builder = self._builder.distinct()
        return LogicalPlanBuilder(builder)

    def sample(self, fraction: float, with_replacement: bool, seed: int | None) -> LogicalPlanBuilder:
        builder = self._builder.sample(fraction, with_replacement, seed)
        return LogicalPlanBuilder(builder)

    def sort(
        self,
        sort_by: list[Expression],
        descending: list[bool] | bool = False,
        nulls_first: list[bool] | bool | None = None,
    ) -> LogicalPlanBuilder:
        sort_by_pyexprs = [expr._expr for expr in sort_by]
        if not isinstance(descending, list):
            descending = [descending] * len(sort_by_pyexprs)
        if nulls_first is None:
            nulls_first = descending
        elif isinstance(nulls_first, bool):
            nulls_first = [nulls_first] * len(sort_by_pyexprs)
        builder = self._builder.sort(sort_by_pyexprs, descending, nulls_first)
        return LogicalPlanBuilder(builder)

    def hash_repartition(self, num_partitions: int | None, partition_by: list[Expression]) -> LogicalPlanBuilder:
        partition_by_pyexprs = [expr._expr for expr in partition_by]
        builder = self._builder.hash_repartition(partition_by_pyexprs, num_partitions=num_partitions)
        return LogicalPlanBuilder(builder)

    def random_shuffle(self, num_partitions: int | None) -> LogicalPlanBuilder:
        builder = self._builder.random_shuffle(num_partitions)
        return LogicalPlanBuilder(builder)

    def into_partitions(self, num_partitions: int) -> LogicalPlanBuilder:
        builder = self._builder.into_partitions(num_partitions)
        return LogicalPlanBuilder(builder)

    def agg(
        self,
        to_agg: list[Expression],
        group_by: list[Expression] | None,
    ) -> LogicalPlanBuilder:
        group_by_pyexprs = [expr._expr for expr in group_by] if group_by is not None else []
        builder = self._builder.aggregate([expr._expr for expr in to_agg], group_by_pyexprs)
        return LogicalPlanBuilder(builder)

    def map_groups(self, udf: Expression, group_by: list[Expression] | None) -> LogicalPlanBuilder:
        group_by_pyexprs = [expr._expr for expr in group_by] if group_by is not None else []
        builder = self._builder.aggregate([udf._expr], group_by_pyexprs)
        return LogicalPlanBuilder(builder)

    def pivot(
        self,
        group_by: list[Expression],
        pivot_col: Expression,
        value_col: Expression,
        agg_fn: Expression,
        names: list[str],
    ) -> LogicalPlanBuilder:
        group_by_pyexprs = [expr._expr for expr in group_by]
        builder = self._builder.pivot(group_by_pyexprs, pivot_col._expr, value_col._expr, agg_fn._expr, names)
        return LogicalPlanBuilder(builder)

    def join(  # type: ignore[override]
        self,
        right: LogicalPlanBuilder,
        left_on: list[Expression],
        right_on: list[Expression],
        how: JoinType = JoinType.Inner,
        strategy: JoinStrategy | None = None,
        prefix: str | None = None,
        suffix: str | None = None,
    ) -> LogicalPlanBuilder:
        builder = self._builder.join(
            right._builder,
            [expr._expr for expr in left_on],
            [expr._expr for expr in right_on],
            how,
            strategy,
            prefix,
            suffix,
        )
        return LogicalPlanBuilder(builder)

    def concat(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:  # type: ignore[override]
        builder = self._builder.concat(other._builder)
        return LogicalPlanBuilder(builder)

    def intersect(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:
        builder = self._builder.intersect(other._builder, False)
        return LogicalPlanBuilder(builder)

    def intersect_all(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:
        builder = self._builder.intersect(other._builder, True)
        return LogicalPlanBuilder(builder)

    def except_distinct(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:
        builder = self._builder.except_(other._builder, False)
        return LogicalPlanBuilder(builder)

    def except_all(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:
        builder = self._builder.except_(other._builder, True)
        return LogicalPlanBuilder(builder)

    def add_monotonically_increasing_id(self, column_name: str | None) -> LogicalPlanBuilder:
        builder = self._builder.add_monotonically_increasing_id(column_name)
        return LogicalPlanBuilder(builder)

    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        io_config: IOConfig,
        partition_cols: list[Expression] | None = None,
        compression: str | None = None,
    ) -> LogicalPlanBuilder:
        if file_format != FileFormat.Csv and file_format != FileFormat.Parquet:
            raise ValueError(f"Writing is only supported for Parquet and CSV file formats, but got: {file_format}")
        part_cols_pyexprs = [expr._expr for expr in partition_cols] if partition_cols is not None else None
        builder = self._builder.table_write(str(root_dir), file_format, part_cols_pyexprs, compression, io_config)
        return LogicalPlanBuilder(builder)

    def write_iceberg(self, table: IcebergTable, io_config: IOConfig) -> LogicalPlanBuilder:
        from daft.iceberg.iceberg_write import get_missing_columns, partition_field_to_expr

        name = ".".join(table.name())
        location = f"{table.location()}/data"
        partition_spec = table.spec()
        schema = table.schema()
        missing_columns = get_missing_columns(self.schema().to_pyarrow_schema(), schema)
        builder = (
            self._builder
            if len(missing_columns) == 0
            else self._builder.with_columns([c._expr for c in missing_columns])
        )
        partition_cols = [partition_field_to_expr(field, schema)._expr for field in partition_spec.fields]
        props = table.properties
        columns = [col.name for col in schema.columns]
        builder = builder.iceberg_write(
            name, location, partition_spec.spec_id, partition_cols, schema, props, columns, io_config
        )
        return LogicalPlanBuilder(builder)

    def write_deltalake(
        self,
        path: str | pathlib.Path,
        mode: str,
        version: int,
        large_dtypes: bool,
        io_config: IOConfig,
        partition_cols: list[str] | None = None,
    ) -> LogicalPlanBuilder:
        columns_name = self.schema().column_names()
        builder = self._builder.delta_write(
            str(path),
            columns_name,
            mode,
            version,
            large_dtypes,
            partition_cols,
            io_config,
        )
        return LogicalPlanBuilder(builder)

    def write_lance(
        self,
        path: str | pathlib.Path,
        mode: str,
        io_config: IOConfig,
        kwargs: dict | None,
    ) -> LogicalPlanBuilder:
        columns_name = self.schema().column_names()
        builder = self._builder.lance_write(
            str(path),
            columns_name,
            mode,
            io_config,
            kwargs,
        )
        return LogicalPlanBuilder(builder)
