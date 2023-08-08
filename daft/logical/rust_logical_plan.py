from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import fsspec

from daft.context import get_context
from daft.daft import FileFormat, FileFormatConfig
from daft.daft import LogicalPlanBuilder as _LogicalPlanBuilder
from daft.daft import PartitionScheme, PartitionSpec
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
    ) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

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
        return RustLogicalPlanBuilder(builder)

    def project(
        self,
        projection: ExpressionsProjection,
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def filter(self, predicate: Expression) -> LogicalPlanBuilder:
        builder = self._builder.filter(predicate._expr)
        return RustLogicalPlanBuilder(builder)

    def limit(self, num_rows: int) -> LogicalPlanBuilder:
        builder = self._builder.limit(num_rows)
        return RustLogicalPlanBuilder(builder)

    def explode(self, explode_expressions: ExpressionsProjection) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def count(self) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def distinct(self) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def sort(self, sort_by: ExpressionsProjection, descending: list[bool] | bool = False) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def repartition(
        self, num_partitions: int, partition_by: ExpressionsProjection, scheme: PartitionScheme
    ) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def coalesce(self, num_partitions: int) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def join(
        self,
        right: LogicalPlanBuilder,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.INNER,
    ) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def concat(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")

    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: ExpressionsProjection | None = None,
        compression: str | None = None,
    ) -> LogicalPlanBuilder:
        raise NotImplementedError("not implemented")
