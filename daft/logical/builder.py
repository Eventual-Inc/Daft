from __future__ import annotations

import pathlib
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import fsspec

from daft.daft import (
    FileFormat,
    FileFormatConfig,
    JoinType,
    PartitionScheme,
    PartitionSpec,
    ResourceRequest,
)
from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionCacheEntry

if TYPE_CHECKING:
    from daft.planner import PhysicalPlanScheduler


class LogicalPlanBuilder(ABC):
    """
    An interface for building a logical plan for the Daft DataFrame.
    """

    @abstractmethod
    def to_physical_plan_scheduler(self) -> PhysicalPlanScheduler:
        """
        Convert the underlying logical plan to a physical plan scheduler, which is
        used to generate executable tasks for the physical plan.

        This should be called after triggering optimization with self.optimize().
        """

    @abstractmethod
    def schema(self) -> Schema:
        """
        The schema of the current logical plan.
        """

    @abstractmethod
    def partition_spec(self) -> PartitionSpec:
        """
        Partition spec for the current logical plan.
        """

    def num_partitions(self) -> int:
        """
        Number of partitions for the current logical plan.
        """
        return self.partition_spec().num_partitions

    @abstractmethod
    def pretty_print(self) -> str:
        """
        Pretty prints the current underlying logical plan.
        """

    @abstractmethod
    def optimize(self) -> LogicalPlanBuilder:
        """
        Optimize the underlying logical plan.
        """

    ### Logical operator builder methods.

    @classmethod
    @abstractmethod
    def from_in_memory_scan(
        cls, partition: PartitionCacheEntry, schema: Schema, partition_spec: PartitionSpec | None = None
    ) -> LogicalPlanBuilder:
        pass

    @classmethod
    @abstractmethod
    def from_tabular_scan(
        cls,
        *,
        paths: list[str],
        file_format_config: FileFormatConfig,
        schema_hint: Schema | None,
        fs: fsspec.AbstractFileSystem | None,
    ) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def project(
        self,
        projection: ExpressionsProjection,
        custom_resource_request: ResourceRequest = ResourceRequest(),
    ) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def filter(self, predicate: Expression) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def limit(self, num_rows: int) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def explode(self, explode_expressions: ExpressionsProjection) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def count(self) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def distinct(self) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def sort(self, sort_by: ExpressionsProjection, descending: list[bool] | bool = False) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def repartition(
        self, num_partitions: int, partition_by: ExpressionsProjection, scheme: PartitionScheme
    ) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def coalesce(self, num_partitions: int) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def agg(self, to_agg: list[tuple[Expression, str]], group_by: ExpressionsProjection | None) -> LogicalPlanBuilder:
        """
        to_agg: (<expression identifying column>, <string identifying agg operation>)
        TODO - clean this up after old logical plan is removed
        """

    @abstractmethod
    def join(
        self,
        right: LogicalPlanBuilder,
        left_on: ExpressionsProjection,
        right_on: ExpressionsProjection,
        how: JoinType = JoinType.Inner,
    ) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def concat(self, other: LogicalPlanBuilder) -> LogicalPlanBuilder:
        pass

    @abstractmethod
    def write_tabular(
        self,
        root_dir: str | pathlib.Path,
        file_format: FileFormat,
        partition_cols: ExpressionsProjection | None = None,
        compression: str | None = None,
    ) -> LogicalPlanBuilder:
        pass
