from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypedDict

from daft.logical.builder import LogicalPlanBuilder
from daft.logical.schema import Schema
from daft.daft import ScanOperatorHandle

if TYPE_CHECKING:
    from daft.daft import FileFormatConfig, StorageConfig


class ReadSource(ABC):

    @abstractmethod
    def to_logical_plan(self, **pushdowns) -> LogicalPlanBuilder:
        """Returns a logical plan from the ReadSource"""

ReadSourcePushdowns = TypedDict


class GlobSourcePushdowns(TypedDict, total=False):
    path_suffix: str | None


class GlobSource(ReadSource):
    paths: list[str]
    infer_schema: bool
    schema: Schema | None
    file_format_config: FileFormatConfig
    storage_config: StorageConfig
    file_path_column: str | None = None
    hive_partitioning: bool = False

    def __init__(
        self,
        paths: list[str],
        infer_schema: bool,
        schema: Schema | None,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
        file_path_column: str | None = None,
        hive_partitioning: bool = False,
    ) -> None:
        super().__init__()
        self.paths = paths
        self.infer_schema = infer_schema
        self.schema = schema
        self.file_format_config = file_format_config
        self.storage_config = storage_config
        self.file_path_column = file_path_column
        self.hive_partitioning = hive_partitioning

    def to_logical_plan(self, **pushdowns: GlobSourcePushdowns) -> LogicalPlanBuilder:
        handle = ScanOperatorHandle.glob_scan(
            self.paths,
            self.file_format_config,
            self.storage_config,
            self.infer_schema,
            self.schema._schema,
            self.file_path_column,
            self.hive_partitioning,
        )
        return LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
