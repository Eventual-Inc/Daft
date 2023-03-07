from __future__ import annotations

from abc import abstractmethod
from typing import Generic, TypeVar

from daft.datasources import SourceInfo
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionSet
from daft.types import ExpressionType

PartitionT = TypeVar("PartitionT")


class RunnerIO(Generic[PartitionT]):
    """Reading and writing data from the Runner.

    This is an abstract class and each runner must write their own implementation.

    This is a generic class and each runner needs to parametrize their implementation with the appropriate
    PartitionT that it uses for in-memory data representation.
    """

    FS_LISTING_PATH_COLUMN_NAME = "path"
    FS_LISTING_SIZE_COLUMN_NAME = "size"
    FS_LISTING_TYPE_COLUMN_NAME = "type"
    FS_LISTING_ROWS_COLUMN_NAME = "rows"
    FS_LISTING_SCHEMA = Schema._from_field_name_and_types(
        [
            (FS_LISTING_SIZE_COLUMN_NAME, ExpressionType.integer()),
            (FS_LISTING_PATH_COLUMN_NAME, ExpressionType.string()),
            (FS_LISTING_TYPE_COLUMN_NAME, ExpressionType.string()),
            (FS_LISTING_ROWS_COLUMN_NAME, ExpressionType.integer()),
        ]
    )

    @abstractmethod
    def glob_paths_details(
        self,
        source_path: str,
        source_info: SourceInfo | None = None,
    ) -> PartitionSet[PartitionT]:
        """Globs the specified filepath to construct Partitions containing file and dir metadata

        Args:
            source_path (str): path to glob

        Returns:
            PartitionSet[PartitionT]: Partitions containing the listings' metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema(self, listing_details_partitions: PartitionSet[PartitionT], source_info: SourceInfo) -> Schema:
        raise NotImplementedError()
