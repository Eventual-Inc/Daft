from __future__ import annotations

from abc import abstractmethod
from typing import Generic, TypeVar

import fsspec

from daft.datasources import (
    CSVSourceInfo,
    JSONSourceInfo,
    ParquetSourceInfo,
    SourceInfo,
    StorageType,
)
from daft.datatype import DataType
from daft.filesystem import get_filesystem_from_path
from daft.logical.schema import Schema
from daft.runners.partitioning import PartitionSet, TableParseCSVOptions
from daft.table import schema_inference

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
            (FS_LISTING_SIZE_COLUMN_NAME, DataType.int64()),
            (FS_LISTING_PATH_COLUMN_NAME, DataType.string()),
            (FS_LISTING_TYPE_COLUMN_NAME, DataType.string()),
            (FS_LISTING_ROWS_COLUMN_NAME, DataType.int64()),
        ]
    )

    @abstractmethod
    def glob_paths_details(
        self,
        source_path: list[str],
        source_info: SourceInfo | None = None,
        fs: fsspec.AbstractFileSystem | None = None,
    ) -> PartitionSet[PartitionT]:
        """Globs the specified filepath to construct Partitions containing file and dir metadata

        Args:
            source_path (str): path to glob

        Returns:
            PartitionSet[PartitionT]: Partitions containing the listings' metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema_from_first_filepath(
        self,
        listing_details_partitions: PartitionSet[PartitionT],
        source_info: SourceInfo,
        fs: fsspec.AbstractFileSystem | None,
    ) -> Schema:
        raise NotImplementedError()


def sample_schema(
    filepath: str,
    source_info: SourceInfo,
    fs: fsspec.AbstractFileSystem | None,
) -> Schema:
    """Helper method that samples a schema from the specified source"""
    if fs is None:
        fs = get_filesystem_from_path(filepath)

    if source_info.scan_type() == StorageType.CSV:
        assert isinstance(source_info, CSVSourceInfo)
        return schema_inference.from_csv(
            file=filepath,
            fs=fs,
            csv_options=TableParseCSVOptions(
                delimiter=source_info.delimiter,
                header_index=0 if source_info.has_headers else None,
            ),
        )
    elif source_info.scan_type() == StorageType.JSON:
        assert isinstance(source_info, JSONSourceInfo)
        return schema_inference.from_json(
            file=filepath,
            fs=fs,
        )
    elif source_info.scan_type() == StorageType.PARQUET:
        assert isinstance(source_info, ParquetSourceInfo)
        return schema_inference.from_parquet(
            file=filepath,
            fs=fs,
            io_config=source_info.io_config,
            use_native_downloader=source_info.use_native_downloader,
        )
    else:
        raise NotImplementedError(f"Schema inference for {source_info} not implemented")
