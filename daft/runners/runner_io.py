from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, TypeVar

from daft.daft import (
    CsvSourceConfig,
    FileFormat,
    FileFormatConfig,
    FileInfos,
    JsonSourceConfig,
    ParquetSourceConfig,
    StorageConfig,
)
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions
from daft.table import schema_inference

if TYPE_CHECKING:
    import fsspec

PartitionT = TypeVar("PartitionT")


class RunnerIO:
    """Reading and writing data from the Runner.

    This is an abstract class and each runner must write their own implementation.
    """

    @abstractmethod
    def glob_paths_details(
        self,
        source_path: list[str],
        file_format_config: FileFormatConfig | None = None,
        fs: fsspec.AbstractFileSystem | None = None,
        storage_config: StorageConfig | None = None,
    ) -> FileInfos:
        """Globs the specified filepath to construct a FileInfos object containing file and dir metadata.

        Args:
            source_path (str): path to glob

        Returns:
            FileInfo: The file infos for the globbed paths.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_schema_from_first_filepath(
        self,
        file_infos: FileInfos,
        file_format_config: FileFormatConfig,
        storage_config: StorageConfig,
    ) -> Schema:
        raise NotImplementedError()


def sample_schema(
    filepath: str,
    file_format_config: FileFormatConfig,
    storage_config: StorageConfig,
) -> Schema:
    """Helper method that samples a schema from the specified source"""
    file_format = file_format_config.file_format()
    config = file_format_config.config
    if file_format == FileFormat.Csv:
        assert isinstance(config, CsvSourceConfig)
        return schema_inference.from_csv(
            file=filepath,
            storage_config=storage_config,
            csv_options=TableParseCSVOptions(
                delimiter=config.delimiter,
                header_index=0 if config.has_headers else None,
            ),
        )
    elif file_format == FileFormat.Json:
        assert isinstance(config, JsonSourceConfig)
        return schema_inference.from_json(
            file=filepath,
            storage_config=storage_config,
        )
    elif file_format == FileFormat.Parquet:
        assert isinstance(config, ParquetSourceConfig)
        return schema_inference.from_parquet(
            file=filepath,
            storage_config=storage_config,
        )
    else:
        raise NotImplementedError(f"Schema inference for {file_format} not implemented")
