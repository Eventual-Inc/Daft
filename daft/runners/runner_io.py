from __future__ import annotations

from abc import abstractmethod
from typing import TypeVar

import fsspec

from daft.daft import (
    CsvSourceConfig,
    FileFormat,
    FileFormatConfig,
    FileInfos,
    JsonSourceConfig,
    ParquetSourceConfig,
)
from daft.filesystem import get_filesystem_from_path
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions
from daft.table import schema_inference

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
        file_info: FileInfos,
        file_format_config: FileFormatConfig,
        fs: fsspec.AbstractFileSystem | None,
    ) -> Schema:
        raise NotImplementedError()


def sample_schema(
    filepath: str,
    file_format_config: FileFormatConfig,
    fs: fsspec.AbstractFileSystem | None,
) -> Schema:
    """Helper method that samples a schema from the specified source"""
    if fs is None:
        fs = get_filesystem_from_path(filepath)

    file_format = file_format_config.file_format()
    config = file_format_config.config
    if file_format == FileFormat.Csv:
        assert isinstance(config, CsvSourceConfig)
        return schema_inference.from_csv(
            file=filepath,
            fs=fs,
            csv_options=TableParseCSVOptions(
                delimiter=config.delimiter,
                header_index=0 if config.has_headers else None,
            ),
        )
    elif file_format == FileFormat.Json:
        assert isinstance(config, JsonSourceConfig)
        return schema_inference.from_json(
            file=filepath,
            fs=fs,
        )
    elif file_format == FileFormat.Parquet:
        assert isinstance(config, ParquetSourceConfig)
        return schema_inference.from_parquet(
            file=filepath,
            fs=fs,
            io_config=config.io_config,
            use_native_downloader=config.use_native_downloader,
        )
    else:
        raise NotImplementedError(f"Schema inference for {file_format} not implemented")
