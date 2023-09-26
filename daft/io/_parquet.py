# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union

from daft.api_annotations import PublicAPI
from daft.daft import (
    FileFormatConfig,
    NativeStorageConfig,
    ParquetSourceConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import _get_tabular_files_scan

if TYPE_CHECKING:
    from daft.io import IOConfig


@PublicAPI
def read_parquet(
    path: Union[str, List[str]],
    schema_hints: Optional[Dict[str, DataType]] = None,
    io_config: Optional["IOConfig"] = None,
) -> DataFrame:
    """Creates a DataFrame from Parquet file(s)

    Example:
        >>> df = daft.read_parquet("/path/to/file.parquet")
        >>> df = daft.read_parquet("/path/to/directory")
        >>> df = daft.read_parquet("/path/to/files-*.parquet")
        >>> df = daft.read_parquet("s3://path/to/files-*.parquet")

    Args:
        path (str): Path to Parquet file (allows for wildcards)
        schema_hints (dict[str, DataType]): A mapping between column names and datatypes - passing this option will
            disable all schema inference on data being read, and throw an error if data being read is incompatible.
        io_config (IOConfig): Config to be used with the native downloader

    returns:
        DataFrame: parsed DataFrame
    """

    if isinstance(path, list) and len(path) == 0:
        raise ValueError(f"Cannot read DataFrame from from empty list of Parquet filepaths")

    file_format_config = FileFormatConfig.from_parquet_config(ParquetSourceConfig())
    builder = _get_tabular_files_scan(
        path, schema_hints, file_format_config, storage_config=StorageConfig.native(NativeStorageConfig(io_config))
    )
    return DataFrame(builder)
