# isort: dont-add-import: from __future__ import annotations

from typing import Dict, List, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    FileFormatConfig,
    IOConfig,
    JsonSourceConfig,
    NativeStorageConfig,
    PythonStorageConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_json(
    path: Union[str, List[str]],
    schema_hints: Optional[Dict[str, DataType]] = None,
    io_config: Optional["IOConfig"] = None,
    use_native_downloader: bool = True,
    _buffer_size: Optional[int] = None,
    _chunk_size: Optional[int] = None,
) -> DataFrame:
    """Creates a DataFrame from line-delimited JSON file(s)

    Example:
        >>> df = daft.read_json("/path/to/file.json")
        >>> df = daft.read_json("/path/to/directory")
        >>> df = daft.read_json("/path/to/files-*.json")
        >>> df = daft.read_json("s3://path/to/files-*.json")

    Args:
        path (str): Path to JSON files (allows for wildcards)
        schema_hints (dict[str, DataType]): A mapping between column names and datatypes - passing this option
            will override the specified columns on the inferred schema with the specified DataTypes
        io_config (IOConfig): Config to be used with the native downloader
        use_native_downloader: Whether to use the native downloader instead of PyArrow for reading Parquet. This
            is currently experimental.

    returns:
        DataFrame: parsed DataFrame
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of JSON filepaths")

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    json_config = JsonSourceConfig(_buffer_size, _chunk_size)
    file_format_config = FileFormatConfig.from_json_config(json_config)
    if use_native_downloader:
        storage_config = StorageConfig.native(NativeStorageConfig(True, io_config))
    else:
        storage_config = StorageConfig.python(PythonStorageConfig(io_config=io_config))
    builder = get_tabular_files_scan(path, schema_hints, file_format_config, storage_config=storage_config)
    return DataFrame(builder)
