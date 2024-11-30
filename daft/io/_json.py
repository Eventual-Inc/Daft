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
    infer_schema: bool = True,
    schema: Optional[Dict[str, DataType]] = None,
    io_config: Optional["IOConfig"] = None,
    file_path_column: Optional[str] = None,
    hive_partitioning: bool = False,
    use_native_downloader: bool = True,
    schema_hints: Optional[Dict[str, DataType]] = None,
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
        infer_schema (bool): Whether to infer the schema of the JSON, defaults to True.
        schema (dict[str, DataType]): A schema that is used as the definitive schema for the JSON if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred.
        io_config (IOConfig): Config to be used with the native downloader
        file_path_column: Include the source path(s) as a column with this name. Defaults to None.
        hive_partitioning: Whether to infer hive_style partitions from file paths and include them as columns in the Dataframe. Defaults to False.
        use_native_downloader: Whether to use the native downloader instead of PyArrow for reading Parquet. This
            is currently experimental.

    returns:
        DataFrame: parsed DataFrame
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of JSON filepaths")

    if schema_hints is not None:
        raise ValueError(
            "Specifying schema_hints is deprecated from Daft version >= 0.3.0! Instead, please use the 'schema' and 'infer_schema' arguments."
        )

    if not infer_schema and schema is None:
        raise ValueError(
            "Cannot read DataFrame with infer_schema=False and schema=None, please provide a schema or set infer_schema=True"
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    json_config = JsonSourceConfig(_buffer_size, _chunk_size)
    file_format_config = FileFormatConfig.from_json_config(json_config)
    if use_native_downloader:
        storage_config = StorageConfig.native(NativeStorageConfig(True, io_config))
    else:
        storage_config = StorageConfig.python(PythonStorageConfig(io_config=io_config))
    builder = get_tabular_files_scan(
        path=path,
        infer_schema=infer_schema,
        schema=schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
        file_path_column=file_path_column,
        hive_partitioning=hive_partitioning,
    )
    return DataFrame(builder)
