# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    FileFormatConfig,
    IOConfig,
    JsonSourceConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_json(
    path: Union[str, list[str]],
    infer_schema: bool = True,
    schema: Optional[dict[str, DataType]] = None,
    io_config: Optional[IOConfig] = None,
    file_path_column: Optional[str] = None,
    hive_partitioning: bool = False,
    skip_empty_files: bool = False,
    _buffer_size: Optional[int] = None,
    _chunk_size: Optional[int] = None,
) -> DataFrame:
    """Creates a DataFrame from line-delimited JSON file(s).

    Args:
        path (str): Path to JSON files (allows for wildcards)
        infer_schema (bool): Whether to infer the schema of the JSON, defaults to True.
        schema (dict[str, DataType]): A schema that is used as the definitive schema for the JSON if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred.
        io_config (IOConfig): Config to be used with the native downloader
        file_path_column: Include the source path(s) as a column with this name. Defaults to None.
        hive_partitioning: Whether to infer hive_style partitions from file paths and include them as columns in the Dataframe. Defaults to False.
        skip_empty_files: Whether to skip empty files when reading. Defaults to False.

    Returns:
        DataFrame: parsed DataFrame

    Examples:
        >>> df = daft.read_json("/path/to/file.json")
        >>> df = daft.read_json("/path/to/directory")
        >>> df = daft.read_json("/path/to/files-*.json")
        >>> df = daft.read_json("s3://path/to/files-*.json")

    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of JSON filepaths")

    if not infer_schema and schema is None:
        raise ValueError(
            "Cannot read DataFrame with infer_schema=False and schema=None, please provide a schema or set infer_schema=True"
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    json_config = JsonSourceConfig(buffer_size=_buffer_size, chunk_size=_chunk_size, skip_empty_files=skip_empty_files)
    file_format_config = FileFormatConfig.from_json_config(json_config)
    storage_config = StorageConfig(True, io_config)

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
