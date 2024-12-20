# isort: dont-add-import: from __future__ import annotations

from typing import Dict, List, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    CsvSourceConfig,
    FileFormatConfig,
    IOConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_csv(
    path: Union[str, List[str]],
    infer_schema: bool = True,
    schema: Optional[Dict[str, DataType]] = None,
    has_headers: bool = True,
    delimiter: Optional[str] = None,
    double_quote: bool = True,
    quote: Optional[str] = None,
    escape_char: Optional[str] = None,
    comment: Optional[str] = None,
    allow_variable_columns: bool = False,
    io_config: Optional["IOConfig"] = None,
    file_path_column: Optional[str] = None,
    hive_partitioning: bool = False,
    schema_hints: Optional[Dict[str, DataType]] = None,
    _buffer_size: Optional[int] = None,
    _chunk_size: Optional[int] = None,
) -> DataFrame:
    """Creates a DataFrame from CSV file(s).

    Example:
        >>> df = daft.read_csv("/path/to/file.csv")
        >>> df = daft.read_csv("/path/to/directory")
        >>> df = daft.read_csv("/path/to/files-*.csv")
        >>> df = daft.read_csv("s3://path/to/files-*.csv")

    Args:
        path (str): Path to CSV (allows for wildcards)
        infer_schema (bool): Whether to infer the schema of the CSV, defaults to True.
        schema (dict[str, DataType]): A schema that is used as the definitive schema for the CSV if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred.
        has_headers (bool): Whether the CSV has a header or not, defaults to True
        delimiter (Str): Delimiter used in the CSV, defaults to ","
        doubled_quote (bool): Whether to support double quote escapes, defaults to True
        escape_char (str): Character to use as the escape character for double quotes, or defaults to `"`
        comment (str): Character to treat as the start of a comment line, or None to not support comments
        allow_variable_columns (bool): Whether to allow for variable number of columns in the CSV, defaults to False. If set to True, Daft will append nulls to rows with less columns than the schema, and ignore extra columns in rows with more columns
        io_config (IOConfig): Config to be used with the native downloader
        file_path_column: Include the source path(s) as a column with this name. Defaults to None.
        hive_partitioning: Whether to infer hive_style partitions from file paths and include them as columns in the Dataframe. Defaults to False.

    returns:
        DataFrame: parsed DataFrame
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of CSV filepaths")

    if schema_hints is not None:
        raise ValueError(
            "Specifying schema_hints is deprecated from Daft version >= 0.3.0! Instead, please use the 'schema' and 'infer_schema' arguments."
        )

    if not infer_schema and schema is None:
        raise ValueError(
            "Cannot read DataFrame with infer_schema=False and schema=None, please provide a schema or set infer_schema=True"
        )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    csv_config = CsvSourceConfig(
        delimiter=delimiter,
        has_headers=has_headers,
        double_quote=double_quote,
        quote=quote,
        escape_char=escape_char,
        comment=comment,
        allow_variable_columns=allow_variable_columns,
        buffer_size=_buffer_size,
        chunk_size=_chunk_size,
    )
    file_format_config = FileFormatConfig.from_csv_config(csv_config)
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
