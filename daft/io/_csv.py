# isort: dont-add-import: from __future__ import annotations

from typing import Dict, List, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    CsvSourceConfig,
    FileFormatConfig,
    IOConfig,
    NativeStorageConfig,
    PythonStorageConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_csv(
    path: Union[str, List[str]],
    schema_hints: Optional[Dict[str, DataType]] = None,
    schema_override: Optional[Dict[str, DataType]] = None,
    has_headers: bool = True,
    delimiter: Optional[str] = None,
    double_quote: bool = True,
    quote: Optional[str] = None,
    escape_char: Optional[str] = None,
    comment: Optional[str] = None,
    allow_variable_columns: bool = False,
    io_config: Optional["IOConfig"] = None,
    use_native_downloader: bool = True,
    _buffer_size: Optional[int] = None,
    _chunk_size: Optional[int] = None,
) -> DataFrame:
    """Creates a DataFrame from CSV file(s)

    Example:
        >>> df = daft.read_csv("/path/to/file.csv")
        >>> df = daft.read_csv("/path/to/directory")
        >>> df = daft.read_csv("/path/to/files-*.csv")
        >>> df = daft.read_csv("s3://path/to/files-*.csv")

    Args:
        path (str): Path to CSV (allows for wildcards)
        schema_hints (dict[str, DataType]): A mapping between column names and datatypes - passing this option
            will override the specified columns on the inferred schema with the specified DataTypes
        schema_override (dict[str, DataType]): A definitive schema to use for the CSV, passing this option will
            disable schema inference
        has_headers (bool): Whether the CSV has a header or not, defaults to True
        delimiter (Str): Delimiter used in the CSV, defaults to ","
        doubled_quote (bool): Whether to support double quote escapes, defaults to True
        escape_char (str): Character to use as the escape character for double quotes, or defaults to `"`
        comment (str): Character to treat as the start of a comment line, or None to not support comments
        allow_variable_columns (bool): Whether to allow for variable number of columns in the CSV, defaults to False. If set to True, Daft will append nulls to rows with less columns than the schema, and ignore extra columns in rows with more columns
        io_config (IOConfig): Config to be used with the native downloader
        use_native_downloader: Whether to use the native downloader instead of PyArrow for reading Parquet. This
            is currently experimental.

    returns:
        DataFrame: parsed DataFrame
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of CSV filepaths")

    if schema_hints is not None and schema_override is not None:
        raise ValueError("Cannot specify both schema_hints and schema_override")

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
    if use_native_downloader:
        storage_config = StorageConfig.native(NativeStorageConfig(True, io_config))
    else:
        storage_config = StorageConfig.python(PythonStorageConfig(io_config=io_config))
    builder = get_tabular_files_scan(
        path, schema_hints, schema_override, file_format_config, storage_config=storage_config
    )
    return DataFrame(builder)
