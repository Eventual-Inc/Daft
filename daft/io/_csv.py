# isort: dont-add-import: from __future__ import annotations

import warnings
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

            .. deprecated:: 0.2.27
                Schema hints are deprecated and will be removed in the next release. Please use `schema` and `infer_schema` instead.
        infer_schema (bool): Whether to infer the schema of the CSV, defaults to True.
        schema (dict[str, DataType]): A schema that is used as the definitive schema for the CSV if infer_schema is False, otherwise it is used as a schema hint that is applied after the schema is inferred.
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

    if schema_hints is not None:
        warnings.warn("schema_hints is deprecated and will be removed in a future release. Please use schema instead.")
        if schema is None:
            schema = schema_hints

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
        is_ray_runner=context.get_context().is_ray_runner,
    )
    return DataFrame(builder)
