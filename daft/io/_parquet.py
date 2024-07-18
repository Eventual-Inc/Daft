# isort: dont-add-import: from __future__ import annotations

from typing import Dict, List, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    FileFormatConfig,
    IOConfig,
    NativeStorageConfig,
    ParquetSourceConfig,
    PythonStorageConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType, TimeUnit
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_parquet(
    path: Union[str, List[str]],
    row_groups: Optional[List[List[int]]] = None,
    schema_hints: Optional[Dict[str, DataType]] = None,
    io_config: Optional["IOConfig"] = None,
    use_native_downloader: bool = True,
    coerce_int96_timestamp_unit: Optional[Union[str, TimeUnit]] = None,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    """Creates a DataFrame from Parquet file(s)

    Example:
        >>> df = daft.read_parquet("/path/to/file.parquet")
        >>> df = daft.read_parquet("/path/to/directory")
        >>> df = daft.read_parquet("/path/to/files-*.parquet")
        >>> df = daft.read_parquet("s3://path/to/files-*.parquet")
        >>> df = daft.read_parquet("gs://path/to/files-*.parquet")

    Args:
        path (str): Path to Parquet file (allows for wildcards)
        row_groups (List[int] or List[List[int]]): List of row groups to read corresponding to each file.
        schema_hints (dict[str, DataType]): A mapping between column names and datatypes - passing this option
            will override the specified columns on the inferred schema with the specified DataTypes
        io_config (IOConfig): Config to be used with the native downloader
        use_native_downloader: Whether to use the native downloader instead of PyArrow for reading Parquet.
        coerce_int96_timestamp_unit: TimeUnit to coerce Int96 TimeStamps to. e.g.: [ns, us, ms], Defaults to None.
        _multithreaded_io: Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    returns:
        DataFrame: parsed DataFrame
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of Parquet filepaths")

    is_ray_runner = context.get_context().is_ray_runner
    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = not is_ray_runner if _multithreaded_io is None else _multithreaded_io

    if isinstance(coerce_int96_timestamp_unit, str):
        coerce_int96_timestamp_unit = TimeUnit.from_str(coerce_int96_timestamp_unit)

    pytimeunit = coerce_int96_timestamp_unit._timeunit if coerce_int96_timestamp_unit is not None else None

    if isinstance(path, list) and row_groups is not None and len(path) != len(row_groups):
        raise ValueError("row_groups must be the same length as the list of paths provided.")
    if isinstance(row_groups, list) and not isinstance(path, list):
        raise ValueError("row_groups are only supported when reading multiple non-globbed/wildcarded files")

    file_format_config = FileFormatConfig.from_parquet_config(
        ParquetSourceConfig(coerce_int96_timestamp_unit=pytimeunit, row_groups=row_groups)
    )
    if use_native_downloader:
        storage_config = StorageConfig.native(NativeStorageConfig(multithreaded_io, io_config))
    else:
        storage_config = StorageConfig.python(PythonStorageConfig(io_config=io_config))

    builder = get_tabular_files_scan(
        path=path,
        infer_schema=True,
        schema=schema_hints,
        file_format_config=file_format_config,
        storage_config=storage_config,
        is_ray_runner=is_ray_runner,
    )
    return DataFrame(builder)
