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
from daft.datatype import DataType
from daft.io.common import _get_tabular_files_scan


@PublicAPI
def read_parquet(
    path: Union[str, List[str]],
    schema_hints: Optional[Dict[str, DataType]] = None,
    io_config: Optional["IOConfig"] = None,
    use_native_downloader: bool = True,
    _multithreaded_io: Optional[bool] = None,
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
        use_native_downloader: Whether to use the native downloader instead of PyArrow for reading Parquet. This
            is currently experimental.
        _multithreaded_io: Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    returns:
        DataFrame: parsed DataFrame
    """

    if isinstance(path, list) and len(path) == 0:
        raise ValueError(f"Cannot read DataFrame from from empty list of Parquet filepaths")

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = not context.get_context().is_ray_runner if _multithreaded_io is None else _multithreaded_io

    file_format_config = FileFormatConfig.from_parquet_config(
        ParquetSourceConfig(
            multithreaded_io=multithreaded_io,
        )
    )
    if use_native_downloader:
        storage_config = StorageConfig.native(NativeStorageConfig(io_config))
    else:
        storage_config = StorageConfig.python(PythonStorageConfig(None))

    builder = _get_tabular_files_scan(path, schema_hints, file_format_config, storage_config=storage_config)
    return DataFrame(builder)
