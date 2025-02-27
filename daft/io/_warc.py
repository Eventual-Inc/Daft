# isort: dont-add-import: from __future__ import annotations

from typing import List, Optional, Union

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    FileFormatConfig,
    IOConfig,
    StorageConfig,
    WarcSourceConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType, TimeUnit
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_warc(
    path: Union[str, List[str]],
    io_config: Optional["IOConfig"] = None,
    # file_path_column: Optional[str] = None,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    """Creates a DataFrame from Parquet file(s).

    Example:
        >>> df = daft.read_parquet("/path/to/file.parquet")
        >>> df = daft.read_parquet("/path/to/directory")
        >>> df = daft.read_parquet("/path/to/files-*.parquet")
        >>> df = daft.read_parquet("s3://path/to/files-*.parquet")
        >>> df = daft.read_parquet("gs://path/to/files-*.parquet")

    Args:
        path (str): Path to Parquet file (allows for wildcards)
        io_config (IOConfig): Config to be used with the native downloader
        file_path_column: Include the source path(s) as a column with this name. Defaults to None.
        _multithreaded_io: Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    returns:
        DataFrame: parsed DataFrame
    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of Warc filepaths")

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = (
        (context.get_context().get_or_create_runner().name != "ray") if _multithreaded_io is None else _multithreaded_io
    )
    storage_config = StorageConfig(multithreaded_io, io_config)

    schema = {
        "warc_record_id": DataType.string(),
        "warc_type": DataType.string(),
        "warc_date": DataType.timestamp(TimeUnit.ns(), timezone="Etc/UTC"),
        "warc_content_length": DataType.int64(),
        "warc_content": DataType.binary(),
        "warc_header": DataType.string(),
    }

    warc_config = WarcSourceConfig()
    file_format_config = FileFormatConfig.from_warc_config(warc_config)

    builder = get_tabular_files_scan(
        path=path,
        infer_schema=False,
        schema=schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
        file_path_column=None,
        hive_partitioning=False,
    )
    return DataFrame(builder)
