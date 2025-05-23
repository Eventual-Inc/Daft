# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import Optional, Union

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
    path: Union[str, list[str]],
    io_config: Optional[IOConfig] = None,
    file_path_column: Optional[str] = None,
    _multithreaded_io: Optional[bool] = None,
) -> DataFrame:
    """Creates a DataFrame from WARC or gzipped WARC file(s). This is an experimental feature and the API may change in the future.

    Args:
        path (Union[str, List[str]]): Path to WARC file (allows for wildcards)
        io_config (Optional[IOConfig]): Config to be used with the native downloader
        file_path_column (Optional[str]): Include the source path(s) as a column with this name. Defaults to None.
        _multithreaded_io (Optional[bool]): Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    Returns:
        DataFrame: parsed DataFrame with mandatory metadata columns ("WARC-Record-ID", "WARC-Type", "WARC-Date", "Content-Length"), one optional
            metadata column ("WARC-Identified-Payload-Type"), one column "warc_content" with the raw byte content of the WARC record,
            and one column "warc_headers" with the remaining headers of the WARC record stored as a JSON string.

    Examples:
        >>> df = daft.read_warc("/path/to/file.warc")
        >>> df = daft.read_warc("/path/to/directory")
        >>> df = daft.read_warc("/path/to/files-*.warc")
        >>> df = daft.read_warc("s3://path/to/files-*.warc")
        >>> df = daft.read_warc("gs://path/to/files-*.warc")

    """
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from from empty list of Warc filepaths")

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections.
    multithreaded_io = (
        (context.get_context().get_or_create_runner().name != "ray") if _multithreaded_io is None else _multithreaded_io
    )
    storage_config = StorageConfig(multithreaded_io, io_config)

    schema = {
        "WARC-Record-ID": DataType.string(),
        "WARC-Type": DataType.string(),
        "WARC-Date": DataType.timestamp(TimeUnit.ns(), timezone="Etc/UTC"),
        "Content-Length": DataType.int64(),
        "WARC-Identified-Payload-Type": DataType.string(),
        "warc_content": DataType.binary(),
        "warc_headers": DataType.string(),
    }

    warc_config = WarcSourceConfig()
    file_format_config = FileFormatConfig.from_warc_config(warc_config)

    builder = get_tabular_files_scan(
        path=path,
        infer_schema=False,
        schema=schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
        file_path_column=file_path_column,
        hive_partitioning=False,
    )
    return DataFrame(builder)
