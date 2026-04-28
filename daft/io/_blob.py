# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from daft import DataType, context
from daft.api_annotations import PublicAPI
from daft.daft import BlobSourceConfig, FileFormatConfig, IOConfig, StorageConfig
from daft.dataframe import DataFrame
from daft.datatype import TimeUnit
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_blob(
    path: str | list[str],
    *,
    file_path_column: str | None = None,
    hive_partitioning: bool = False,
    io_config: IOConfig | None = None,
    _buffer_size: int | None = None,
) -> DataFrame:
    """Creates a DataFrame from arbitrary binary file(s).

    Args:
        path: Path to binary file(s). Supports wildcards and remote URLs such as ``s3://`` or ``gs://``.
        file_path_column: Include the source path(s) as a column with this name. Defaults to ``None``.
        hive_partitioning: Whether to infer hive-style partitions from file paths and include them as
            columns in the DataFrame. Defaults to ``False``.
        io_config: IO configuration for the native downloader.
        _buffer_size: Optional tuning parameter for the underlying streaming reader buffer size (bytes).

    Returns:
        DataFrame: A DataFrame with columns ``content`` (``Binary``), ``size`` (``Int64``),
            and ``last_modified`` (``Timestamp(us, UTC)``). ``size`` and ``last_modified`` are
            ``NULL`` when the underlying filesystem does not expose the metadata.

    Examples:
        Read all files in a directory as blobs:

        >>> import daft
        >>> df = daft.read_blob("/path/to/files/*")  # doctest: +SKIP
        >>> df.show()  # doctest: +SKIP

        Read only the sizes without opening the files:

        >>> sizes = daft.read_blob("/path/to/files/*").select("size")  # doctest: +SKIP
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from empty list of blob filepaths")

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    blob_config = BlobSourceConfig(buffer_size=_buffer_size)
    file_format_config = FileFormatConfig.from_blob_config(blob_config)
    storage_config = StorageConfig(True, io_config)

    # Blob schema is fixed
    schema = {
        "content": DataType.binary(),
        "size": DataType.int64(),
        "last_modified": DataType.timestamp(TimeUnit.us(), timezone="Etc/UTC"),
    }
    builder = get_tabular_files_scan(
        path=path,
        infer_schema=False,
        schema=schema,
        file_format_config=file_format_config,
        storage_config=storage_config,
        file_path_column=file_path_column,
        hive_partitioning=hive_partitioning,
    )
    return DataFrame(builder)
