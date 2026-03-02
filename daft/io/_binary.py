# isort: dont-add-import: from __future__ import annotations

from __future__ import annotations

from daft import context
from daft.api_annotations import PublicAPI
from daft.daft import (
    BinarySourceConfig,
    FileFormatConfig,
    IOConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_binary_files(
    path: str | list[str],
    *,
    file_path_column: str | None = None,
    hive_partitioning: bool = False,
    io_config: IOConfig | None = None,
    _buffer_size: int | None = None,
    _max_bytes: int | None = None,
) -> DataFrame:
    """Creates a DataFrame from binary file(s).

    Each file is read as a single row with a ``bytes`` column containing the full file contents.

    Args:
        path: Path(s) to binary files. Supports wildcards and remote URLs such as ``s3://`` or ``gs://``.
        file_path_column: Include the source path(s) as a column with this name. Defaults to ``None``.
        hive_partitioning: Whether to infer hive-style partitions from file paths and include them as columns in the DataFrame.
            Defaults to ``False``.
        io_config: IO configuration for the native downloader.
        _buffer_size: Optional tuning parameter for the underlying reader buffer size (bytes).
        _max_bytes: Optional limit to reject files larger than this many bytes.

    Returns:
        DataFrame: A DataFrame with a fixed ``bytes`` column.

    Examples:
        Read binary files from a local directory:

        >>> import daft
        >>> df = daft.read_binary_files("/path/to/dir")

        Read binary files and include their paths:

        >>> df = daft.read_binary_files("/path/to/files-*", file_path_column="path")
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from empty list of binary filepaths")

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    binary_config = BinarySourceConfig(buffer_size=_buffer_size, max_bytes=_max_bytes)
    file_format_config = FileFormatConfig.from_binary_config(binary_config)

    storage_config = StorageConfig(True, io_config)

    # Binary schema is fixed.
    schema = {"bytes": DataType.binary()}

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
