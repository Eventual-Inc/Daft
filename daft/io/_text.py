# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from daft import DataType, context
from daft.api_annotations import PublicAPI
from daft.daft import FileFormatConfig, IOConfig, StorageConfig, TextSourceConfig
from daft.dataframe import DataFrame
from daft.io.common import get_tabular_files_scan


@PublicAPI
def read_text(
    path: str | list[str],
    *,
    encoding: str = "utf-8",
    skip_blank_lines: bool = True,
    file_path_column: str | None = None,
    hive_partitioning: bool = False,
    io_config: IOConfig | None = None,
    _buffer_size: int | None = None,
    _chunk_size: int | None = None,
) -> DataFrame:
    """Creates a DataFrame from line-oriented text file(s).

    Args:
        path: Path to text file(s). Supports wildcards and remote URLs such as ``s3://`` or ``gs://``.
        encoding: Encoding of the input files, defaults to ``"utf-8"``.
        skip_blank_lines: Whether to skip empty lines (after stripping whitespace). Defaults to ``True``.
        file_path_column: Include the source path(s) as a column with this name. Defaults to ``None``.
        hive_partitioning: Whether to infer hive-style partitions from file paths and include them as
            columns in the DataFrame. Defaults to ``False``.
        io_config: IO configuration for the native downloader.
        _buffer_size: Optional tuning parameter for the underlying streaming reader buffer size (bytes).
        _chunk_size: Optional tuning parameter for the underlying streaming reader chunk size (rows).

    Returns:
        DataFrame: A DataFrame with a single ``"text"`` column containing lines from the input files.

    Examples:
        Read a text file from a local path:

        >>> import daft
        >>> df = daft.read_text("/path/to/file.txt")
        >>> df.show()

        Read a text file from a public S3 bucket:

        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_text("s3://path/to/files-*.txt", io_config=io_config)
        >>> df.show()
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from empty list of text filepaths")

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    text_config = TextSourceConfig(
        encoding=encoding,
        skip_blank_lines=skip_blank_lines,
        buffer_size=_buffer_size,
        chunk_size=_chunk_size,
    )
    file_format_config = FileFormatConfig.from_text_config(text_config)
    storage_config = StorageConfig(True, io_config)

    # Text schema is fixed
    schema = {"text": DataType.string()}
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
