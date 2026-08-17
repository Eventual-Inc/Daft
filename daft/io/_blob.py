# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import Literal

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe import DataFrame
from daft.expressions import col
from daft.functions.url import download
from daft.io.file_path import from_glob_path


@PublicAPI
def read_blob(
    path: str | list[str],
    *,
    max_connections: int = 32,
    on_error: Literal["raise", "null"] = "raise",
    io_config: IOConfig | None = None,
) -> DataFrame:
    """Creates a DataFrame from the contents of file(s), where each file is read as a single row of raw bytes.

    This is similar to DuckDB's `read_blob` function, and is useful for reading files that are not stored
    in a tabular format, such as images, audio files, or other binary data.

    This method supports wildcards:

    1. `*` matches any number of any characters including none
    2. `?` matches any single character
    3. `[...]` matches any single character in the brackets
    4. `**` recursively matches any number of layers of directories

    The returned DataFrame will have the following columns:

    1. path: the path to the file
    2. size: size of the file in bytes
    3. content: the raw bytes contents of the file

    Args:
        path (str|list): Path to file(s) on disk or remote object stores such as ``s3://`` or ``gs://`` (allows wildcards).
        max_connections: The maximum number of connections to use per thread to use for downloading file contents. Defaults to 32.
        on_error: Behavior when a file download error is encountered - "raise" to raise the error immediately or "null" to log
            the error but fallback to a Null value. Defaults to "raise".
        io_config (IOConfig): Configuration to use when running IO with remote services

    Returns:
        DataFrame: DataFrame with one row per file, containing the path, size and raw bytes contents of each file.

    Examples:
        Read files from a local path:

        >>> df = daft.read_blob("/path/to/files/*.jpeg")  # doctest: +SKIP
        >>> df = daft.read_blob("/path/to/files/**/*.jpeg")  # doctest: +SKIP

        Read files from a public S3 bucket:

        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_blob("s3://path/to/files-*.jpeg", io_config=io_config)  # doctest: +SKIP
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError("Cannot read DataFrame from empty list of blob filepaths")

    io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config

    df = from_glob_path(path, io_config=io_config)
    return df.select(
        col("path"),
        col("size"),
        download(
            col("path"),
            max_connections=max_connections,
            on_error=on_error,
            io_config=io_config,
        ).alias("content"),
    )
