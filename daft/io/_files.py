# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from daft.api_annotations import PublicAPI
from daft.daft import IOConfig
from daft.dataframe import DataFrame
from daft.expressions import col
from daft.functions.file_ import file as new_file
from daft.io.file_path import from_glob_path


@PublicAPI
def from_files(path: str | list[str], io_config: IOConfig | None = None) -> DataFrame:
    """Creates a DataFrame of `daft.File` references from a glob path.

    This method supports wildcards:

    1. ``*`` matches any number of any characters including none
    2. ``?`` matches any single character
    3. ``[...]`` matches any single character in the brackets
    4. ``**`` recursively matches any number of layers of directories

    The returned DataFrame will have a single ``"file"`` column of type `daft.DataType.file`.
    Files are not downloaded eagerly; the ``File`` type is a lazy reference that can be read on demand.

    Args:
        path (str | list[str]): Path to files on disk (allows wildcards). Supports remote URLs such as ``s3://``, ``gs://``, or ``az://``.
        io_config (IOConfig | None): Configuration to use when running IO with remote services.

    Returns:
        DataFrame: DataFrame with a single ``"file"`` column containing `daft.File` references.

    Note:
        If no files match the glob pattern(s), an empty DataFrame is returned instead of raising an error.

    Examples:
        Read all JPEG files under a directory:

        >>> import daft
        >>> df = daft.from_files("/path/to/files/*.jpeg")  # doctest: +SKIP

        Read recursively:

        >>> df = daft.from_files("/path/to/files/**/*.jpeg")  # doctest: +SKIP

        Read from S3:

        >>> df = daft.from_files("s3://my-bucket/images/*.png")  # doctest: +SKIP

        Read from multiple glob patterns:

        >>> df = daft.from_files(["/path/to/files/*.jpeg", "/path/to/others/*.jpeg"])  # doctest: +SKIP
    """
    return from_glob_path(path, io_config=io_config).select(new_file(col("path"), io_config=io_config).alias("file"))
