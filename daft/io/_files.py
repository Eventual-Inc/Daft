# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe import DataFrame
from daft.functions.file_ import file as new_file
from daft.logical.builder import LogicalPlanBuilder


@PublicAPI
def from_files(path: str | list[str], io_config: IOConfig | None = None) -> DataFrame:
    """Creates a DataFrame of :class:`~daft.File` references from a glob path.

    This method supports wildcards:

    1. ``*`` matches any number of any characters including none
    2. ``?`` matches any single character
    3. ``[...]`` matches any single character in the brackets
    4. ``**`` recursively matches any number of layers of directories

    The returned DataFrame will have a single ``"file"`` column of type :meth:`~daft.DataType.file`.
    Files are not downloaded eagerly; the ``File`` type is a lazy reference that can be read on demand.

    Args:
        path (str | list[str]): Path to files on disk (allows wildcards). Supports remote URLs such as ``s3://``, ``gs://``, or ``az://``.
        io_config (IOConfig): Configuration to use when running IO with remote services.

    Returns:
        DataFrame: DataFrame with a single ``"file"`` column containing :class:`~daft.File` references.

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
    if isinstance(path, str):
        path = [path]

    if len(path) == 0:
        raise ValueError("Must specify at least one glob path")

    context = get_context()
    io_config = context.daft_planning_config.default_io_config if io_config is None else io_config

    builder = LogicalPlanBuilder.from_glob_scan(
        glob_paths=path,
        io_config=io_config,
    )
    df = DataFrame(builder)

    return df.select(new_file(df["path"], io_config=io_config).alias("file"))
