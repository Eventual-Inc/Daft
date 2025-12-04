# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from typing import Optional, Union

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder


@PublicAPI
def from_glob_path(path: Union[str, list[str]], io_config: Optional[IOConfig] = None) -> DataFrame:
    """Creates a DataFrame of file paths and other metadata from a glob path.

    This method supports wildcards:

    1. `*` matches any number of any characters including none
    2. `?` matches any single character
    3. `[...]` matches any single character in the brackets
    4. `**` recursively matches any number of layers of directories

    The returned DataFrame will have the following columns:

    1. path: the path to the file/directory
    2. size: size of the object in bytes
    3. rows: the total rows of parquet object, it's None for other formats.

    Args:
        path (str|list): Path to files on disk (allows wildcards).
        io_config (IOConfig): Configuration to use when running IO with remote services

    Returns:
        DataFrame: DataFrame containing the path to each file as a row, along with other metadata parsed from the provided filesystem.

    Raises:
        FileNotFoundError: If none of files found with the glob paths.

    Examples:
        >>> df = daft.from_glob_path("/path/to/files/*.jpeg")
        >>> df = daft.from_glob_path("/path/to/files/**/*.jpeg")
        >>> df = daft.from_glob_path("/path/to/files/**/image-?.jpeg")
        >>> df = daft.from_glob_path(["/path/to/files/*.jpeg", "/path/to/others/*.jpeg"])
    """
    if isinstance(path, str):
        path = [path]

    context = get_context()
    io_config = context.daft_planning_config.default_io_config if io_config is None else io_config

    builder = LogicalPlanBuilder.from_glob_scan(
        glob_paths=path,
        io_config=io_config,
    )
    return DataFrame(builder)
