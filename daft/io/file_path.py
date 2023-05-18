# isort: dont-add-import: from __future__ import annotations

from typing import Optional

import fsspec

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.dataframe import DataFrame
from daft.logical import logical_plan


@PublicAPI
def from_glob_path(path: str, fs: Optional[fsspec.AbstractFileSystem] = None) -> DataFrame:
    """Creates a DataFrame of file paths and other metadata from a glob path.

    This method supports wildcards:

    1. "*" matches any number of any characters including none
    2. "?" matches any single character
    3. "[...]" matches any single character in the brackets
    4. "**" recursively matches any number of layers of directories

    The returned DataFrame will have the following columns:

    1. path: the path to the file/directory
    2. size: size of the object in bytes
    3. type: either "file" or "directory"

    Example:
        >>> df = daft.from_glob_path("/path/to/files/*.jpeg")
        >>> df = daft.from_glob_path("/path/to/files/**/*.jpeg")
        >>> df = daft.from_glob_path("/path/to/files/**/image-?.jpeg")

    Args:
        path (str): Path to files on disk (allows wildcards).
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for globbing and fetching metadata.
            By default, Daft will automatically construct a FileSystem instance internally.

    Returns:
        DataFrame: DataFrame containing the path to each file as a row, along with other metadata
            parsed from the provided filesystem.
    """
    runner_io = get_context().runner().runner_io()
    partition_set = runner_io.glob_paths_details(path, fs=fs)
    cache_entry = get_context().runner().put_partition_set_into_cache(partition_set)
    filepath_plan = logical_plan.InMemoryScan(
        cache_entry=cache_entry,
        schema=runner_io.FS_LISTING_SCHEMA,
        partition_spec=logical_plan.PartitionSpec(logical_plan.PartitionScheme.UNKNOWN, partition_set.num_partitions()),
    )
    return DataFrame(filepath_plan)
