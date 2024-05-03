# isort: dont-add-import: from __future__ import annotations


from typing import Optional

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import IOConfig
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder
from daft.runners.pyrunner import LocalPartitionSet
from daft.table import MicroPartition


@PublicAPI
def from_glob_path(path: str, io_config: Optional[IOConfig] = None) -> DataFrame:
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
        io_config (IOConfig): Configuration to use when running IO with remote services

    Returns:
        DataFrame: DataFrame containing the path to each file as a row, along with other metadata
            parsed from the provided filesystem.
    """
    context = get_context()
    io_config = context.daft_planning_config.default_io_config if io_config is None else io_config
    runner_io = context.runner().runner_io()
    file_infos = runner_io.glob_paths_details([path], io_config=io_config)
    file_infos_table = MicroPartition._from_pytable(file_infos.to_table())
    partition = LocalPartitionSet()
    partition.set_partition_from_table(0, file_infos_table)
    cache_entry = context.runner().put_partition_set_into_cache(partition)
    size_bytes = partition.size_bytes()
    num_rows = len(partition)

    assert size_bytes is not None, "In-memory data should always have non-None size in bytes"
    builder = LogicalPlanBuilder.from_in_memory_scan(
        cache_entry,
        schema=file_infos_table.schema(),
        num_partitions=partition.num_partitions(),
        size_bytes=size_bytes,
        num_rows=num_rows,
    )
    return DataFrame(builder)
