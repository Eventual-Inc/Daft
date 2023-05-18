# isort: dont-add-import: from __future__ import annotations

from typing import Optional

import fsspec

from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datasources import ParquetSourceInfo
from daft.io.common import _get_tabular_files_scan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


@PublicAPI
def read_parquet(path: str, fs: Optional[fsspec.AbstractFileSystem] = None) -> DataFrame:
    """Creates a DataFrame from Parquet file(s)

    Example:
        >>> df = daft.read_parquet("/path/to/file.parquet")
        >>> df = daft.read_parquet("/path/to/directory")
        >>> df = daft.read_parquet("/path/to/files-*.parquet")
        >>> df = daft.read_parquet("s3://path/to/files-*.parquet")

    Args:
        path (str): Path to Parquet file (allows for wildcards)
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.

    returns:
        DataFrame: parsed DataFrame
    """
    plan = _get_tabular_files_scan(
        path,
        ParquetSourceInfo(),
        fs,
        vPartitionSchemaInferenceOptions(),
    )
    return DataFrame(plan)
