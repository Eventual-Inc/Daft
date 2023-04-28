# isort: dont-add-import: from __future__ import annotations

from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datasources import ParquetSourceInfo
from daft.io.common import _get_tabular_files_scan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


@PublicAPI
def read_parquet(path: str) -> DataFrame:
    """Creates a DataFrame from Parquet file(s)

    Example:
        >>> df = DataFrame.read_parquet("/path/to/file.parquet")
        >>> df = DataFrame.read_parquet("/path/to/directory")
        >>> df = DataFrame.read_parquet("/path/to/files-*.parquet")
        >>> df = DataFrame.read_parquet("s3://path/to/files-*.parquet")

    Args:
        path (str): Path to Parquet file (allows for wildcards)

    returns:
        DataFrame: parsed DataFrame
    """
    plan = _get_tabular_files_scan(
        path,
        ParquetSourceInfo(),
        vPartitionSchemaInferenceOptions(),
    )
    return DataFrame(plan)
