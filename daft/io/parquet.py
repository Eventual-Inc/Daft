# isort: dont-add-import: from __future__ import annotations

from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datasources import ParquetSourceInfo
from daft.io.common import get_tabular_files_scan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


@PublicAPI
def read_parquet(path: str) -> DataFrame:
    """Creates a DataFrame from Parquet file(s)

    Example:
        >>> df = daft.read_parquet("/path/to/file.parquet")
        >>> df = daft.read_parquet("/path/to/directory")
        >>> df = daft.read_parquet("/path/to/files-*.parquet")
        >>> df = daft.read_parquet("s3://path/to/files-*.parquet")

    Args:
        path (str): Path to Parquet file (allows for wildcards)

    returns:
        DataFrame: parsed DataFrame
    """
    plan = get_tabular_files_scan(
        path,
        # TODO(jay): Allow passing of schema hints here
        None,
        ParquetSourceInfo(),
        vPartitionSchemaInferenceOptions(),
    )
    return DataFrame(plan)
