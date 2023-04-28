# isort: dont-add-import: from __future__ import annotations

from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datasources import JSONSourceInfo
from daft.io.common import _get_tabular_files_scan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


@PublicAPI
def read_json(
    path: str,
) -> DataFrame:
    """Creates a DataFrame from line-delimited JSON file(s)

    Example:
        >>> df = daft.read_json("/path/to/file.json")
        >>> df = daft.read_json("/path/to/directory")
        >>> df = daft.read_json("/path/to/files-*.json")
        >>> df = daft.read_json("s3://path/to/files-*.json")

    Args:
        path (str): Path to JSON files (allows for wildcards)

    returns:
        DataFrame: parsed DataFrame
    """
    plan = _get_tabular_files_scan(
        path,
        JSONSourceInfo(),
        vPartitionSchemaInferenceOptions(),
    )
    return DataFrame(plan)
