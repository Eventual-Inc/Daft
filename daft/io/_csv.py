# isort: dont-add-import: from __future__ import annotations

from typing import List, Optional

import fsspec

from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datasources import CSVSourceInfo
from daft.io.common import _get_tabular_files_scan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


@PublicAPI
def read_csv(
    path: str,
    fs: Optional[fsspec.AbstractFileSystem] = None,
    has_headers: bool = True,
    column_names: Optional[List[str]] = None,
    delimiter: str = ",",
) -> DataFrame:
    """Creates a DataFrame from CSV file(s)

    Example:
        >>> df = daft.read_csv("/path/to/file.csv")
        >>> df = daft.read_csv("/path/to/directory")
        >>> df = daft.read_csv("/path/to/files-*.csv")
        >>> df = daft.read_csv("s3://path/to/files-*.csv")

    Args:
        path (str): Path to CSV (allows for wildcards)
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        has_headers (bool): Whether the CSV has a header or not, defaults to True
        column_names (Optional[List[str]]): Custom column names to assign to the DataFrame, defaults to None
        delimiter (Str): Delimiter used in the CSV, defaults to ","

    returns:
        DataFrame: parsed DataFrame
    """
    plan = _get_tabular_files_scan(
        path,
        CSVSourceInfo(
            delimiter=delimiter,
            has_headers=has_headers,
        ),
        fs,
        vPartitionSchemaInferenceOptions(inference_column_names=column_names),
    )
    return DataFrame(plan)
