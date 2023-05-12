# isort: dont-add-import: from __future__ import annotations

from typing import Optional

<<<<<<< HEAD
import fsspec

=======
>>>>>>> e374c88f (thread schema arg to top-level read APIs)
from daft.api_annotations import PublicAPI
from daft.dataframe import DataFrame
from daft.datasources import JSONSourceInfo
from daft.io.common import UserProvidedSchemaHints, get_tabular_files_scan
from daft.runners.partitioning import vPartitionSchemaInferenceOptions


@PublicAPI
def read_json(
    path: str,
    fs: Optional[fsspec.AbstractFileSystem] = None,
    schema_hints: Optional[UserProvidedSchemaHints] = None,
) -> DataFrame:
    """Creates a DataFrame from line-delimited JSON file(s)

    Example:
        >>> df = daft.read_json("/path/to/file.json")
        >>> df = daft.read_json("/path/to/directory")
        >>> df = daft.read_json("/path/to/files-*.json")
        >>> df = daft.read_json("s3://path/to/files-*.json")

    Args:
        path (str): Path to JSON files (allows for wildcards)
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        schema_hints (Optional[UserProvidedSchemaHints]): A mapping between column names and datatypes - passing this option will
            disable all type inference on the JSON files being read, and throw a runtime error if data being read is incompatible. Defaults to None.

    returns:
        DataFrame: parsed DataFrame
    """
    plan = get_tabular_files_scan(
        path,
        schema_hints,
        JSONSourceInfo(),
        fs,
        vPartitionSchemaInferenceOptions(),
    )
    return DataFrame(plan)
