# isort: dont-add-import: from __future__ import annotations

from typing import Dict, List, Optional, Union

import fsspec

from daft.api_annotations import PublicAPI
from daft.daft import (
    FileFormatConfig,
    JsonSourceConfig,
    PythonStorageConfig,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io.common import _get_tabular_files_scan


@PublicAPI
def read_json(
    path: Union[str, List[str]],
    schema_hints: Optional[Dict[str, DataType]] = None,
    fs: Optional[fsspec.AbstractFileSystem] = None,
) -> DataFrame:
    """Creates a DataFrame from line-delimited JSON file(s)

    Example:
        >>> df = daft.read_json("/path/to/file.json")
        >>> df = daft.read_json("/path/to/directory")
        >>> df = daft.read_json("/path/to/files-*.json")
        >>> df = daft.read_json("s3://path/to/files-*.json")

    Args:
        path (str): Path to JSON files (allows for wildcards)
        schema_hints (dict[str, DataType]): A mapping between column names and datatypes - passing this option will
            disable all schema inference on data being read, and throw an error if data being read is incompatible.
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.

    returns:
        DataFrame: parsed DataFrame
    """
    if isinstance(path, list) and len(path) == 0:
        raise ValueError(f"Cannot read DataFrame from from empty list of JSON filepaths")

    json_config = JsonSourceConfig()
    file_format_config = FileFormatConfig.from_json_config(json_config)
    storage_config = StorageConfig.python(PythonStorageConfig(fs))
    builder = _get_tabular_files_scan(path, schema_hints, file_format_config, storage_config=storage_config)
    return DataFrame(builder)
