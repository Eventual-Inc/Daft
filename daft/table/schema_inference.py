from __future__ import annotations

import pathlib

import pyarrow.csv as pacsv
import pyarrow.json as pajson

from daft.daft import NativeStorageConfig
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions
from daft.table import Table
from daft.table.table_io import FileInput, _open_stream


def from_csv(
    file: FileInput,
    csv_options: TableParseCSVOptions = TableParseCSVOptions(),
) -> Schema:
    """Infers a Schema from a CSV file
    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        csv_options (vPartitionParseCSVOptions, optional): CSV-specific configs to apply when reading the file
        read_options (TableReadOptions, optional): Options for reading the file
    Returns:
        Schema: Inferred Schema from the CSV
    """
    # Have PyArrow generate the column names if user specifies that there are no headers
    pyarrow_autogenerate_column_names = csv_options.header_index is None

    with _open_stream(file) as f:
        table = pacsv.read_csv(
            f,
            parse_options=pacsv.ParseOptions(
                delimiter=csv_options.delimiter,
            ),
            read_options=pacsv.ReadOptions(
                autogenerate_column_names=pyarrow_autogenerate_column_names,
            ),
        )

    return Table.from_arrow(table).schema()


def from_json(
    file: FileInput,
) -> Schema:
    """Reads a Schema from a JSON file

    Args:
        file (FileInput): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        read_options (TableReadOptions, optional): Options for reading the file

    Returns:
        Schema: Inferred Schema from the JSON
    """
    with _open_stream(file) as f:
        table = pajson.read_json(f)

    return Table.from_arrow(table).schema()


def from_parquet(
    file: FileInput,
    storage_config: NativeStorageConfig,
) -> Schema:
    """Infers a Schema from a Parquet file"""
    assert isinstance(file, (str, pathlib.Path)), "Native downloader only works on string inputs to read_parquet"
    io_config = storage_config.io_config
    return Schema.from_parquet(str(file), io_config=io_config)
