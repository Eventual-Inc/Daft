from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

from daft.datatype import DataType
from daft.filesystem import _resolve_paths_and_filesystem
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions
from daft.table import Table
from daft.table.table_io import FileInput, _open_stream

if TYPE_CHECKING:
    import fsspec


def from_csv(
    file: FileInput,
    fs: fsspec.AbstractFileSystem | None = None,
    override_column_names: list[str] | None = None,
    csv_options: TableParseCSVOptions = TableParseCSVOptions(),
) -> Schema:
    """Infers a Schema from a CSV file
    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        override_column_names (list[str]): column names to use instead of those found in the CSV - will throw an error if its length does not
            match the actual number of columns found in the CSV
        csv_options (vPartitionParseCSVOptions, optional): CSV-specific configs to apply when reading the file
        read_options (TableReadOptions, optional): Options for reading the file
    Returns:
        Schema: Inferred Schema from the CSV
    """

    # Have PyArrow generate the column names if the CSV has no header and no column names were provided
    pyarrow_autogenerate_column_names = (csv_options.header_index is None) and (override_column_names is None)

    # Have Pyarrow skip the header row if override_column_names were provided, and a header exists in the CSV
    pyarrow_skip_rows_after_names = (
        1 if override_column_names is not None and csv_options.header_index is not None else 0
    )

    with _open_stream(file, fs) as f:
        table = pacsv.read_csv(
            f,
            parse_options=pacsv.ParseOptions(
                delimiter=csv_options.delimiter,
            ),
            # First skip_rows is applied, then header row is read if column_names is None, then skip_rows_after_names is applied
            read_options=pacsv.ReadOptions(
                autogenerate_column_names=pyarrow_autogenerate_column_names,
                column_names=override_column_names,
                skip_rows_after_names=pyarrow_skip_rows_after_names,
                skip_rows=csv_options.header_index,
            ),
        )

    return Table.from_arrow(table).schema()


def from_json(
    file: FileInput,
    fs: fsspec.AbstractFileSystem | None = None,
) -> Schema:
    """Reads a Schema from a JSON file

    Args:
        file (FileInput): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        read_options (TableReadOptions, optional): Options for reading the file

    Returns:
        Schema: Inferred Schema from the JSON
    """
    with _open_stream(file, fs) as f:
        table = pajson.read_json(f)

    return Table.from_arrow(table).schema()


def from_parquet(
    file: FileInput,
    fs: fsspec.AbstractFileSystem | None = None,
) -> Schema:
    """Infers a Schema from a Parquet file"""
    if not isinstance(file, (str, pathlib.Path)):
        # BytesIO path.
        f = file
    else:
        paths, fs = _resolve_paths_and_filesystem(file, fs)
        assert len(paths) == 1
        path = paths[0]
        f = fs.open_input_file(path)

    pqf = papq.ParquetFile(f)
    arrow_schema = pqf.metadata.schema.to_arrow_schema()

    return Schema._from_field_name_and_types([(f.name, DataType.from_arrow_type(f.type)) for f in arrow_schema])
