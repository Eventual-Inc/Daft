from __future__ import annotations

import contextlib
import pathlib
from collections.abc import Generator
from typing import IO, Union
from uuid import uuid4

import fsspec
import pyarrow as pa
from pyarrow import csv as pacsv
from pyarrow import dataset as pads
from pyarrow import json as pajson
from pyarrow import parquet as papq
from pyarrow.fs import FileSystem

from daft.expressions import ExpressionsProjection
from daft.filesystem import _resolve_paths_and_filesystem
from daft.runners.partitioning import (
    vPartitionParseCSVOptions,
    vPartitionReadOptions,
)
from daft.logical.schema import Schema
from daft.runners.partitioning import vPartitionParseCSVOptions, vPartitionReadOptions
from daft.table import Table

FileInput = Union[pathlib.Path, str, IO[bytes]]


@contextlib.contextmanager
def _open_stream(
    file: FileInput,
    fs: FileSystem | fsspec.AbstractFileSystem | None,
) -> Generator[pa.NativeFile, None, None]:
    """Opens the provided file for reading, yield a pyarrow file handle."""
    if isinstance(file, (pathlib.Path, str)):
        paths, fs = _resolve_paths_and_filesystem(file, fs)
        assert len(paths) == 1
        path = paths[0]
        with fs.open_input_stream(path) as f:
            yield f
    else:
        yield file


def _from_arrow_table(table: pa.Table, schema: Schema, read_options: vPartitionReadOptions) -> Table:
    """Produces a Daft Table from an Arrow table, applying the provided schema and read_options correctly"""
    pruned_schema = (
        Schema._from_field_name_and_types([(c, schema[c].dtype) for c in read_options.column_names])
        if read_options.column_names is not None
        else schema
    )
    table = Table.from_arrow(table)
    table = table.cast_to_schema(pruned_schema)
    return table


def infer_schema_json(
    file: FileInput,
    fs: fsspec.AbstractFileSystem | None = None,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Schema:
    """Reads a Schema from a JSON file

    Args:
        file (FileInput): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        read_options (vPartitionReadOptions, optional): Options for reading the file

    Returns:
        Schema: Inferred Schema from the JSON
    """
    with _open_stream(file, fs) as f:
        table = pajson.read_json(f)

    if read_options.column_names is not None:
        table = table.select(read_options.column_names)

    return Table.from_arrow(table).schema()


def read_json_with_schema(
    file: FileInput,
    schema: Schema,
    fs: fsspec.AbstractFileSystem | None = None,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    """Reads a Table from a JSON file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        schema (Schema): Daft schema to read the JSON file into
        read_options (vPartitionReadOptions, optional): Options for reading the file

    Returns:
        Table: Parsed Table from JSON
    """
    with _open_stream(file, fs) as f:
        table = pajson.read_json(f)

    return _from_arrow_table(table, schema, read_options)


def read_parquet(
    file: FileInput,
    fs: fsspec.AbstractFileSystem | None = None,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    """Reads a Table from a Parquet file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        read_options (vPartitionReadOptions, optional): Options for reading the file

    Returns:
        Table: Parsed Table from Parquet
    """
    if not isinstance(file, (str, pathlib.Path)):
        # BytesIO path.
        return Table.from_arrow(papq.read_table(file, columns=read_options.column_names))

    paths, fs = _resolve_paths_and_filesystem(file, fs)
    assert len(paths) == 1
    path = paths[0]
    f = fs.open_input_file(path)
    pqf = papq.ParquetFile(f)
    # If no rows required, we manually construct an empty table with the right schema
    if read_options.num_rows == 0:
        arrow_schema = pqf.metadata.schema.to_arrow_schema()
        table = pa.Table.from_arrays([pa.array([], type=field.type) for field in arrow_schema], schema=arrow_schema)
    elif read_options.num_rows is not None:
        # Only read the required row groups.
        rows_needed = read_options.num_rows
        for i in range(pqf.metadata.num_row_groups):
            row_group_meta = pqf.metadata.row_group(i)
            rows_needed -= row_group_meta.num_rows
            if rows_needed <= 0:
                break
        table = pqf.read_row_groups(list(range(i + 1)), columns=read_options.column_names)
        if rows_needed < 0:
            # Need to truncate the table to the row limit.
            table = table.slice(length=read_options.num_rows)
    else:
        table = papq.read_table(
            f,
            columns=read_options.column_names,
        )

    return Table.from_arrow(table)


def read_csv_with_schema(
    file: FileInput,
    schema: Schema,
    fs: fsspec.AbstractFileSystem | None = None,
    csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    """Reads a Table from a CSV file and a provided Schema

    Args:
        file (FileInput): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        schema (Schema): Daft schema to read the CSV file into
        fs (AbstractFileSystem): FSSpec filesystem to use when reading
        csv_options (vPartitionParseCSVOptions, optional): options for reading the CSV file. Defaults to vPartitionParseCSVOptions().
        read_options (vPartitionReadOptions, optional): options for reading the Table. Defaults to vPartitionReadOptions().
    """
    with _open_stream(file, fs) as f:
        table = pacsv.read_csv(
            f,
            parse_options=pacsv.ParseOptions(
                delimiter=csv_options.delimiter,
            ),
            read_options=pacsv.ReadOptions(
                # Use the provided schema's field names as the column names, and skip parsing headers entirely
                column_names=schema.column_names(),
                skip_rows=(0 if csv_options.header_index is None else csv_options.header_index + 1),
            ),
            convert_options=pacsv.ConvertOptions(
                # Column pruning
                include_columns=read_options.column_names,
                # If any columns are missing, parse as null array
                include_missing_columns=True,
            ),
        )

    return _from_arrow_table(table, schema, read_options)


def infer_schema_csv(
    file: FileInput,
    fs: fsspec.AbstractFileSystem | None = None,
    override_column_names: list[str] | None = None,
    csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Schema:
    """Infers a Schema from a CSV file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        override_column_names (list[str]): column names to use instead of those found in the CSV - will throw an error if its length does not
            match the actual number of columns found in the CSV
        csv_options (vPartitionParseCSVOptions, optional): CSV-specific configs to apply when reading the file
        read_options (vPartitionReadOptions, optional): Options for reading the file

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
        # TODO(jay): Can't limit number of rows with current PyArrow filesystem so this reads the entire CSV to sample the schema
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
            convert_options=pacsv.ConvertOptions(include_columns=read_options.column_names),
        )

    return Table.from_arrow(table).schema()


def write_csv(
    table: Table,
    path: str | pathlib.Path,
    compression: str | None = None,
    partition_cols: ExpressionsProjection | None = None,
) -> list[str]:
    return _to_file(
        table=table,
        file_format="csv",
        path=path,
        partition_cols=partition_cols,
        compression=compression,
    )


def write_parquet(
    table: Table,
    path: str | pathlib.Path,
    compression: str | None = None,
    partition_cols: ExpressionsProjection | None = None,
) -> list[str]:
    return _to_file(
        table=table,
        file_format="parquet",
        path=path,
        partition_cols=partition_cols,
        compression=compression,
    )


def _to_file(
    table: Table,
    file_format: str,
    path: str | pathlib.Path,
    partition_cols: ExpressionsProjection | None = None,
    compression: str | None = None,
) -> list[str]:
    arrow_table = table.to_arrow()

    partitioning = [e.name() for e in (partition_cols or [])]
    if partitioning:
        # In partition cols, downcast large_string to string,
        # since pyarrow.dataset.write_dataset breaks for large_string partitioning columns.
        downcasted_schema = pa.schema(
            [
                pa.field(
                    name=field.name,
                    type=pa.string(),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                if field.name in partitioning and field.type == pa.large_string()
                else field
                for field in arrow_table.schema
            ]
        )
        arrow_table = arrow_table.cast(downcasted_schema)

    if file_format == "parquet":
        format = pads.ParquetFileFormat()
        opts = format.make_write_options(compression=compression)
    elif file_format == "csv":
        format = pads.CsvFileFormat()
        opts = None
        assert compression is None
    else:
        raise ValueError(f"Unsupported file format {file_format}")

    visited_paths = []

    def file_visitor(written_file):
        visited_paths.append(written_file.path)

    pads.write_dataset(
        arrow_table,
        base_dir=path,
        basename_template=str(uuid4()) + "-{i}." + format.default_extname,
        format=format,
        partitioning=partitioning,
        file_options=opts,
        file_visitor=file_visitor,
        use_threads=False,
        existing_data_behavior="overwrite_or_ignore",
    )

    return visited_paths
