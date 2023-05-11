from __future__ import annotations

import contextlib
import io
import pathlib
from typing import IO, Iterator, Union
from uuid import uuid4

import pyarrow as pa
from pyarrow import csv as pacsv
from pyarrow import dataset as pads
from pyarrow import json as pajson
from pyarrow import parquet as papq

from daft.expressions import ExpressionsProjection
from daft.filesystem import get_filesystem_from_path
from daft.logical.schema import Schema
from daft.runners.partitioning import vPartitionParseCSVOptions, vPartitionReadOptions
from daft.table import Table

FileInput = Union[pathlib.Path, str, IO[bytes]]


def _limit_num_rows(buf: IO, num_rows: int) -> IO:
    """Limites a buffer to a certain number of rows using an in-memory buffer."""
    sampled_bytes = io.BytesIO()
    for i, line in enumerate(buf):
        if i >= num_rows:
            break
        sampled_bytes.write(line)
    sampled_bytes.seek(0)
    return sampled_bytes


@contextlib.contextmanager
def _get_file(file: FileInput) -> Iterator[IO[bytes]]:
    """Helper method to return a file handle if input is a string."""
    if isinstance(file, str):
        fs = get_filesystem_from_path(file)
        with fs.open(file, compression="infer") as f:
            yield f
    elif isinstance(file, pathlib.Path):
        fs = get_filesystem_from_path(str(file))
        with fs.open(file, compression="infer") as f:
            yield f
    else:
        yield file


def read_json(
    file: FileInput,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    """Reads a Table from a JSON file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        read_options (vPartitionReadOptions, optional): Options for reading the file

    Returns:
        Table: Parsed Table from JSON
    """
    with _get_file(file) as f:
        if read_options.num_rows is not None:
            f = _limit_num_rows(f, read_options.num_rows)
        table = pajson.read_json(f)

    if read_options.column_names is not None:
        table = table.select(read_options.column_names)

    return Table.from_arrow(table)


def read_parquet(
    file: FileInput,
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    """Reads a Table from a Parquet file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        read_options (vPartitionReadOptions, optional): Options for reading the file

    Returns:
        Table: Parsed Table from Parquet
    """
    with _get_file(file) as f:
        pqf = papq.ParquetFile(f)
        # If no rows required, we manually construct an empty table with the right schema
        if read_options.num_rows == 0:
            arrow_schema = pqf.metadata.schema.to_arrow_schema()
            table = pa.Table.from_arrays([pa.array([], type=field.type) for field in arrow_schema], schema=arrow_schema)
        elif read_options.num_rows is not None:
            # Read the file by rowgroup.
            tables = []
            rows_read = 0
            for i in range(pqf.metadata.num_row_groups):
                tables.append(pqf.read_row_group(i, columns=read_options.column_names))
                rows_read += len(tables[i])
                if rows_read >= read_options.num_rows:
                    break
            table = pa.concat_tables(tables)
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
    csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Table:
    """Reads a Table from a CSV file and a provided Schema

    Args:
        file (FileInput): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        schema (Schema): Daft schema to read the CSV file into
        csv_options (vPartitionParseCSVOptions, optional): options for reading the CSV file. Defaults to vPartitionParseCSVOptions().
        read_options (vPartitionReadOptions, optional): options for reading the Table. Defaults to vPartitionReadOptions().
    """
    with _get_file(file) as f:

        if read_options.num_rows is not None:
            num_rows_to_read = (
                # Extra rows before the header
                0
                if csv_options.header_index is None
                else csv_options.header_index
                # Header row
                + (1 if csv_options.header_index is not None else 0)
                # Actual number of data rows to read
                + read_options.num_rows
            )
            f = _limit_num_rows(f, num_rows_to_read)

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

    pruned_schema = (
        Schema._from_field_name_and_types([(c, schema[c].dtype) for c in read_options.column_names])
        if read_options.column_names is not None
        else schema
    )
    table = Table.from_arrow(table)
    table = table.cast_to_schema(pruned_schema)
    return table


def infer_schema_csv(
    file: FileInput,
    override_column_names: list[str] | None = None,
    csv_options: vPartitionParseCSVOptions = vPartitionParseCSVOptions(),
    read_options: vPartitionReadOptions = vPartitionReadOptions(),
) -> Schema:
    """Reads a Schema from a CSV file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
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

    with _get_file(file) as f:

        if read_options.num_rows is not None:
            num_rows_to_read = (
                # Extra rows before the header
                (0 if csv_options.header_index is None else csv_options.header_index)
                # Header row
                + (1 if csv_options.header_index is not None else 0)
                # Actual number of data rows to read
                + read_options.num_rows
            )
            f = _limit_num_rows(f, num_rows_to_read)

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
