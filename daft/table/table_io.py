from __future__ import annotations

import contextlib
import math
import pathlib
from collections.abc import Callable, Generator
from typing import IO, Any, Union
from uuid import uuid4

import pyarrow as pa
from pyarrow import csv as pacsv
from pyarrow import dataset as pads
from pyarrow import json as pajson
from pyarrow import parquet as papq

from daft.context import get_context
from daft.daft import (
    CsvConvertOptions,
    CsvParseOptions,
    CsvReadOptions,
    FileFormat,
    IOConfig,
    JsonConvertOptions,
    JsonParseOptions,
    JsonReadOptions,
    NativeStorageConfig,
    PythonStorageConfig,
    StorageConfig,
)
from daft.datatype import DataType
from daft.expressions import ExpressionsProjection
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    TableParseCSVOptions,
    TableParseParquetOptions,
    TableReadOptions,
)
from daft.series import Series
from daft.table import MicroPartition

FileInput = Union[pathlib.Path, str, IO[bytes]]


@contextlib.contextmanager
def _open_stream(
    file: FileInput,
    io_config: IOConfig | None,
) -> Generator[pa.NativeFile, None, None]:
    """Opens the provided file for reading, yield a pyarrow file handle."""
    if isinstance(file, (pathlib.Path, str)):
        paths, fs = _resolve_paths_and_filesystem(file, io_config=io_config)
        assert len(paths) == 1
        path = paths[0]
        with fs.open_input_stream(path) as f:
            yield f
    else:
        yield file


def _cast_table_to_schema(table: MicroPartition, read_options: TableReadOptions, schema: Schema) -> pa.Table:
    """Performs a cast of a Daft MicroPartition to the requested Schema/Data. This is required because:

    1. Data read from the datasource may have types that do not match the inferred global schema
    2. Data read from the datasource may have columns that are out-of-order with the inferred schema
    3. We may need only a subset of columns, or differently-ordered columns, in `read_options`

    This helper function takes care of all that, ensuring that the resulting MicroPartition has all column types matching
    their corresponding dtype in `schema`, and column ordering/inclusion matches `read_options.column_names` (if provided).
    """
    pruned_schema = schema

    # If reading only a subset of fields, prune the schema
    if read_options.column_names is not None:
        pruned_schema = Schema._from_fields([schema[name] for name in read_options.column_names])

    table = table.cast_to_schema(pruned_schema)
    return table


def read_json(
    file: FileInput,
    schema: Schema,
    storage_config: StorageConfig | None = None,
    json_read_options: JsonReadOptions | None = None,
    read_options: TableReadOptions = TableReadOptions(),
) -> MicroPartition:
    """Reads a MicroPartition from a JSON file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        json_read_options (JsonReadOptions, optional): JSON-specific configs to apply when reading the file
        read_options (TableReadOptions, optional): Non-format-specific options for reading the file

    Returns:
        MicroPartition: Parsed MicroPartition from JSON
    """
    io_config = None
    if storage_config is not None:
        config = storage_config.config
        if isinstance(config, NativeStorageConfig):
            assert isinstance(file, (str, pathlib.Path)), "Native downloader only works on string inputs to read_json"
            json_convert_options = JsonConvertOptions(
                limit=read_options.num_rows,
                include_columns=read_options.column_names,
                schema=schema._schema if schema is not None else None,
            )
            json_parse_options = JsonParseOptions()
            tbl = MicroPartition.read_json(
                str(file),
                convert_options=json_convert_options,
                parse_options=json_parse_options,
                read_options=json_read_options,
                io_config=config.io_config,
            )
            return _cast_table_to_schema(tbl, read_options=read_options, schema=schema)
        else:
            assert isinstance(config, PythonStorageConfig)
            io_config = config.io_config

    with _open_stream(file, io_config) as f:
        table = pajson.read_json(f)

    if read_options.column_names is not None:
        table = table.select(read_options.column_names)

    # TODO(jay): Can't limit number of rows with current PyArrow filesystem so we have to shave it off after the read
    if read_options.num_rows is not None:
        table = table[: read_options.num_rows]

    return _cast_table_to_schema(MicroPartition.from_arrow(table), read_options=read_options, schema=schema)


def read_parquet(
    file: FileInput,
    schema: Schema,
    storage_config: StorageConfig | None = None,
    read_options: TableReadOptions = TableReadOptions(),
    parquet_options: TableParseParquetOptions = TableParseParquetOptions(),
) -> MicroPartition:
    """Reads a MicroPartition from a Parquet file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        read_options (TableReadOptions, optional): Options for reading the file

    Returns:
        MicroPartition: Parsed MicroPartition from Parquet
    """
    io_config = None
    if storage_config is not None:
        config = storage_config.config
        if isinstance(config, NativeStorageConfig):
            assert isinstance(
                file, (str, pathlib.Path)
            ), "Native downloader only works on string or Path inputs to read_parquet"
            tbl = MicroPartition.read_parquet(
                str(file),
                columns=read_options.column_names,
                num_rows=read_options.num_rows,
                io_config=config.io_config,
                coerce_int96_timestamp_unit=parquet_options.coerce_int96_timestamp_unit,
                multithreaded_io=config.multithreaded_io,
            )
            return _cast_table_to_schema(tbl, read_options=read_options, schema=schema)

        assert isinstance(config, PythonStorageConfig)
        io_config = config.io_config

    f: IO
    if not isinstance(file, (str, pathlib.Path)):
        f = file
    else:
        paths, fs = _resolve_paths_and_filesystem(file, io_config=io_config)
        assert len(paths) == 1
        path = paths[0]
        f = fs.open_input_file(path)

    # If no rows required, we manually construct an empty table with the right schema
    if read_options.num_rows == 0:
        pqf = papq.ParquetFile(f, coerce_int96_timestamp_unit=str(parquet_options.coerce_int96_timestamp_unit))
        arrow_schema = pqf.metadata.schema.to_arrow_schema()
        table = pa.Table.from_arrays([pa.array([], type=field.type) for field in arrow_schema], schema=arrow_schema)
    elif read_options.num_rows is not None:
        pqf = papq.ParquetFile(f, coerce_int96_timestamp_unit=str(parquet_options.coerce_int96_timestamp_unit))
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
            coerce_int96_timestamp_unit=str(parquet_options.coerce_int96_timestamp_unit),
        )

    return _cast_table_to_schema(MicroPartition.from_arrow(table), read_options=read_options, schema=schema)


class PACSVStreamHelper:
    def __init__(self, stream: pa.CSVStreamReader) -> None:
        self.stream = stream

    def __next__(self) -> pa.RecordBatch:
        return self.stream.read_next_batch()

    def __iter__(self) -> PACSVStreamHelper:
        return self


def skip_comment(comment: str | None) -> Callable | None:
    if comment is None:
        return None
    else:
        return lambda row: "skip" if row.text.startswith(comment) else "error"


def read_csv(
    file: FileInput,
    schema: Schema,
    storage_config: StorageConfig | None = None,
    csv_options: TableParseCSVOptions = TableParseCSVOptions(),
    read_options: TableReadOptions = TableReadOptions(),
) -> MicroPartition:
    """Reads a MicroPartition from a CSV file

    Args:
        file (str | IO): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        schema (Schema): Daft schema to read the CSV file into
        fs (fsspec.AbstractFileSystem): fsspec FileSystem to use for reading data.
            By default, Daft will automatically construct a FileSystem instance internally.
        csv_options (TableParseCSVOptions, optional): CSV-specific configs to apply when reading the file
        read_options (TableReadOptions, optional): Options for reading the file

    Returns:
        MicroPartition: Parsed MicroPartition from CSV
    """
    io_config = None
    if storage_config is not None:
        config = storage_config.config
        if isinstance(config, NativeStorageConfig):
            assert isinstance(
                file, (str, pathlib.Path)
            ), "Native downloader only works on string or Path inputs to read_csv"
            has_header = csv_options.header_index is not None
            csv_convert_options = CsvConvertOptions(
                limit=read_options.num_rows,
                include_columns=read_options.column_names,
                column_names=schema.column_names() if not has_header else None,
                schema=schema._schema if schema is not None else None,
            )
            csv_parse_options = CsvParseOptions(
                has_header=has_header,
                delimiter=csv_options.delimiter,
                double_quote=csv_options.double_quote,
                quote=csv_options.quote,
                escape_char=csv_options.escape_char,
                comment=csv_options.comment,
            )
            csv_read_options = CsvReadOptions(buffer_size=csv_options.buffer_size, chunk_size=csv_options.chunk_size)
            tbl = MicroPartition.read_csv(
                str(file),
                convert_options=csv_convert_options,
                parse_options=csv_parse_options,
                read_options=csv_read_options,
                io_config=config.io_config,
            )
            return _cast_table_to_schema(tbl, read_options=read_options, schema=schema)
        else:
            assert isinstance(config, PythonStorageConfig)
            io_config = config.io_config

    with _open_stream(file, io_config) as f:
        from daft.utils import ARROW_VERSION

        if csv_options.comment is not None and ARROW_VERSION < (7, 0, 0):
            raise ValueError(
                "pyarrow < 7.0.0 doesn't support handling comments in CSVs, please upgrade pyarrow to 7.0.0+."
            )

        parse_options = pacsv.ParseOptions(
            delimiter=csv_options.delimiter,
            quote_char=csv_options.quote,
            escape_char=csv_options.escape_char,
        )

        if ARROW_VERSION >= (7, 0, 0):
            parse_options.invalid_row_handler = skip_comment(csv_options.comment)

        pacsv_stream = pacsv.open_csv(
            f,
            parse_options=parse_options,
            read_options=pacsv.ReadOptions(
                # If no header, we use the schema's column names. Otherwise we use the headers in the CSV file.
                column_names=schema.column_names()
                if csv_options.header_index is None
                else None,
            ),
            convert_options=pacsv.ConvertOptions(
                # Column pruning
                include_columns=read_options.column_names,
                # If any columns are missing, parse as null array
                include_missing_columns=True,
            ),
        )

        if read_options.num_rows is not None:
            rows_left = read_options.num_rows
            pa_batches = []
            pa_schema = None
            for record_batch in PACSVStreamHelper(pacsv_stream):
                if pa_schema is None:
                    pa_schema = record_batch.schema
                if record_batch.num_rows > rows_left:
                    record_batch = record_batch.slice(0, rows_left)
                pa_batches.append(record_batch)
                rows_left -= record_batch.num_rows

                # Break needs to be here; always need to process at least one record batch
                if rows_left <= 0:
                    break

            # If source schema isn't determined, then the file was truly empty; set an empty source schema
            if pa_schema is None:
                pa_schema = pa.schema([])

            daft_table = MicroPartition.from_arrow_record_batches(pa_batches, pa_schema)
            assert len(daft_table) <= read_options.num_rows

        else:
            pa_table = pacsv_stream.read_all()
            daft_table = MicroPartition.from_arrow(pa_table)

    return _cast_table_to_schema(daft_table, read_options=read_options, schema=schema)


def write_tabular(
    table: MicroPartition,
    file_format: FileFormat,
    path: str | pathlib.Path,
    schema: Schema,
    partition_cols: ExpressionsProjection | None = None,
    compression: str | None = None,
    io_config: IOConfig | None = None,
    partition_null_fallback: str = "__HIVE_DEFAULT_PARTITION__",
) -> MicroPartition:

    from daft.utils import ARROW_VERSION

    [resolved_path], fs = _resolve_paths_and_filesystem(path, io_config=io_config)
    if isinstance(path, pathlib.Path):
        path_str = str(path)
    else:
        path_str = path

    protocol = get_protocol_from_path(path_str)
    canonicalized_protocol = canonicalize_protocol(protocol)

    is_local_fs = canonicalized_protocol == "file"

    tables_to_write: list[MicroPartition]
    part_keys_postfix_per_table: list[str | None]
    partition_values = None
    if partition_cols and len(partition_cols) > 0:

        default_part = Series.from_pylist([partition_null_fallback])
        split_tables, partition_values = table.partition_by_value(partition_keys=partition_cols)
        assert len(split_tables) == len(partition_values)
        pkey_names = partition_values.column_names()

        values_string_values = []

        for c in pkey_names:
            column = partition_values.get_column(c)
            string_names = column._to_str_values()
            null_filled = column.is_null().if_else(default_part, string_names)
            values_string_values.append(null_filled.to_pylist())

        part_keys_postfix_per_table = []
        for i in range(len(partition_values)):
            postfix = "/".join(f"{pkey}={values[i]}" for pkey, values in zip(pkey_names, values_string_values))
            part_keys_postfix_per_table.append(postfix)
        tables_to_write = split_tables
    else:
        tables_to_write = [table]
        part_keys_postfix_per_table = [None]

    visited_paths = []
    partition_idx = []

    execution_config = get_context().daft_execution_config

    TARGET_ROW_GROUP_SIZE = execution_config.parquet_target_row_group_size

    if file_format == FileFormat.Parquet:
        format = pads.ParquetFileFormat()
        inflation_factor = execution_config.parquet_inflation_factor
        target_file_size = execution_config.parquet_target_filesize
        opts = format.make_write_options(compression=compression)
    elif file_format == FileFormat.Csv:
        format = pads.CsvFileFormat()
        opts = None
        assert compression is None
        inflation_factor = execution_config.csv_inflation_factor
        target_file_size = execution_config.csv_target_filesize
    else:
        raise ValueError(f"Unsupported file format {file_format}")

    for i, (tab, pf) in enumerate(zip(tables_to_write, part_keys_postfix_per_table)):
        full_path = resolved_path
        if pf is not None and len(pf) > 0:
            full_path = f"{full_path}/{pf}"

        arrow_table = tab.to_arrow()

        size_bytes = arrow_table.nbytes

        target_num_files = max(math.ceil(size_bytes / target_file_size / inflation_factor), 1)
        num_rows = len(arrow_table)

        rows_per_file = max(math.ceil(num_rows / target_num_files), 1)

        target_row_groups = max(math.ceil(size_bytes / TARGET_ROW_GROUP_SIZE / inflation_factor), 1)
        rows_per_row_group = max(min(math.ceil(num_rows / target_row_groups), rows_per_file), 1)

        def file_visitor(written_file, i=i):
            visited_paths.append(written_file.path)
            partition_idx.append(i)

        kwargs = dict()

        if ARROW_VERSION >= (7, 0, 0):
            kwargs["max_rows_per_file"] = rows_per_file
            kwargs["min_rows_per_group"] = rows_per_row_group
            kwargs["max_rows_per_group"] = rows_per_row_group

        if ARROW_VERSION >= (8, 0, 0) and not is_local_fs:
            kwargs["create_dir"] = False

        pads.write_dataset(
            arrow_table,
            base_dir=full_path,
            basename_template=str(uuid4()) + "-{i}." + format.default_extname,
            format=format,
            partitioning=None,
            file_options=opts,
            file_visitor=file_visitor,
            use_threads=True,
            existing_data_behavior="overwrite_or_ignore",
            filesystem=fs,
            **kwargs,
        )

    data_dict: dict[str, Any] = {
        schema.column_names()[0]: Series.from_pylist(visited_paths, name=schema.column_names()[0]).cast(
            DataType.string()
        )
    }

    if partition_values is not None:
        partition_idx_series = Series.from_pylist(partition_idx).cast(DataType.int64())
        for c_name in partition_values.column_names():
            data_dict[c_name] = partition_values.get_column(c_name).take(partition_idx_series)
    return MicroPartition.from_pydict(data_dict)
