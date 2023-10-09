from __future__ import annotations

import pathlib

import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

from daft.daft import NativeStorageConfig, PythonStorageConfig, StorageConfig
from daft.datatype import DataType
from daft.filesystem import _resolve_paths_and_filesystem
from daft.logical.schema import Schema
from daft.runners.partitioning import TableParseCSVOptions
from daft.table import Table
from daft.table.table_io import FileInput, _open_stream


def from_csv(
    file: FileInput,
    storage_config: StorageConfig | None = None,
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

    if storage_config is not None:
        config = storage_config.config
        if isinstance(config, NativeStorageConfig):
            assert isinstance(file, (str, pathlib.Path)), "Native downloader only works on string inputs to read_csv"
            io_config = config.io_config
            return Schema.from_csv(
                str(file),
                has_header=csv_options.header_index is not None,
                delimiter=csv_options.delimiter,
                io_config=io_config,
            )

        assert isinstance(config, PythonStorageConfig)
        fs = config.fs
    else:
        fs = None
    with _open_stream(file, fs) as f:
        reader = pacsv.open_csv(
            f,
            parse_options=pacsv.ParseOptions(
                delimiter=csv_options.delimiter,
            ),
            read_options=pacsv.ReadOptions(
                autogenerate_column_names=pyarrow_autogenerate_column_names,
            ),
        )

    return Schema.from_pyarrow_schema(reader.schema)


def from_json(
    file: FileInput,
    storage_config: StorageConfig | None = None,
) -> Schema:
    """Reads a Schema from a JSON file

    Args:
        file (FileInput): either a file-like object or a string file path (potentially prefixed with a protocol such as "s3://")
        read_options (TableReadOptions, optional): Options for reading the file

    Returns:
        Schema: Inferred Schema from the JSON
    """
    if storage_config is not None:
        config = storage_config.config
        assert isinstance(config, PythonStorageConfig)
        fs = config.fs
    else:
        fs = None
    with _open_stream(file, fs) as f:
        table = pajson.read_json(f)

    return Table.from_arrow(table).schema()


def from_parquet(
    file: FileInput,
    storage_config: StorageConfig | None = None,
) -> Schema:
    """Infers a Schema from a Parquet file"""
    if storage_config is not None:
        config = storage_config.config
        if isinstance(config, NativeStorageConfig):
            assert isinstance(
                file, (str, pathlib.Path)
            ), "Native downloader only works on string inputs to read_parquet"
            io_config = config.io_config
            return Schema.from_parquet(str(file), io_config=io_config)

        assert isinstance(config, PythonStorageConfig)
        fs = config.fs
    else:
        fs = None

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
