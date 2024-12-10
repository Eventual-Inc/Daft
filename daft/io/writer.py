import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional

from daft.daft import IOConfig
from daft.delta_lake.delta_lake_write import make_deltalake_add_action, make_deltalake_fs, sanitize_table_for_deltalake
from daft.dependencies import pa, pacsv, pq
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.iceberg.iceberg_write import (
    coerce_pyarrow_table_to_schema,
    make_iceberg_data_file,
    make_iceberg_record,
)
from daft.series import Series
from daft.table.micropartition import MicroPartition
from daft.table.partitioning import (
    partition_strings_to_path,
    partition_values_to_str_mapping,
)
from daft.table.table import Table

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties


class FileWriterBase(ABC):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        file_format: str,
        partition_values: Optional[Table] = None,
        compression: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
        version: Optional[int] = None,
        default_partition_fallback: Optional[str] = None,
    ):
        resolved_path, self.fs = self.resolve_path_and_fs(root_dir, io_config=io_config)
        protocol = get_protocol_from_path(root_dir)
        canonicalized_protocol = canonicalize_protocol(protocol)
        is_local_fs = canonicalized_protocol == "file"

        self.file_name = (
            f"{uuid.uuid4()}-{file_idx}.{file_format}"
            if version is None
            else f"{version}-{uuid.uuid4()}-{file_idx}.{file_format}"
        )
        self.partition_values = partition_values
        if self.partition_values is not None:
            self.partition_strings = {
                key: values.to_pylist()[0]
                for key, values in partition_values_to_str_mapping(self.partition_values).items()
            }
            self.dir_path = partition_strings_to_path(
                resolved_path,
                self.partition_strings,
                (
                    default_partition_fallback
                    if default_partition_fallback is not None
                    else "__HIVE_DEFAULT_PARTITION__"
                ),
            )
        else:
            self.partition_strings = {}
            self.dir_path = f"{resolved_path}"

        self.full_path = f"{self.dir_path}/{self.file_name}"
        if is_local_fs:
            self.fs.create_dir(self.dir_path, recursive=True)

        self.compression = compression if compression is not None else "none"
        self.position = 0

    def resolve_path_and_fs(self, root_dir: str, io_config: Optional[IOConfig] = None):
        [resolved_path], fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)
        return resolved_path, fs

    @abstractmethod
    def write(self, table: MicroPartition) -> int:
        """Write data to the file using the appropriate writer.

        Args:
            table: MicroPartition containing the data to be written.

        Returns:
            int: The number of bytes written to the file.
        """
        pass

    @abstractmethod
    def close(self) -> Table:
        """Close the writer and return metadata about the written file. Write should not be called after close.

        Returns:
            Table containing metadata about the written file, including path and partition values.
        """
        pass


class ParquetFileWriter(FileWriterBase):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        partition_values: Optional[Table] = None,
        compression: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
        version: Optional[int] = None,
        default_partition_fallback: Optional[str] = None,
        metadata_collector: Optional[List[pq.FileMetaData]] = None,
    ):
        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            file_format="parquet",
            partition_values=partition_values,
            compression=compression,
            io_config=io_config,
            version=version,
            default_partition_fallback=default_partition_fallback,
        )
        self.is_closed = False
        self.current_writer: Optional[pq.ParquetWriter] = None
        self.metadata_collector: Optional[List[pq.FileMetaData]] = metadata_collector

    def _create_writer(self, schema: pa.Schema) -> pq.ParquetWriter:
        opts = {}
        if self.metadata_collector is not None:
            opts["metadata_collector"] = self.metadata_collector
        return pq.ParquetWriter(
            self.full_path,
            schema,
            compression=self.compression,
            use_compliant_nested_type=False,
            filesystem=self.fs,
            **opts,
        )

    def write(self, table: MicroPartition) -> int:
        assert not self.is_closed, "Cannot write to a closed ParquetFileWriter"
        if self.current_writer is None:
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow(), row_group_size=len(table))

        current_position = self.current_writer.file_handle.tell()
        bytes_written = current_position - self.position
        self.position = current_position
        return bytes_written

    def close(self) -> Table:
        if self.current_writer is not None:
            self.current_writer.close()

        self.is_closed = True
        metadata = {"path": Series.from_pylist([self.full_path])}
        if self.partition_values is not None:
            for col_name in self.partition_values.column_names():
                metadata[col_name] = self.partition_values.get_column(col_name)
        return Table.from_pydict(metadata)


class CSVFileWriter(FileWriterBase):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        partition_values: Optional[Table] = None,
        io_config: Optional[IOConfig] = None,
    ):
        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            file_format="csv",
            partition_values=partition_values,
            io_config=io_config,
        )
        self.file_handle = None
        self.current_writer: Optional[pacsv.CSVWriter] = None
        self.is_closed = False

    def _create_writer(self, schema: pa.Schema) -> pacsv.CSVWriter:
        self.file_handle = self.fs.open_output_stream(self.full_path)
        return pacsv.CSVWriter(
            self.file_handle,
            schema,
        )

    def write(self, table: MicroPartition) -> int:
        assert not self.is_closed, "Cannot write to a closed CSVFileWriter"
        if self.current_writer is None:
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow())

        assert self.file_handle is not None  # We should have created the file handle in _create_writer
        current_position = self.file_handle.tell()
        bytes_written = current_position - self.position
        self.position = current_position
        return bytes_written

    def close(self) -> Table:
        if self.current_writer is not None:
            self.current_writer.close()

        self.is_closed = True
        metadata = {"path": Series.from_pylist([self.full_path])}
        if self.partition_values is not None:
            for col_name in self.partition_values.column_names():
                metadata[col_name] = self.partition_values.get_column(col_name)
        return Table.from_pydict(metadata)


class IcebergWriter(ParquetFileWriter):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        schema: "IcebergSchema",
        properties: "IcebergTableProperties",
        partition_spec_id: int,
        partition_values: Optional[Table] = None,
        io_config: Optional[IOConfig] = None,
    ):
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            partition_values=partition_values,
            compression="zstd",
            io_config=io_config,
            version=None,
            default_partition_fallback="null",
            metadata_collector=[],
        )

        self.part_record = make_iceberg_record(
            partition_values.to_pylist()[0] if partition_values is not None else None
        )
        self.iceberg_schema = schema
        self.file_schema = schema_to_pyarrow(schema)
        self.partition_spec_id = partition_spec_id
        self.properties = properties

    def write(self, table: MicroPartition) -> int:
        assert not self.is_closed, "Cannot write to a closed IcebergFileWriter"
        if self.current_writer is None:
            self.current_writer = self._create_writer(self.file_schema)
        casted = coerce_pyarrow_table_to_schema(table.to_arrow(), self.file_schema)
        self.current_writer.write_table(casted)

        current_position = self.current_writer.file_handle.tell()
        bytes_written = current_position - self.position
        self.position = current_position
        return bytes_written

    def close(self) -> Table:
        if self.current_writer is not None:
            self.current_writer.close()
        self.is_closed = True

        assert self.metadata_collector is not None
        metadata = self.metadata_collector[0]
        size = self.fs.get_file_info(self.full_path).size
        data_file = make_iceberg_data_file(
            self.full_path,
            size,
            metadata,
            self.part_record,
            self.partition_spec_id,
            self.iceberg_schema,
            self.properties,
        )
        return Table.from_pydict({"data_file": [data_file]})


class DeltalakeWriter(ParquetFileWriter):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        version: int,
        large_dtypes: bool,
        partition_values: Optional[Table] = None,
        io_config: Optional[IOConfig] = None,
    ):
        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            partition_values=partition_values,
            compression=None,
            io_config=io_config,
            version=version,
            default_partition_fallback=None,
            metadata_collector=[],
        )

        self.large_dtypes = large_dtypes

    def resolve_path_and_fs(self, root_dir: str, io_config: Optional[IOConfig] = None):
        return "", make_deltalake_fs(root_dir, io_config)

    def write(self, table: MicroPartition) -> int:
        assert not self.is_closed, "Cannot write to a closed DeltalakeFileWriter"

        converted_arrow_table = sanitize_table_for_deltalake(
            table,
            self.large_dtypes,
            self.partition_values.column_names() if self.partition_values is not None else None,
        )
        if self.current_writer is None:
            self.current_writer = self._create_writer(converted_arrow_table.schema)
        self.current_writer.write_table(converted_arrow_table)

        current_position = self.current_writer.file_handle.tell()
        bytes_written = current_position - self.position
        self.position = current_position
        return bytes_written

    def close(self) -> Table:
        if self.current_writer is not None:
            self.current_writer.close()
        self.is_closed = True

        assert self.metadata_collector is not None
        metadata = self.metadata_collector[0]
        size = self.fs.get_file_info(self.full_path).size
        add_action = make_deltalake_add_action(
            path=self.full_path,
            metadata=metadata,
            size=size,
            partition_values=self.partition_strings,
        )

        return Table.from_pydict({"add_action": [add_action]})
