import uuid
from abc import ABC, abstractmethod
from typing import Optional

from daft.daft import IOConfig
from daft.dependencies import pa, pacsv, pq
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.series import Series
from daft.table.micropartition import MicroPartition
from daft.table.partitioning import (
    partition_strings_to_path,
    partition_values_to_str_mapping,
)
from daft.table.table import Table


class FileWriterBase(ABC):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        file_format: str,
        partition_values: Optional[Table] = None,
        compression: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
        default_partition_fallback: str = "__HIVE_DEFAULT_PARTITION__",
    ):
        [self.resolved_path], self.fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)
        protocol = get_protocol_from_path(root_dir)
        canonicalized_protocol = canonicalize_protocol(protocol)
        is_local_fs = canonicalized_protocol == "file"

        self.file_name = f"{uuid.uuid4()}-{file_idx}.{file_format}"
        self.partition_values = partition_values
        if self.partition_values is not None:
            partition_strings = {
                key: values.to_pylist()[0]
                for key, values in partition_values_to_str_mapping(self.partition_values).items()
            }
            self.dir_path = partition_strings_to_path(self.resolved_path, partition_strings, default_partition_fallback)
        else:
            self.dir_path = f"{self.resolved_path}"

        self.full_path = f"{self.dir_path}/{self.file_name}"
        if is_local_fs:
            self.fs.create_dir(self.dir_path, recursive=True)

        self.compression = compression if compression is not None else "none"

    @abstractmethod
    def write(self, table: MicroPartition) -> None:
        """Write data to the file using the appropriate writer.

        Args:
            table: MicroPartition containing the data to be written.
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
        compression: str = "none",
        io_config: Optional[IOConfig] = None,
    ):
        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            file_format="parquet",
            partition_values=partition_values,
            compression=compression,
            io_config=io_config,
        )
        self.is_closed = False
        self.current_writer: Optional[pq.ParquetWriter] = None

    def _create_writer(self, schema: pa.Schema) -> pq.ParquetWriter:
        return pq.ParquetWriter(
            self.full_path,
            schema,
            compression=self.compression,
            use_compliant_nested_type=False,
            filesystem=self.fs,
        )

    def write(self, table: MicroPartition) -> None:
        assert not self.is_closed, "Cannot write to a closed ParquetFileWriter"
        if self.current_writer is None:
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow())

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
        self.current_writer: Optional[pacsv.CSVWriter] = None
        self.is_closed = False

    def _create_writer(self, schema: pa.Schema) -> pacsv.CSVWriter:
        output_file = self.fs.open_output_stream(self.full_path)
        return pacsv.CSVWriter(
            output_file,
            schema,
        )

    def write(self, table: MicroPartition) -> None:
        assert not self.is_closed, "Cannot write to a closed CSVFileWriter"
        if self.current_writer is None:
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow())

    def close(self) -> Table:
        if self.current_writer is not None:
            self.current_writer.close()

        self.is_closed = True
        metadata = {"path": Series.from_pylist([self.full_path])}
        if self.partition_values is not None:
            for col_name in self.partition_values.column_names():
                metadata[col_name] = self.partition_values.get_column(col_name)
        return Table.from_pydict(metadata)
