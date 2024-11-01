import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Optional, Tuple

from daft.context import get_context
from daft.daft import IOConfig
from daft.dependencies import pa, pacsv, pafs, pq
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.iceberg.iceberg_write import (
    coerce_pyarrow_table_to_schema,
    make_iceberg_data_file,
    to_partition_representation,
)
from daft.series import Series
from daft.table.micropartition import MicroPartition
from daft.table.partitioning import (
    partition_strings_to_path,
    partition_values_to_str_mapping,
)
from daft.table.table import Table
from daft.table.table_io import make_deltalake_add_action

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
        resolved_path_and_fs: Optional[Tuple[str, pafs.FileSystem]] = None,
        default_partition_fallback: Optional[str] = None,
    ):
        if resolved_path_and_fs is None:
            [resolved_path], self.fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)
        else:
            resolved_path, self.fs = resolved_path_and_fs

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
            self.dir_path = partition_strings_to_path(resolved_path, self.partition_strings, default_partition_fallback)
        else:
            self.partition_strings = {}
            self.dir_path = f"{resolved_path}"

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
        compression: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
        version: Optional[int] = None,
        resolved_path_and_fs: Optional[Tuple[str, pafs.FileSystem]] = None,
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
            resolved_path_and_fs=resolved_path_and_fs,
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
        return pacsv.CSVWriter(
            self.full_path,
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
        from pyiceberg.typedef import Record as IcebergRecord

        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            partition_values=partition_values,
            compression="zstd",
            io_config=io_config,
            version=None,
            resolved_path_and_fs=None,
            default_partition_fallback="null",
            metadata_collector=[],
        )

        if partition_values is None:
            self.part_record = IcebergRecord()
        else:
            part_vals = partition_values.to_pylist()[0]
            iceberg_part_vals = {k: to_partition_representation(v) for k, v in part_vals.items()}
            self.part_record = IcebergRecord(**iceberg_part_vals)

        self.iceberg_schema = schema
        self.file_schema = schema_to_pyarrow(schema)
        self.partition_spec_id = partition_spec_id
        self.properties = properties

    def write(self, table: MicroPartition):
        assert not self.is_closed, "Cannot write to a closed IcebergFileWriter"
        if self.current_writer is None:
            self.current_writer = self._create_writer(self.file_schema)
        casted = coerce_pyarrow_table_to_schema(table.to_arrow(), self.file_schema)
        self.current_writer.write_table(casted)

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
        from deltalake.writer import DeltaStorageHandler
        from pyarrow.fs import PyFileSystem

        from daft.io.object_store_options import io_config_to_storage_options

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        storage_options = io_config_to_storage_options(io_config, root_dir)
        fs = PyFileSystem(DeltaStorageHandler(root_dir, storage_options))

        super().__init__(
            root_dir=root_dir,
            file_idx=file_idx,
            partition_values=partition_values,
            compression=None,
            io_config=io_config,
            version=version,
            resolved_path_and_fs=("", fs),
            default_partition_fallback=None,
            metadata_collector=[],
        )

        self.large_dtypes = large_dtypes

    def write(self, table: MicroPartition):
        assert not self.is_closed, "Cannot write to a closed DeltalakeFileWriter"
        from deltalake.schema import _convert_pa_schema_to_delta

        from daft.io._deltalake import large_dtypes_kwargs

        arrow_table = table.to_arrow()
        if self.partition_values is not None:
            partition_keys = self.partition_values.column_names()
            arrow_table = arrow_table.drop_columns(partition_keys)

        converted_schema = _convert_pa_schema_to_delta(arrow_table.schema, **large_dtypes_kwargs(self.large_dtypes))
        converted_arrow_table = arrow_table.cast(converted_schema)
        if self.current_writer is None:
            self.current_writer = self._create_writer(converted_arrow_table.schema)
        self.current_writer.write_table(converted_arrow_table)

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
