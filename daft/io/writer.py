import uuid
from typing import Optional, Union, TYPE_CHECKING

from daft.daft import IOConfig, PyMicroPartition
from daft.dependencies import pa, pacsv, pq
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.iceberg.iceberg_write import (
    add_missing_columns,
    coerce_pyarrow_table_to_schema,
    to_partition_representation,
)
from daft.table.micropartition import MicroPartition

if TYPE_CHECKING:
    from pyiceberg.partitioning import PartitionSpec as IcebergPartitionSpec
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties


class FileWriterBase:
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        file_format: str,
        compression: Optional[str] = None,
        io_config: Optional[IOConfig] = None,
    ):
        [self.resolved_path], self.fs = _resolve_paths_and_filesystem(
            root_dir, io_config=io_config
        )
        protocol = get_protocol_from_path(root_dir)
        canonicalized_protocol = canonicalize_protocol(protocol)
        is_local_fs = canonicalized_protocol == "file"
        if is_local_fs:
            self.fs.create_dir(self.resolved_path, recursive=True)

        self.file_name = f"{uuid.uuid4()}-{file_idx}.{file_format}"
        self.full_path = f"{self.resolved_path}/{self.file_name}"
        self.compression = compression if compression is not None else "none"
        self.current_writer: Optional[Union[pq.ParquetWriter, pacsv.CSVWriter]] = None

    def _create_writer(self, schema: pa.Schema):
        raise NotImplementedError("Subclasses must implement this method.")

    def write(self, table: MicroPartition):
        if self.current_writer is None:
            self.current_writer = self._create_writer(
                table.schema().to_pyarrow_schema()
            )
        self.current_writer.write_table(table.to_arrow())

    def close(self) -> Optional[str]:
        if self.current_writer is None:
            return None
        self.current_writer.close()
        return self.full_path


class ParquetFileWriter(FileWriterBase):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        compression: str = "none",
        io_config: Optional[IOConfig] = None,
    ):
        super().__init__(root_dir, file_idx, "parquet", compression, io_config)

    def _create_writer(self, schema: pa.Schema) -> pq.ParquetWriter:
        return pq.ParquetWriter(
            self.full_path,
            schema,
            compression=self.compression,
            use_compliant_nested_type=False,
            filesystem=self.fs,
        )


class CSVFileWriter(FileWriterBase):
    def __init__(
        self, root_dir: str, file_idx: int, io_config: Optional[IOConfig] = None
    ):
        super().__init__(root_dir, file_idx, "csv", None, io_config)

    def _create_writer(self, schema: pa.Schema) -> pacsv.CSVWriter:
        file_path = f"{self.resolved_path}/{self.file_name}"
        return pacsv.CSVWriter(
            file_path,
            schema,
        )


class IcebergFileWriter(FileWriterBase):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        schema: "IcebergSchema",
        properties: "IcebergTableProperties",
        partition_spec: "IcebergPartitionSpec",
        partition_values: Optional[MicroPartition] = None,
        compression: str = "zstd",
        io_config: Optional[IOConfig] = None,
    ):
        from pyiceberg.typedef import Record as IcebergRecord
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        print("IcebergFileWriter root_dir:", root_dir)
        super().__init__(root_dir, file_idx, "parquet", compression, io_config)
        if partition_values is None:
            self.part_record = IcebergRecord()
        else:
            part_vals = partition_values.to_pylist()
            iceberg_part_vals = {
                k: to_partition_representation(v) for k, v in part_vals.items()
            }
            self.part_record = IcebergRecord(**iceberg_part_vals)
        print("IcebergFileWriter iceberg schema:", schema)
        self.iceberg_schema = schema
        self.file_schema = schema_to_pyarrow(schema)
        print("IcebergFileWriter file schema:", self.file_schema)
        self.metadata_collector = []
        self.partition_spec = partition_spec
        self.properties = properties

    def _create_writer(self, schema: pa.Schema) -> pq.ParquetWriter:
        return pq.ParquetWriter(
            self.full_path,
            schema,
            compression=self.compression,
            use_compliant_nested_type=False,
            filesystem=self.fs,
            metadata_collector=self.metadata_collector,
        )

    def write(self, table: MicroPartition):
        print("IcebergFileWriter write table:", table)
        if self.current_writer is None:
            self.current_writer = self._create_writer(self.file_schema)
        table = add_missing_columns(table, self.file_schema)
        casted = coerce_pyarrow_table_to_schema(table.to_arrow(), self.file_schema)
        self.current_writer.write_table(casted)
        print("IcebergFileWriter write table done")

    def close(self) -> PyMicroPartition:
        import pyiceberg
        from packaging.version import parse
        from pyiceberg.io.pyarrow import (
            compute_statistics_plan,
            parquet_path_to_id_mapping,
        )
        from pyiceberg.manifest import DataFile, DataFileContent
        from pyiceberg.manifest import FileFormat as IcebergFileFormat

        file_path = super().close()
        print("IcebergFileWriter close file_path:", file_path)
        metadata = self.metadata_collector[0]
        print("IcebergFileWriter close metadata:", metadata)
        row_groups_size = sum(metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups))
        metadata_size = metadata.serialized_size

        total_file_size_bytes = row_groups_size + metadata_size
        kwargs = {
            "content": DataFileContent.DATA,
            "file_path": file_path,
            "file_format": IcebergFileFormat.PARQUET,
            "partition": self.part_record,
            "file_size_in_bytes": total_file_size_bytes,
            # After this has been fixed:
            # https://github.com/apache/iceberg-python/issues/271
            # "sort_order_id": task.sort_order_id,
            "sort_order_id": None,
            # Just copy these from the table for now
            "spec_id": self.partition_spec.spec_id,
            "equality_ids": None,
            "key_metadata": None,
        }

        if parse(pyiceberg.__version__) >= parse("0.7.0"):
            from pyiceberg.io.pyarrow import data_file_statistics_from_parquet_metadata
            print("IcebergFileWriter close before statistics")
            statistics = data_file_statistics_from_parquet_metadata(
                parquet_metadata=metadata,
                stats_columns=compute_statistics_plan(
                    self.iceberg_schema, self.properties
                ),
                parquet_column_mapping=parquet_path_to_id_mapping(self.iceberg_schema),
            )
            print("IcebergFileWriter close statistics:", statistics)
            data_file = DataFile(
                **{
                    **kwargs,
                    **statistics.to_serialized_dict(),
                }
            )
        else:
            from pyiceberg.io.pyarrow import fill_parquet_file_metadata

            data_file = DataFile(**kwargs)

            fill_parquet_file_metadata(
                data_file=data_file,
                parquet_metadata=metadata,
                stats_columns=compute_statistics_plan(
                    self.iceberg_schema, self.properties
                ),
                parquet_column_mapping=parquet_path_to_id_mapping(self.iceberg_schema),
            )
        print("IcebergFileWriter close data_file:", data_file)
        return MicroPartition.from_pydict({"data_file": [data_file]})._micropartition
