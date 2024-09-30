import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from daft.context import get_context
from daft.daft import IOConfig, PyTable
from daft.dependencies import pa, pacsv, pq
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.iceberg.iceberg_write import (
    coerce_pyarrow_table_to_schema,
    to_partition_representation,
)
from daft.series import Series
from daft.table.micropartition import MicroPartition
from daft.table.table import Table

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
        [self.resolved_path], self.fs = _resolve_paths_and_filesystem(root_dir, io_config=io_config)
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
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow())

    def close(self) -> Optional[Any]:
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
    def __init__(self, root_dir: str, file_idx: int, io_config: Optional[IOConfig] = None):
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
        partition_values: Optional[PyTable] = None,
        compression: str = "zstd",
        io_config: Optional[IOConfig] = None,
    ):
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.typedef import Record as IcebergRecord

        super().__init__(root_dir, file_idx, "parquet", compression, io_config)
        if partition_values is None:
            self.part_record = IcebergRecord()
        else:
            part_vals = Table._from_pytable(partition_values).to_pylist()[0]
            iceberg_part_vals = {k: to_partition_representation(v) for k, v in part_vals.items()}
            self.part_record = IcebergRecord(**iceberg_part_vals)
        self.iceberg_schema = schema
        self.file_schema = schema_to_pyarrow(schema)
        self.metadata_collector: List[pq.FileMetaData] = []
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
        if self.current_writer is None:
            self.current_writer = self._create_writer(self.file_schema)
        casted = coerce_pyarrow_table_to_schema(table.to_arrow(), self.file_schema)
        self.current_writer.write_table(casted)

    def close(self) -> PyTable:
        import pyiceberg
        from packaging.version import parse
        from pyiceberg.io.pyarrow import (
            compute_statistics_plan,
            parquet_path_to_id_mapping,
        )
        from pyiceberg.manifest import DataFile, DataFileContent
        from pyiceberg.manifest import FileFormat as IcebergFileFormat

        file_path = super().close()
        metadata = self.metadata_collector[0]
        size = self.fs.get_file_info(file_path).size
        kwargs = {
            "content": DataFileContent.DATA,
            "file_path": file_path,
            "file_format": IcebergFileFormat.PARQUET,
            "partition": self.part_record,
            "file_size_in_bytes": size,
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

            statistics = data_file_statistics_from_parquet_metadata(
                parquet_metadata=metadata,
                stats_columns=compute_statistics_plan(self.iceberg_schema, self.properties),
                parquet_column_mapping=parquet_path_to_id_mapping(self.iceberg_schema),
            )
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
                stats_columns=compute_statistics_plan(self.iceberg_schema, self.properties),
                parquet_column_mapping=parquet_path_to_id_mapping(self.iceberg_schema),
            )
        return Table.from_pydict({"data_file": [data_file]})._table


class DeltalakeFileWriter(FileWriterBase):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        version: int,
        large_dtypes: bool,
        partition_values: Optional[PyTable] = None,
        postfix: str = "",
        io_config: Optional[IOConfig] = None,
    ):
        from deltalake.writer import DeltaStorageHandler
        from pyarrow.fs import PyFileSystem

        from daft.io.object_store_options import io_config_to_storage_options

        io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        storage_options = io_config_to_storage_options(io_config, root_dir)
        self.fs = PyFileSystem(DeltaStorageHandler(root_dir, storage_options))

        protocol = get_protocol_from_path(root_dir)
        canonicalized_protocol = canonicalize_protocol(protocol)
        is_local_fs = canonicalized_protocol == "file"
        if is_local_fs:
            self.fs.create_dir(root_dir, recursive=True)

        self.file_name = f"{version}-{uuid.uuid4()}-{file_idx}.parquet"
        self.full_path = f"{postfix}/{self.file_name}"
        self.postfix = postfix
        self.current_writer: Optional[pq.ParquetWriter] = None
        self.version = version
        self.large_dtypes = large_dtypes
        self.metadata_collector: List[pq.FileMetaData] = []
        self.partition_values = partition_values
        if self.partition_values is None:
            self.partition_value_mapping = {}
        else:
            self.partition_value_mapping = self.partition_values_str(Table._from_pytable(self.partition_values))

    @staticmethod
    def partition_values_str(partition_values: Table) -> Dict[str, str]:
        """
        Returns the partition values converted to human-readable strings, keeping null values as null.

        If the table is not partitioned, returns None.
        """
        null_part = Series.from_pylist([None])
        pkey_names = partition_values.column_names()

        partition_strings = {}

        for c in pkey_names:
            column = partition_values.get_column(c)
            string_names = column._to_str_values()
            null_filled = column.is_null().if_else(null_part, string_names)
            partition_strings[c] = null_filled.to_pylist()[0]

        return partition_strings

    def _create_writer(self, schema: pa.Schema) -> pq.ParquetWriter:
        return pq.ParquetWriter(
            self.full_path,
            schema,
            use_compliant_nested_type=False,
            filesystem=self.fs,
            metadata_collector=self.metadata_collector,
        )

    def write(self, table: MicroPartition):
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

    def close(self) -> PyTable:
        import json
        from datetime import datetime

        import deltalake
        from deltalake.writer import (
            AddAction,
            DeltaJSONEncoder,
            get_file_stats_from_metadata,
        )
        from packaging.version import parse

        # added to get_file_stats_from_metadata in deltalake v0.17.4: non-optional "num_indexed_cols" and "columns_to_collect_stats" arguments
        # https://github.com/delta-io/delta-rs/blob/353e08be0202c45334dcdceee65a8679f35de710/python/deltalake/writer.py#L725
        if parse(deltalake.__version__) < parse("0.17.4"):
            file_stats_args = {}
        else:
            file_stats_args = {"num_indexed_cols": -1, "columns_to_collect_stats": None}

        file_path = super().close()
        metadata = self.metadata_collector[0]
        stats = get_file_stats_from_metadata(metadata, **file_stats_args)
        size = self.fs.get_file_info(file_path).size
        print("mapping", self.partition_value_mapping)
        add_action = AddAction(
            file_path,
            size,
            self.partition_value_mapping,
            int(datetime.now().timestamp() * 1000),
            True,
            json.dumps(stats, cls=DeltaJSONEncoder),
        )

        return Table.from_pydict({"add_action": [add_action]})._table
