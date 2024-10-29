import uuid
from abc import ABC, abstractmethod
from typing import Dict, Optional, Union

from daft.daft import IOConfig
from daft.dependencies import pa, pacsv, pq
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.series import Series
from daft.table.micropartition import MicroPartition
from daft.table.table import Table


def partition_values_to_str_mapping(
    partition_values: Table,
) -> Dict[str, str]:
    null_part = Series.from_pylist(
        [None]
    )  # This is to ensure that the null values are replaced with the default_partition_fallback value
    pkey_names = partition_values.column_names()

    partition_strings = {}

    for c in pkey_names:
        column = partition_values.get_column(c)
        string_names = column._to_str_values()
        null_filled = column.is_null().if_else(null_part, string_names)
        partition_strings[c] = null_filled.to_pylist()[0]

    return partition_strings


def partition_string_mapping_to_postfix(
    partition_strings: Dict[str, str],
    default_partition_fallback: str,
) -> str:
    postfix = "/".join(
        f"{k}={v if v is not None else default_partition_fallback}" for k, v in partition_strings.items()
    )
    return postfix


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
            partition_strings = partition_values_to_str_mapping(self.partition_values)
            postfix = partition_string_mapping_to_postfix(partition_strings, default_partition_fallback)
            self.dir_path = f"{self.resolved_path}/{postfix}"
        else:
            self.dir_path = f"{self.resolved_path}"

        self.full_path = f"{self.dir_path}/{self.file_name}"
        if is_local_fs:
            self.fs.create_dir(self.dir_path, recursive=True)

        self.compression = compression if compression is not None else "none"
        self.current_writer: Optional[Union[pq.ParquetWriter, pacsv.CSVWriter]] = None

    @abstractmethod
    def _create_writer(self, schema: pa.Schema) -> Union[pq.ParquetWriter, pacsv.CSVWriter]:
        """Create a writer instance for the specific file format.

        Args:
            schema: PyArrow schema defining the structure of the data to be written.

        Returns:
            A writer instance specific to the file format (Parquet or CSV).
        """
        pass

    def write(self, table: MicroPartition) -> None:
        """Write data to the file using the appropriate writer.

        Args:
            table: MicroPartition containing the data to be written.
        """
        if self.current_writer is None:
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow())

    def close(self) -> Table:
        """Close the writer and return metadata about the written file.

        Returns:
            Table containing metadata about the written file, including path and partition values.
        """
        if self.current_writer is not None:
            self.current_writer.close()

        metadata = {"path": Series.from_pylist([self.full_path])}
        if self.partition_values is not None:
            for col_name in self.partition_values.column_names():
                metadata[col_name] = self.partition_values.get_column(col_name)
        return Table.from_pydict(metadata)


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

    def _create_writer(self, schema: pa.Schema) -> pacsv.CSVWriter:
        return pacsv.CSVWriter(
            self.full_path,
            schema,
        )
