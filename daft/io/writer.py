import uuid
from typing import Optional, Union

from daft.daft import IOConfig
from daft.dependencies import pa
from daft.filesystem import (
    _resolve_paths_and_filesystem,
    canonicalize_protocol,
    get_protocol_from_path,
)
from daft.table.micropartition import MicroPartition


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
            self.fs.create_dir(root_dir)

        self.file_name = f"{uuid.uuid4()}-{file_idx}.{file_format}"
        self.compression = compression if compression is not None else "none"
        self.current_writer: Optional[Union[pa.papq.ParquetWriter, pa.pacsv.CSVWriter]] = None

    def _create_writer(self, schema: pa.Schema):
        raise NotImplementedError("Subclasses must implement this method.")

    def write(self, table: MicroPartition):
        if self.current_writer is None:
            self.current_writer = self._create_writer(table.schema().to_pyarrow_schema())
        self.current_writer.write_table(table.to_arrow())

    def close(self) -> Optional[str]:
        if self.current_writer is None:
            return None
        self.current_writer.close()
        return f"{self.resolved_path}/{self.file_name}"


class ParquetFileWriter(FileWriterBase):
    def __init__(
        self,
        root_dir: str,
        file_idx: int,
        compression: str = "none",
        io_config: Optional[IOConfig] = None,
    ):
        super().__init__(root_dir, file_idx, "parquet", compression, io_config)

    def _create_writer(self, schema: pa.Schema) -> pa.papq.ParquetWriter:
        file_path = f"{self.resolved_path}/{self.file_name}"
        return pa.papq.ParquetWriter(
            file_path,
            schema,
            compression=self.compression,
            use_compliant_nested_type=False,
            filesystem=self.fs,
        )


class CSVFileWriter(FileWriterBase):
    def __init__(self, root_dir: str, file_idx: int, io_config: Optional[IOConfig] = None):
        super().__init__(root_dir, file_idx, "csv", None, io_config)

    def _create_writer(self, schema: pa.Schema) -> pa.pacsv.CSVWriter:
        file_path = f"{self.resolved_path}/{self.file_name}"
        return pa.pacsv.CSVWriter(
            file_path,
            schema,
        )
