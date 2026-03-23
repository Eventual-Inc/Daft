from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa  # noqa: TID253

from daft.datatype import DataType
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.table.file_store_table import FileStoreTable


class PaimonDataSink(DataSink[list]):
    """DataSink for writing data to an Apache Paimon table.

    Delegates all file I/O and commit logic to pypaimon's BatchTableWrite /
    BatchTableCommit APIs. Each `write()` call (one per parallel worker) opens
    an independent BatchTableWrite, accumulates micro-partitions, then calls
    `prepare_commit()` to obtain CommitMessage objects. `finalize()` gathers
    all CommitMessages from all workers and performs a single atomic commit.
    """

    def __init__(self, table: FileStoreTable, mode: str = "append") -> None:
        if mode not in ("append", "overwrite"):
            raise ValueError(
                f"Only 'append' or 'overwrite' mode is supported for write_paimon, got: {mode!r}"
            )
        self._table = table
        self._mode = mode

        from pypaimon.schema.data_types import PyarrowFieldParser

        self._target_schema: pa.Schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        self._write_builder = table.new_batch_write_builder()
        if mode == "overwrite":
            self._write_builder.overwrite({})

    def name(self) -> str:
        return "Paimon Write"

    def schema(self) -> Schema:
        return Schema._from_field_name_and_types(
            [
                ("operation", DataType.string()),
                ("rows", DataType.int64()),
                ("file_size", DataType.int64()),
                ("file_name", DataType.string()),
            ]
        )

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[list]]:
        table_write = self._write_builder.new_write()

        # Lazily compute which fields need type casting on the first batch.
        cast_fields: list[tuple[int, pa.DataType]] | None = None

        total_rows = 0
        total_bytes = 0
        try:
            for mp in micropartitions:
                for rb in mp.get_record_batches():
                    batch = rb.to_arrow_record_batch()
                    if cast_fields is None:
                        cast_fields = [
                            (i, field.type)
                            for i, field in enumerate(self._target_schema)
                            if batch.column(i).type != field.type
                        ]
                    if cast_fields:
                        arrays = list(batch.columns)
                        for i, target_type in cast_fields:
                            arrays[i] = arrays[i].cast(target_type)
                        batch = pa.RecordBatch.from_arrays(arrays, schema=self._target_schema)
                    table_write.write_arrow_batch(batch)
                    total_rows += batch.num_rows
                    total_bytes += batch.nbytes
            commit_messages = table_write.prepare_commit()
        finally:
            table_write.close()

        yield WriteResult(
            result=list(commit_messages),
            bytes_written=total_bytes,
            rows_written=total_rows,
        )

    def finalize(self, write_results: list[WriteResult[list]]) -> MicroPartition:
        all_commit_messages = [msg for wr in write_results for msg in wr.result]

        table_commit = self._write_builder.new_commit()
        try:
            table_commit.commit(all_commit_messages)
        finally:
            table_commit.close()

        operation_label = "OVERWRITE" if self._mode == "overwrite" else "ADD"
        all_files = [f for msg in all_commit_messages for f in msg.new_files]

        return MicroPartition.from_pydict(
            {
                "operation": pa.array([operation_label] * len(all_files), type=pa.string()),
                "rows": pa.array([f.row_count for f in all_files], type=pa.int64()),
                "file_size": pa.array([f.file_size for f in all_files], type=pa.int64()),
                "file_name": pa.array([f.file_name for f in all_files], type=pa.string()),
            }
        )
