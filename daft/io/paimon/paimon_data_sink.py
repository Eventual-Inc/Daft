from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from daft.datatype import DataType
from daft.dependencies import pa as daft_pa
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.table.file_store_table import FileStoreTable


def _cast_batch_to_paimon_schema(
    batch: pa.RecordBatch,
    target_schema: pa.Schema,
) -> pa.RecordBatch:
    arrays = []
    for field in target_schema:
        col = batch.column(field.name)
        if col.type != field.type:
            try:
                col = col.cast(field.type)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                pass
        arrays.append(col)
    return pa.RecordBatch.from_arrays(arrays, schema=target_schema)


class PaimonDataSink(DataSink[list]):
    """DataSink for writing data to an Apache Paimon table.

    Delegates all file I/O and commit logic to pypaimon's BatchTableWrite /
    BatchTableCommit APIs. Each `write()` call (one per parallel worker) opens
    an independent BatchTableWrite, accumulates micro-partitions, then calls
    `prepare_commit()` to obtain CommitMessage objects. `finalize()` gathers
    all CommitMessages from all workers and performs a single atomic commit.
    """

    def __init__(self, table: "FileStoreTable", mode: str = "append") -> None:
        if mode not in ("append", "overwrite"):
            raise ValueError(
                f"Only 'append' or 'overwrite' mode is supported for write_paimon, got: {mode!r}"
            )
        self._table = table
        self._mode = mode

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
        from pypaimon.schema.data_types import PyarrowFieldParser

        target_schema = PyarrowFieldParser.from_paimon_schema(self._table.fields)

        write_builder = self._table.new_batch_write_builder()
        if self._mode == "overwrite":
            write_builder.overwrite({})
        table_write = write_builder.new_write()

        total_rows = 0
        total_bytes = 0
        try:
            for mp in micropartitions:
                arrow_table = mp.to_arrow()
                for batch in arrow_table.to_batches():
                    casted = _cast_batch_to_paimon_schema(batch, target_schema)
                    table_write.write_arrow_batch(casted)
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

        commit_builder = self._table.new_batch_write_builder()
        if self._mode == "overwrite":
            commit_builder.overwrite({})
        table_commit = commit_builder.new_commit()
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
