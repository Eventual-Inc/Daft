from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.dependencies import pa
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.table.file_store_table import FileStoreTable


class PaimonDataSink(DataSink[list[Any]]):
    """DataSink for writing data to an Apache Paimon table.

    Delegates all file I/O and commit logic to pypaimon's BatchTableWrite /
    BatchTableCommit APIs. Each `write()` call (one per parallel worker) opens
    an independent BatchTableWrite, accumulates micro-partitions, then calls
    `prepare_commit()` to obtain CommitMessage objects. `finalize()` gathers
    all CommitMessages from all workers and performs a single atomic commit.
    """

    def __init__(self, table: FileStoreTable, mode: str = "append") -> None:
        if mode not in ("append", "overwrite"):
            raise ValueError(f"Only 'append' or 'overwrite' mode is supported for write_paimon, got: {mode!r}")
        self._table = table
        self._mode = mode

        # Apply pypaimon patch for complex type stats
        from daft.io.paimon.paimon_write import _patch_pypaimon_stats_for_complex_types

        _patch_pypaimon_stats_for_complex_types()

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

    def _align_batch_to_target_schema(self, batch: pa.RecordBatch, *, validate_schema: bool = True) -> pa.RecordBatch:
        target_names = self._target_schema.names
        input_names = batch.schema.names

        if validate_schema:
            if len(set(input_names)) != len(input_names):
                raise ValueError(f"Cannot write to Paimon with duplicate input field names: {input_names}")

            if len(set(target_names)) != len(target_names):
                raise ValueError(f"Cannot write to Paimon with duplicate target field names: {target_names}")

            missing = [name for name in target_names if name not in input_names]
            extra = [name for name in input_names if name not in target_names]
            if missing or extra:
                details = []
                if missing:
                    details.append(f"missing fields: {missing}")
                if extra:
                    details.append(f"extra fields: {extra}")
                raise ValueError(f"Paimon write schema mismatch: {'; '.join(details)}")

        if input_names != target_names:
            batch = batch.select(target_names)

        if batch.schema != self._target_schema:
            batch = batch.cast(self._target_schema)

        return batch

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[list[Any]]]:
        table_write = self._write_builder.new_write()

        total_rows = 0
        total_bytes = 0
        validated_schema: pa.Schema | None = None
        try:
            for mp in micropartitions:
                for rb in mp.get_record_batches():
                    batch = rb.to_arrow_record_batch()
                    validate_schema = batch.schema != validated_schema
                    input_schema = batch.schema
                    batch = self._align_batch_to_target_schema(batch, validate_schema=validate_schema)
                    if validate_schema:
                        validated_schema = input_schema
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

    def finalize(self, write_results: list[WriteResult[list[Any]]]) -> MicroPartition:
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
