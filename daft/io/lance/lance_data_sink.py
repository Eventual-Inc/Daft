from __future__ import annotations

import logging
import pathlib
from itertools import chain
from typing import TYPE_CHECKING, Any, Literal

import lance

from daft.context import get_context
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from daft.daft import IOConfig


logger = logging.getLogger(__name__)


def pyarrow_schema_castable(src: pa.Schema, dst: pa.Schema) -> bool:
    if len(src) != len(dst):
        return False
    for src_field, dst_field in zip(src, dst):
        empty_array = pa.array([], type=src_field.type)
        try:
            empty_array.cast(dst_field.type)
        except Exception:
            return False
    return True


class LanceDataSink(DataSink[list[lance.FragmentMetadata]]):
    """WriteSink for writing data to a Lance dataset."""

    def _import_lance(self) -> ModuleType:
        try:
            import lance

            return lance
        except ImportError:
            raise ImportError("lance is not installed. Please install lance using `pip install daft[lance]`")

    def __init__(
        self,
        uri: str | pathlib.Path,
        schema: Schema,
        mode: Literal["create", "append", "overwrite"],
        io_config: IOConfig | None = None,
        batch_size: int = 1,
        max_batch_rows: int = 100000,
        **kwargs: Any,
    ) -> None:
        from daft.io.object_store_options import io_config_to_storage_options

        lance = self._import_lance()
        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")
        self._table_uri = str(uri)
        self._mode = mode
        self._io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        self._kwargs = kwargs

        self._storage_options = io_config_to_storage_options(self._io_config, self._table_uri)

        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if max_batch_rows < 1:
            raise ValueError(f"max_batch_rows must be >= 1, got {max_batch_rows}")

        self._batch_size = batch_size
        self._max_batch_rows = max_batch_rows

        self._batch_tables: list[pa.Table] = []
        self._batch_row_count: int = 0
        self._batch_count: int = 0

        self._pyarrow_schema = schema.to_pyarrow_schema()

        try:
            table = lance.dataset(self._table_uri, storage_options=self._storage_options)
        except ValueError:
            table = None

        self._version: int = 0
        self._table_schema: pa.Schema | None = None
        if table is not None:
            self._table_schema = table.schema
            self._version = table.latest_version
            if not pyarrow_schema_castable(self._pyarrow_schema, self._table_schema) and not (
                self._mode == "overwrite"
            ):
                raise ValueError(
                    "Schema of data does not match table schema\n"
                    f"Data schema:\n{self._pyarrow_schema}\nTable Schema:\n{self._table_schema}"
                )

        self._schema = Schema._from_field_name_and_types(
            [
                ("num_fragments", DataType.int64()),
                ("num_deleted_rows", DataType.int64()),
                ("num_small_files", DataType.int64()),
                ("version", DataType.int64()),
            ]
        )

    def name(self) -> str:
        """Optional custom sink name."""
        return "Lance Write"

    def schema(self) -> Schema:
        return self._schema

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[list[lance.FragmentMetadata]]]:
        """Writes fragments from the given micropartitions with class-level batching support."""
        self._reset_batch_state()

        lance = self._import_lance()

        if self._batch_size == 1:
            for micropartition in micropartitions:
                arrow_table = pa.Table.from_batches(
                    micropartition.to_arrow().to_batches(),
                    self._pyarrow_schema,
                )
                if self._table_schema is not None:
                    arrow_table = arrow_table.cast(self._table_schema)

                bytes_written = arrow_table.nbytes
                rows_written = arrow_table.num_rows

                fragments = lance.fragment.write_fragments(
                    arrow_table,
                    dataset_uri=self._table_uri,
                    mode=self._mode,
                    storage_options=self._storage_options,
                    **self._kwargs,
                )
                yield WriteResult(
                    result=fragments,
                    bytes_written=bytes_written,
                    rows_written=rows_written,
                )
        else:
            try:
                for micropartition in micropartitions:
                    try:
                        arrow_table = pa.Table.from_batches(
                            micropartition.to_arrow().to_batches(),
                            self._pyarrow_schema,
                        )
                        if self._table_schema is not None:
                            arrow_table = arrow_table.cast(self._table_schema)

                        self._batch_tables.append(arrow_table)
                        self._batch_row_count += arrow_table.num_rows
                        self._batch_count += 1

                        if self._should_flush_batch(self._batch_count, self._batch_row_count):
                            yield from self._flush_batch(lance)

                    except Exception as e:
                        logger.error("Error processing micropartition: %s", e)
                        if self._batch_tables:
                            try:
                                yield from self._flush_batch(lance)
                            except Exception as flush_error:
                                logger.error("Batch flush failed: %s", flush_error)
                                for table in self._batch_tables:
                                    yield from self._write_single_table(table, lance)
                        self._reset_batch_state()

                        try:
                            arrow_table = pa.Table.from_batches(
                                micropartition.to_arrow().to_batches(),
                                self._pyarrow_schema,
                            )
                            if self._table_schema is not None:
                                arrow_table = arrow_table.cast(self._table_schema)
                            yield from self._write_single_table(arrow_table, lance)
                        except Exception as individual_error:
                            logger.error("Failed to process micropartition individually: %s", individual_error)
                            raise e
            finally:
                if self._batch_tables:
                    try:
                        yield from self._flush_batch(lance)
                    except Exception as final_flush_error:
                        logger.error("Final batch flush failed: %s", final_flush_error)
                        for table in self._batch_tables:
                            try:
                                yield from self._write_single_table(table, lance)
                            except Exception as table_error:
                                logger.error("Failed to write individual table in final cleanup: %s", table_error)
                        self._reset_batch_state()

    def _should_flush_batch(self, batch_count: int, batch_row_count: int) -> bool:
        """Determines if the current batch should be flushed."""
        should_flush = batch_count >= self._batch_size or batch_row_count >= self._max_batch_rows

        logger.debug(
            "Batch status: count=%d/%d, rows=%d/%d, should_flush=%s",
            batch_count,
            self._batch_size,
            batch_row_count,
            self._max_batch_rows,
            should_flush,
        )

        return should_flush

    def _flush_batch(self, lance: ModuleType) -> Iterator[WriteResult[list[lance.FragmentMetadata]]]:
        """Flushes the class-level batch of Arrow tables by concatenating and writing them."""
        if not self._batch_tables:
            return

        logger.debug(
            "Flushing class-level batch with %d tables, total rows: %d", len(self._batch_tables), self._batch_row_count
        )

        try:
            if len(self._batch_tables) == 1:
                yield from self._write_single_table(self._batch_tables[0], lance)
            else:
                try:
                    combined_table = pa.concat_tables(self._batch_tables)
                    yield from self._write_single_table(combined_table, lance)
                except Exception as concat_error:
                    logger.warning("Table concatenation failed: %s, falling back to individual writes", concat_error)
                    for i, table in enumerate(self._batch_tables):
                        try:
                            yield from self._write_single_table(table, lance)
                        except Exception as table_error:
                            logger.error("Failed to write individual table %d: %s", i, table_error)
                            raise
        except Exception as e:
            logger.error("Batch flush failed with %d tables: %s", len(self._batch_tables), e)
            raise
        finally:
            self._reset_batch_state()

    def _reset_batch_state(self) -> None:
        """Resets the class-level batch state."""
        self._batch_tables = []
        self._batch_row_count = 0
        self._batch_count = 0

    def flush_batch(self) -> Iterator[WriteResult[list[lance.FragmentMetadata]]]:
        """Public method to manually flush any accumulated batch data."""
        if self._batch_tables:
            lance = self._import_lance()
            yield from self._flush_batch(lance)

    def _write_single_table(
        self, arrow_table: pa.Table, lance: ModuleType
    ) -> Iterator[WriteResult[list[lance.FragmentMetadata]]]:
        """Writes a single Arrow table to Lance."""
        bytes_written = arrow_table.nbytes
        rows_written = arrow_table.num_rows

        fragments = lance.fragment.write_fragments(
            arrow_table,
            dataset_uri=self._table_uri,
            mode=self._mode,
            storage_options=self._storage_options,
            **self._kwargs,
        )
        yield WriteResult(
            result=fragments,
            bytes_written=bytes_written,
            rows_written=rows_written,
        )

    def finalize(self, write_results: list[WriteResult[list[lance.FragmentMetadata]]]) -> MicroPartition:
        """Commits the fragments to the Lance dataset. Returns a DataFrame with the stats of the dataset."""
        lance = self._import_lance()

        remaining_results = list(self.flush_batch())
        write_results.extend(remaining_results)

        fragments = list(chain.from_iterable(write_result.result for write_result in write_results))

        if self._mode == "create" or self._mode == "overwrite":
            operation = lance.LanceOperation.Overwrite(self._pyarrow_schema, fragments)
        elif self._mode == "append":
            operation = lance.LanceOperation.Append(fragments)

        dataset = lance.LanceDataset.commit(
            self._table_uri,
            operation,
            read_version=self._version,
            storage_options=self._storage_options,
        )
        stats = dataset.stats.dataset_stats()

        tbl = MicroPartition.from_pydict(
            {
                "num_fragments": pa.array([stats["num_fragments"]], type=pa.int64()),
                "num_deleted_rows": pa.array([stats["num_deleted_rows"]], type=pa.int64()),
                "num_small_files": pa.array([stats["num_small_files"]], type=pa.int64()),
                "version": pa.array([dataset.version], type=pa.int64()),
            }
        )
        return tbl
