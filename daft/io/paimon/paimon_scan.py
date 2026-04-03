from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import daft
from daft.dependencies import pa
from daft.expressions import ExpressionsProjection
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pypaimon.read.split import Split
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import StorageConfig
    from daft.io.partitioning import PartitionField
    from daft.io.pushdowns import Pushdowns

logger = logging.getLogger(__name__)

# File format constants from pypaimon.common.core_options.CoreOptions
PAIMON_FILE_FORMAT_PARQUET = "parquet"
PAIMON_FILE_FORMAT_ORC = "orc"
PAIMON_FILE_FORMAT_AVRO = "avro"


class _PaimonPKSplitTask(DataSourceTask):
    """DataSourceTask for PK-table splits that require LSM-tree merge.

    Used when split.raw_convertible is False (overlapping levels exist) or
    when the file format is not Parquet (ORC, Avro). Delegates to pypaimon's
    native reader which handles LSM merging internally.
    """

    def __init__(self, table: FileStoreTable, split: Split, schema: Schema) -> None:
        self._table = table
        self._split = split
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        table_read = self._table.new_read_builder().new_read()
        reader = table_read.to_arrow_batch_reader([self._split])
        for batch in iter(reader.read_next_batch, None):
            yield RecordBatch.from_arrow_record_batches([batch], reader.schema)


class PaimonDataSource(DataSource):
    """DataSource for Apache Paimon tables.

    Uses pypaimon for catalog metadata and scan planning (file listing,
    partition pruning, statistics-based file skipping), then yields
    DataSourceTask objects executed by Daft's native Parquet reader.

    For primary-key tables whose splits cannot be read directly without an
    LSM-tree merge, a _PaimonPKSplitTask is yielded which delegates back
    to pypaimon's native reader.
    """

    def __init__(
        self,
        table: FileStoreTable,
        storage_config: StorageConfig,
        catalog_options: dict[str, str],
    ) -> None:
        self._table = table
        self._storage_config = storage_config
        self._catalog_options = catalog_options

        from pypaimon.schema.data_types import PyarrowFieldParser

        pa_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        self._schema = Schema.from_pyarrow_schema(pa_schema)

        warehouse = catalog_options.get("warehouse", "")
        self._warehouse_scheme = urlparse(warehouse).scheme

        self._file_format = table.options.get("file.format", PAIMON_FILE_FORMAT_PARQUET).lower()
        self._use_native_parquet = self._file_format == PAIMON_FILE_FORMAT_PARQUET

        self._partition_field_arrow_types: dict[str, pa.DataType] = (
            {f.name: PyarrowFieldParser.from_paimon_type(f.type) for f in table.partition_keys_fields}
            if table.partition_keys
            else {}
        )

    @property
    def name(self) -> str:
        table_path = getattr(self._table, "table_path", None)
        return f"PaimonDataSource({table_path})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        from daft.io.partitioning import PartitionField

        partition_key_names = set(self._table.partition_keys)
        return [PartitionField.create(f) for f in self._schema if f.name in partition_key_names]

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        read_builder = self._table.new_read_builder()

        if pushdowns.columns is not None:
            columns = list(pushdowns.columns)
            partition_keys = self._table.partition_keys
            projected = list(dict.fromkeys(columns + partition_keys))
            read_builder = read_builder.with_projection(projected)

        if pushdowns.limit is not None:
            read_builder = read_builder.with_limit(pushdowns.limit)

        if self._table.partition_keys and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partition keys %s but no partition filter was specified. "
                "This will result in a full table scan.",
                self.name,
                list(self._table.partition_keys),
            )

        plan = read_builder.new_scan().plan()

        pv_cache: dict[tuple[Any, ...], RecordBatch | None] = {}

        for split in plan.splits():
            # Evaluate partition filter against this split's partition values.
            # partition_filters is the DataSource's responsibility: if this split's
            # partition doesn't match, skip it entirely (the Filter node was absorbed
            # into pushdowns.partition_filters by the optimizer).
            if self._table.partition_keys and pushdowns.partition_filters is not None:
                pv_key = tuple(sorted(split.partition.to_dict().items()))
                if pv_key not in pv_cache:
                    pv_cache[pv_key] = self._build_partition_values(split)
                pv = pv_cache[pv_key]
                if pv is not None and len(pv.filter(ExpressionsProjection([pushdowns.partition_filters]))) == 0:
                    continue

            if self._use_native_parquet and (not self._table.is_primary_key_table or split.raw_convertible):
                pv = None
                if self._table.partition_keys:
                    pv_key = tuple(sorted(split.partition.to_dict().items()))
                    if pv_key not in pv_cache:
                        pv_cache[pv_key] = self._build_partition_values(split)
                    pv = pv_cache[pv_key]

                for data_file in split.files:
                    file_uri = self._build_file_uri(data_file.file_path)
                    yield DataSourceTask.parquet(
                        path=file_uri,
                        schema=self._schema,
                        pushdowns=pushdowns,
                        num_rows=data_file.row_count,
                        size_bytes=data_file.file_size,
                        partition_values=pv,
                        storage_config=self._storage_config,
                    )
            else:
                reason = "non-parquet format" if not self._use_native_parquet else "LSM merge required"
                logger.debug(
                    "Split with %d files using pypaimon fallback (%s).",
                    len(split.files),
                    reason,
                )
                yield _PaimonPKSplitTask(self._table, split, self._schema)

    def _build_file_uri(self, file_path: str) -> str:
        """Reconstruct a full URI from a (potentially scheme-stripped) file_path."""
        if self._warehouse_scheme:
            return f"{self._warehouse_scheme}://{file_path}"
        return f"file://{file_path}"

    def _build_partition_values(self, split: Split) -> daft.recordbatch.RecordBatch | None:
        """Build a single-row RecordBatch encoding the partition values for a split."""
        if not self._table.partition_keys:
            return None

        partition_dict = split.partition.to_dict()
        arrays: dict[str, daft.Series] = {}
        for pfield in self._table.partition_keys_fields:
            value = partition_dict.get(pfield.name)
            arrow_type = self._partition_field_arrow_types[pfield.name]
            arrays[pfield.name] = daft.Series.from_arrow(pa.array([value], type=arrow_type), name=pfield.name)

        if not arrays:
            return None
        return daft.recordbatch.RecordBatch.from_pydict(arrays)
