from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import daft
from daft.dependencies import pa
from daft.expressions import ExpressionsProjection
from daft.io.paimon._blob import FILE_PHYSICAL_TYPE, blob_column_to_file_array
from daft.io.paimon._predicate_visitor import convert_filters_to_paimon
from daft.io.partitioning import PartitionField
from daft.io.source import DataSource, DataSourceTask
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from pypaimon.common.predicate import Predicate
    from pypaimon.read.split import Split
    from pypaimon.table.file_store_table import FileStoreTable

    from daft.daft import PyExpr, StorageConfig
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

    def __init__(
        self,
        table: FileStoreTable,
        split: Split,
        schema: Schema,
        blob_column_names: set[str] | None = None,
    ) -> None:
        self._table = table
        self._split = split
        self._schema = schema
        self._blob_column_names = blob_column_names or set()

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        table_read = self._table.new_read_builder().new_read()
        reader = table_read.to_arrow_batch_reader([self._split])
        for batch in iter(reader.read_next_batch, None):
            if self._blob_column_names:
                batch = _convert_blob_columns(batch, self._blob_column_names)
            rb = RecordBatch.from_arrow_record_batches([batch], batch.schema)
            if self._blob_column_names:
                rb = _cast_blob_columns_to_file(rb, self._blob_column_names)
            yield rb


def _convert_blob_columns(batch: pa.RecordBatch, blob_column_names: set[str]) -> pa.RecordBatch:
    """Replace serialized BlobDescriptor columns with the File physical struct layout."""
    arrays = []
    fields = []
    for i, field in enumerate(batch.schema):
        col = batch.column(i)
        if field.name in blob_column_names and (pa.types.is_large_binary(field.type) or pa.types.is_binary(field.type)):
            arrays.append(blob_column_to_file_array(col))
            fields.append(pa.field(field.name, FILE_PHYSICAL_TYPE, nullable=field.nullable))
        else:
            arrays.append(col)
            fields.append(field)
    return pa.RecordBatch.from_arrays(arrays, schema=pa.schema(fields))


def _cast_blob_columns_to_file(rb: RecordBatch, blob_column_names: set[str]) -> RecordBatch:
    """Cast struct-typed blob columns in a RecordBatch to DataType.file()."""
    from daft.datatype import DataType

    file_dtype = DataType.file()
    columns = {}
    for i, field in enumerate(rb.schema()):
        col = rb.get_column(i)
        if field.name in blob_column_names:
            col = col.cast(file_dtype)
        columns[field.name] = col
    return RecordBatch.from_pydict(columns)


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

        self._blob_column_names: set[str] = {field.name for field in pa_schema if pa.types.is_large_binary(field.type)}
        self._has_blob_columns = bool(self._blob_column_names)

        if self._has_blob_columns:
            from daft.datatype import DataType

            base_schema = Schema.from_pyarrow_schema(pa_schema)
            fields = []
            for f in base_schema:
                if f.name in self._blob_column_names:
                    fields.append((f.name, DataType.file()))
                else:
                    fields.append((f.name, f.dtype))
            self._schema = Schema.from_field_name_and_types(fields)
        else:
            self._schema = Schema.from_pyarrow_schema(pa_schema)

        warehouse = catalog_options.get("warehouse", "")
        self._warehouse_scheme = urlparse(warehouse).scheme

        self._file_format = table.options.file_format().lower()
        self._use_native_parquet = self._file_format == PAIMON_FILE_FORMAT_PARQUET and not self._has_blob_columns

        self._partition_field_arrow_types: dict[str, pa.DataType] = (
            {f.name: PyarrowFieldParser.from_paimon_type(f.type) for f in table.partition_keys_fields}
            if table.partition_keys
            else {}
        )

        # Filter pushdown state (populated by push_filters, consumed by get_tasks)
        self._paimon_predicate: Predicate | None = None

    @property
    def name(self) -> str:
        table_path = getattr(self._table, "table_path", None)
        return f"PaimonDataSource({table_path})"

    @property
    def schema(self) -> Schema:
        return self._schema

    def get_partition_fields(self) -> list[PartitionField]:
        partition_key_names = set(self._table.partition_keys)
        return [PartitionField.create(f) for f in self._schema if f.name in partition_key_names]

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        """Push filters down to Paimon scan.

        Converts Daft expressions to Paimon predicates where possible.
        Returns (pushed_filters, remaining_filters) where:
        - pushed_filters: filters that were converted to Paimon predicates
        - remaining_filters: filters that need post-scan evaluation
        """
        pushed_filters, remaining_filters, paimon_predicate = convert_filters_to_paimon(self._table, filters)

        self._paimon_predicate = paimon_predicate

        if pushed_filters:
            logger.debug(
                "Paimon filter pushdown: %d filters pushed, %d remaining",
                len(pushed_filters),
                len(remaining_filters),
            )

        return pushed_filters, remaining_filters

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        read_table = self._table
        if self._has_blob_columns:
            read_table = self._table.copy({"blob-as-descriptor": "true"})

        read_builder = read_table.new_read_builder()

        if pushdowns.columns is not None:
            read_builder = read_builder.with_projection(list(pushdowns.columns))

        if pushdowns.limit is not None:
            read_builder = read_builder.with_limit(pushdowns.limit)

        if self._paimon_predicate is not None:
            read_builder = read_builder.with_filter(self._paimon_predicate)
            logger.debug(
                "Applied Paimon filter pushdown predicate: %s",
                self._paimon_predicate,
            )

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
                yield _PaimonPKSplitTask(read_table, split, self._schema, self._blob_column_names)

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
