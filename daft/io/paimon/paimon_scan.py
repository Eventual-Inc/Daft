from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import daft
from daft.daft import (
    FileFormatConfig,
    ParquetSourceConfig,
    PyExpr,
    PyPartitionField,
    PyPushdowns,
    ScanTask,
    StorageConfig,
)
from daft.dependencies import pa
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters


if TYPE_CHECKING:
    from collections.abc import Iterator

    from pypaimon.common.predicate import Predicate
    from pypaimon.read.split import Split
    from pypaimon.table.file_store_table import FileStoreTable


logger = logging.getLogger(__name__)

# File format constants from pypaimon.common.core_options.CoreOptions
PAIMON_FILE_FORMAT_PARQUET = "parquet"
PAIMON_FILE_FORMAT_ORC = "orc"
PAIMON_FILE_FORMAT_AVRO = "avro"


def _paimon_read_split(
    table: FileStoreTable,
    split: Split,
    schema: Schema,
) -> Iterator[Any]:
    """Fall-back reader for splits that cannot be handled by Daft's native Parquet reader.

    This includes:
    - PK-table splits that require LSM-tree merge (split.raw_convertible == False)
    - Non-parquet file formats (ORC, Avro) that Daft doesn't natively support

    For these cases, we delegate to pypaimon's native reader.
    """
    table_read = table.new_read_builder().new_read()
    reader = table_read.to_arrow_batch_reader([split])
    for batch in iter(reader.read_next_batch, None):
        yield RecordBatch.from_arrow_record_batches([batch], reader.schema)._recordbatch


class PaimonScanOperator(ScanOperator, SupportsPushdownFilters):
    """Scan operator for Apache Paimon tables.

    Uses pypaimon for catalog metadata and scan planning (file listing,
    partition pruning, statistics-based file skipping), then creates Daft
    ScanTasks that are executed by Daft's native Parquet reader.

    For primary-key tables whose splits cannot be read directly without an
    LSM-tree merge, a Python factory function task is created that delegates
    back to pypaimon's native reader.
    """

    def __init__(
        self,
        table: FileStoreTable,
        storage_config: StorageConfig,
        catalog_options: dict[str, str],
    ) -> None:
        super().__init__()
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

        partition_key_names = set(table.partition_keys)
        self._partition_keys: list[PyPartitionField] = [
            PyPartitionField(f._field) for f in self._schema if f.name in partition_key_names
        ]

        # Filter pushdown state
        self._pushed_filters: list[PyExpr] | None = None
        self._remaining_filters: list[PyExpr] | None = None
        self._paimon_predicate: Predicate | None = None

    def schema(self) -> Schema:
        return self._schema

    def name(self) -> str:
        return "PaimonScanOperator"

    def display_name(self) -> str:
        table_name = getattr(self._table, "table_path", None)
        return f"PaimonScanOperator({table_name})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partition_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partitioning keys = {self._partition_keys}",
            f"Storage config = {self._storage_config}",
        ]

    def can_absorb_filter(self) -> bool:
        """Paimon supports filter pushdown via SupportsPushdownFilters."""
        return isinstance(self, SupportsPushdownFilters)

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return True

    def as_pushdown_filter(self) -> SupportsPushdownFilters | None:
        """Returns this scan operator as a SupportsPushdownFilters for filter pushdown."""
        return self

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        """Push filters down to Paimon scan.

        Converts Daft expressions to Paimon predicates where possible.
        Returns (pushed_filters, remaining_filters) where:
        - pushed_filters: filters that were converted to Paimon predicates
        - remaining_filters: filters that need post-scan evaluation
        """
        from daft.io.paimon._predicate_visitor import convert_filters_to_paimon

        pushed_filters, remaining_filters, paimon_predicate = convert_filters_to_paimon(
            self._table, filters
        )

        self._pushed_filters = pushed_filters if pushed_filters else None
        self._remaining_filters = remaining_filters if remaining_filters else None
        self._paimon_predicate = paimon_predicate

        if pushed_filters:
            logger.debug(
                "Paimon filter pushdown: %d filters pushed, %d remaining",
                len(pushed_filters),
                len(remaining_filters),
            )

        return pushed_filters, remaining_filters

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        read_builder = self._table.new_read_builder()

        if pushdowns.columns is not None:
            read_builder = read_builder.with_projection(list(pushdowns.columns))

        if pushdowns.limit is not None:
            read_builder = read_builder.with_limit(pushdowns.limit)

        # Apply filter pushdown using pre-converted predicate from push_filters()
        if self._paimon_predicate is not None:
            read_builder = read_builder.with_filter(self._paimon_predicate)
            logger.debug(
                "Applied Paimon filter pushdown predicate: %s",
                self._paimon_predicate,
            )

        if self._partition_keys and pushdowns.partition_filters is None:
            logger.warning(
                "%s has partition keys %s but no partition filter was specified. "
                "This will result in a full table scan.",
                self.display_name(),
                list(self._table.partition_keys),
            )

        plan = read_builder.new_scan().plan()

        pv_cache: dict[tuple[Any, ...], daft.recordbatch.RecordBatch | None] = {}

        for split in plan.splits():
            # Native path: use Daft's Rust Parquet reader when:
            # 1. File format is Parquet
            # 2. Either not a PK table, or split is raw-convertible (no LSM merge needed)
            if self._use_native_parquet and (not self._table.is_primary_key_table or split.raw_convertible):
                if self._partition_keys:
                    pv_key = tuple(sorted(split.partition.to_dict().items()))
                    if pv_key not in pv_cache:
                        pv_cache[pv_key] = self._build_partition_values(split)
                    pv = pv_cache[pv_key]
                    pv_recordbatch = pv._recordbatch if pv is not None else None
                else:
                    pv_recordbatch = None
                for data_file in split.files:
                    file_uri = self._build_file_uri(data_file.file_path)
                    st = ScanTask.catalog_scan_task(
                        file=file_uri,
                        file_format=FileFormatConfig.from_parquet_config(ParquetSourceConfig()),
                        schema=self._schema._schema,
                        num_rows=data_file.row_count,
                        storage_config=self._storage_config,
                        size_bytes=data_file.file_size,
                        iceberg_delete_files=None,
                        pushdowns=pushdowns,
                        partition_values=pv_recordbatch,
                        stats=None,
                    )
                    if st is not None:
                        yield st
            else:
                # Fallback to pypaimon native reader for:
                # - Non-parquet formats (ORC, Avro)
                # - PK table splits requiring LSM merge (raw_convertible == False)
                reason = "non-parquet format" if not self._use_native_parquet else "LSM merge required"
                logger.debug(
                    "Split with %d files using pypaimon fallback (%s).",
                    len(split.files),
                    reason,
                )
                yield ScanTask.python_factory_func_scan_task(
                    module=_paimon_read_split.__module__,
                    func_name=_paimon_read_split.__name__,
                    func_args=(self._table, split, self._schema),
                    schema=self._schema._schema,
                    num_rows=split.row_count,
                    size_bytes=split.file_size,
                    pushdowns=pushdowns,
                    stats=None,
                    source_name=self.display_name(),
                )

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
