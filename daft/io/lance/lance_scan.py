# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from daft.daft import PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

if TYPE_CHECKING:
    import lance
    import pyarrow


# TODO support fts and fast_search
def _lancedb_table_factory_function(
    ds: "lance.LanceDataset",
    fragment_ids: Optional[list[int]] = None,
    required_columns: Optional[list[str]] = None,
    filter: Optional["pyarrow.compute.Expression"] = None,
    limit: Optional[int] = None,
) -> Iterator[PyRecordBatch]:
    fragments = [ds.get_fragment(id) for id in (fragment_ids or [])]
    if not fragments:
        raise RuntimeError(f"Unable to find lance fragments {fragment_ids}")
    scanner = ds.scanner(fragments=fragments, columns=required_columns, filter=filter, limit=limit)
    return (RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch for rb in scanner.to_batches())


class LanceDBScanOperator(ScanOperator):
    def __init__(self, ds: "lance.LanceDataset"):
        self._ds = ds

    def name(self) -> str:
        return "LanceDBScanOperator"

    def display_name(self) -> str:
        return f"LanceDBScanOperator({self._ds.uri})"

    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._ds.schema)

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return False

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        required_columns: Optional[list[str]]
        if pushdowns.columns is None:
            required_columns = None
        else:
            filter_required_column_names = pushdowns.filter_required_column_names()
            required_columns = (
                pushdowns.columns
                if filter_required_column_names is None
                else pushdowns.columns + filter_required_column_names
            )

        # TODO: figure out how to translate Pushdowns into LanceDB filters
        filters = None
        fragments = self._ds.get_fragments()
        for fragment in fragments:
            # TODO: figure out how if we can get this metadata from LanceDB fragments cheaply
            size_bytes = None
            stats = None

            # NOTE: `fragment.count_rows()` should result in 1 IO call for the data file
            # (1 fragment = 1 data file) and 1 more IO call for the deletion file (if present).
            # This could potentially be expensive to perform serially if there are thousands of files.
            # Given that num_rows isn't leveraged for much at the moment, and without statistics
            # we will probably end up materializing the data anyways for any operations, we leave this
            # as None.
            num_rows = None

            yield ScanTask.python_factory_func_scan_task(
                module=_lancedb_table_factory_function.__module__,
                func_name=_lancedb_table_factory_function.__name__,
                func_args=(self._ds, [fragment.fragment_id], required_columns, filters, pushdowns.limit),
                schema=self.schema()._schema,
                num_rows=num_rows,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                stats=stats,
            )
