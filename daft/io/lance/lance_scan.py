# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional, Union

from daft.daft import CountMode, PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.dependencies import pa
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters

if TYPE_CHECKING:
    import lance

logger = logging.getLogger(__name__)


# TODO support fts and fast_search
def _lancedb_table_factory_function(
    ds: "lance.LanceDataset",
    fragment_ids: Optional[list[int]] = None,
    required_columns: Optional[list[str]] = None,
    filter: Optional["pa.compute.Expression"] = None,
    limit: Optional[int] = None,
) -> Iterator[PyRecordBatch]:
    fragments = [ds.get_fragment(id) for id in (fragment_ids or [])]
    if not fragments:
        raise RuntimeError(f"Unable to find lance fragments {fragment_ids}")
    scanner = ds.scanner(fragments=fragments, columns=required_columns, filter=filter, limit=limit)
    return (RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch for rb in scanner.to_batches())


def _lancedb_count_result_function(
    ds: "lance.LanceDataset",
    required_column: str,
    filters: Optional[list[Any]] = None,
) -> Iterator[PyRecordBatch]:
    """Use LanceDB's API to count rows and return a record batch with the count result."""
    count = 0
    if filters is None:
        logger.debug("Using metadata for counting all rows (no filters)")
        count = ds.count_rows()
    else:
        # TODO: If filters are provided, we need to apply them after counting
        logger.debug("Counting rows with filters applied")
        scanner = ds.scanner(filter=filters)
        for batch in scanner.to_batches():
            count += batch.num_rows

    arrow_schema = pa.schema([pa.field(required_column, pa.uint64())])
    arrow_array = pa.array([count], type=pa.uint64())
    arrow_batch = pa.RecordBatch.from_arrays([arrow_array], [required_column])
    result_batch = RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema)._recordbatch
    return (result_batch for _ in [1])


class LanceDBScanOperator(ScanOperator, SupportsPushdownFilters):
    def __init__(self, ds: "lance.LanceDataset"):
        self._ds = ds
        self._pushed_filters: Union[list[PyExpr], None] = None

    def name(self) -> str:
        return "LanceDBScanOperator"

    def display_name(self) -> str:
        return f"LanceDBScanOperator({self._ds.uri})"

    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(self._ds.schema)

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return isinstance(self, SupportsPushdownFilters)

    def can_absorb_limit(self) -> bool:
        return False

    def can_absorb_select(self) -> bool:
        return False

    def supports_count_pushdown(self) -> bool:
        """Returns whether this scan operator supports count pushdown."""
        return True

    def supported_count_modes(self) -> list[CountMode]:
        """Returns the count modes supported by this scan operator."""
        return [CountMode.All]

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        from daft.expressions import Expression

        pushed = []
        remaining = []
        for expr in filters:
            try:
                filters = Expression._from_pyexpr(expr).to_arrow_expr()
                pushed.append(expr)
            except NotImplementedError:
                remaining.append(expr)

        if pushed:
            self._pushed_filters = pushed
        else:
            self._pushed_filters = None

        return pushed, remaining

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        required_columns: Optional[list[str]]
        if pushdowns.columns is None:
            required_columns = None
        else:
            filter_required_column_names = pushdowns.filter_required_column_names()
            required_columns = list(
                set(
                    pushdowns.columns
                    if filter_required_column_names is None
                    else pushdowns.columns + filter_required_column_names
                )
            )

        # Check if there is a count aggregation pushdown
        if (
            pushdowns.aggregation is not None
            and pushdowns.aggregation_count_mode() is not None
            and pushdowns.aggregation_required_column_names()
        ):
            count_mode = pushdowns.aggregation_count_mode()
            fields = pushdowns.aggregation_required_column_names()

            if count_mode not in self.supported_count_modes():
                logger.warning(
                    "Count mode %s is not supported for pushdown, falling back to original logic",
                    count_mode,
                )
                yield from self._create_regular_scan_tasks(pushdowns, required_columns)

            # TODO: If there are pushed filters, convert them to Arrow expressions
            filters = None

            new_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(fields[0], pa.uint64())]))
            yield ScanTask.python_factory_func_scan_task(
                module=_lancedb_count_result_function.__module__,
                func_name=_lancedb_count_result_function.__name__,
                func_args=(self._ds, fields[0], filters),
                schema=new_schema._schema,
                num_rows=1,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
            )
        else:
            # Regular scan without count pushdown
            yield from self._create_regular_scan_tasks(pushdowns, required_columns)

    def _create_regular_scan_tasks(
        self, pushdowns: PyPushdowns, required_columns: Optional[list[str]]
    ) -> Iterator[ScanTask]:
        """Create regular scan tasks without count pushdown."""
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
