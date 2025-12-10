# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    import lance

from daft.context import get_context
from daft.daft import CountMode, PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.dependencies import pa
from daft.expressions import Expression
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters
from .point_lookup import detect_point_lookup_columns

logger = logging.getLogger(__name__)


# TODO support fts and fast_search
def _lancedb_table_factory_function(
    ds_uri: str,
    open_kwargs: Optional[dict[Any, Any]] = None,
    fragment_ids: Optional[list[int]] = None,
    required_columns: Optional[list[str]] = None,
    filter: Optional["pa.compute.Expression"] = None,
    limit: Optional[int] = None,
) -> Iterator[PyRecordBatch]:
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    # Attempt to import lance and reconstruct with best-effort kwargs
    ds = lance.dataset(ds_uri, **(open_kwargs or {}))
    # If fragment_ids is None, let Lance choose fragments via index; omit the fragments parameter.
    if fragment_ids is None:
        scanner = ds.scanner(columns=required_columns, filter=filter, limit=limit)
    else:
        fragments = [ds.get_fragment(id) for id in fragment_ids]
        if not fragments:
            raise RuntimeError(f"Unable to find lance fragments {fragment_ids}")
        scanner = ds.scanner(fragments=fragments, columns=required_columns, filter=filter, limit=limit)
    return (RecordBatch.from_arrow_record_batches([rb], rb.schema)._recordbatch for rb in scanner.to_batches())


def _lancedb_count_result_function(
    ds_uri: str,
    open_kwargs: Optional[dict[Any, Any]],
    required_column: str,
    filter: Optional["pa.compute.Expression"] = None,
) -> Iterator[PyRecordBatch]:
    """Use LanceDB's API to count rows and return a record batch with the count result."""
    try:
        import lance
    except ImportError as e:
        raise ImportError(
            "Unable to import the `lance` package, please ensure that Daft is installed with the lance extra dependency: `pip install daft[lance]`"
        ) from e

    # Attempt to reconstruct with best-effort kwargs
    ds = lance.dataset(ds_uri, **(open_kwargs or {}))
    logger.debug("Using metadata for counting all rows")
    count = ds.count_rows(filter=filter)

    arrow_schema = pa.schema([pa.field(required_column, pa.uint64())])
    arrow_array = pa.array([count], type=pa.uint64())
    arrow_batch = pa.RecordBatch.from_arrays([arrow_array], [required_column])
    result_batch = RecordBatch.from_arrow_record_batches([arrow_batch], arrow_schema)._recordbatch
    return (result_batch for _ in [1])


class LanceDBScanOperator(ScanOperator, SupportsPushdownFilters):
    def __init__(self, ds: "lance.LanceDataset", fragment_group_size: Optional[int] = None):
        self._ds = ds
        self._pushed_filters: Union[list[PyExpr], None] = None
        self._remaining_filters: Union[list[PyExpr], None] = None
        self._fragment_group_size = fragment_group_size
        self._enable_strict_filter_pushdown = get_context().daft_planning_config.enable_strict_filter_pushdown
        self._schema = Schema.from_pyarrow_schema(self._ds.schema)

    def name(self) -> str:
        return "LanceDBScanOperator"

    def display_name(self) -> str:
        return f"LanceDBScanOperator({self._ds.uri})"

    def schema(self) -> Schema:
        return self._schema

    def partitioning_keys(self) -> list[PyPartitionField]:
        return []

    def can_absorb_filter(self) -> bool:
        return isinstance(self, SupportsPushdownFilters)

    def can_absorb_limit(self) -> bool:
        return True

    def can_absorb_select(self) -> bool:
        return True

    def supports_count_pushdown(self) -> bool:
        """Returns whether this scan operator supports count pushdown."""
        return True

    def supported_count_modes(self) -> list[CountMode]:
        """Returns the count modes supported by this scan operator."""
        return [CountMode.All]

    def as_pushdown_filter(self) -> Union[SupportsPushdownFilters, None]:
        return self

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self.schema()}",
        ]

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        pushed = []
        remaining = []

        for expr in filters:
            try:
                Expression._from_pyexpr(expr).to_arrow_expr()
                pushed.append(expr)
            except NotImplementedError:
                remaining.append(expr)

        if pushed:
            self._pushed_filters = pushed
        else:
            self._pushed_filters = None

        self._remaining_filters = remaining if remaining else None

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
                return

            filters = self._combine_filters_to_arrow()

            new_schema = Schema.from_pyarrow_schema(pa.schema([pa.field(fields[0], pa.uint64())]))
            open_kwargs = getattr(self._ds, "_lance_open_kwargs", None)
            yield ScanTask.python_factory_func_scan_task(
                module=_lancedb_count_result_function.__module__,
                func_name=_lancedb_count_result_function.__name__,
                func_args=(self._ds.uri, open_kwargs, fields[0], filters),
                schema=new_schema._schema,
                num_rows=1,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
                source_type=self.name(),
            )
        # Check if there is a limit pushdown and no filters
        elif pushdowns.limit is not None and self._pushed_filters is None and pushdowns.filters is None:
            yield from self._create_scan_tasks_with_limit_and_no_filters(pushdowns, required_columns)
        else:
            yield from self._create_regular_scan_tasks(pushdowns, required_columns)

    def _create_scan_tasks_with_limit_and_no_filters(
        self, pushdowns: PyPushdowns, required_columns: Optional[list[str]]
    ) -> Iterator[ScanTask]:
        """Create scan tasks optimized for limit pushdown with no filters."""
        assert self._pushed_filters is None, "Expected no filters when creating scan tasks with limit and no filters"
        assert pushdowns.limit is not None, "Expected a limit when creating scan tasks with limit and no filters"

        open_kwargs = getattr(self._ds, "_lance_open_kwargs", None)
        fragments = self._ds.get_fragments()
        remaining_limit = pushdowns.limit

        for fragment in fragments:
            if remaining_limit <= 0:
                # No more rows needed, stop creating scan tasks
                break

            # Calculate effective rows using fragment.count_rows()
            # This is not expensive because count_rows simply checks physical_rows - num_deletions when there are no filters
            # https://github.com/lancedb/lance/blob/v0.34.0/rust/lance/src/dataset/fragment.rs#L1049-L1055
            effective_rows = fragment.count_rows()

            if effective_rows > 0:
                # Determine how many rows this fragment should contribute
                rows_to_scan = min(remaining_limit, effective_rows)
                remaining_limit -= rows_to_scan

                yield ScanTask.python_factory_func_scan_task(
                    module=_lancedb_table_factory_function.__module__,
                    func_name=_lancedb_table_factory_function.__name__,
                    func_args=(self._ds.uri, open_kwargs, [fragment.fragment_id], required_columns, None, rows_to_scan),
                    schema=self.schema()._schema,
                    num_rows=rows_to_scan,
                    size_bytes=sum(file.file_size_bytes for file in fragment.metadata.files),
                    pushdowns=pushdowns,
                    stats=None,
                    source_type=self.name(),
                )

    def _create_regular_scan_tasks(
        self, pushdowns: PyPushdowns, required_columns: Optional[list[str]]
    ) -> Iterator[ScanTask]:
        """Create regular scan tasks without count pushdown."""
        open_kwargs = getattr(self._ds, "_lance_open_kwargs", None)
        fragments = self._ds.get_fragments()
        pushed_expr = self._combine_filters_to_arrow()

        def _python_factory_func_scan_task(
            fragment_ids: Optional[list[int]] = None,
            *,
            num_rows: Optional[int] = None,
            size_bytes: Optional[int] = None,
        ) -> ScanTask:
            return ScanTask.python_factory_func_scan_task(
                module=_lancedb_table_factory_function.__module__,
                func_name=_lancedb_table_factory_function.__name__,
                func_args=(
                    self._ds.uri,
                    open_kwargs,
                    fragment_ids,
                    required_columns,
                    pushed_expr,
                    self._compute_limit_pushdown_with_filter(pushdowns),
                ),
                schema=self.schema()._schema,
                num_rows=num_rows,
                size_bytes=size_bytes,
                pushdowns=pushdowns,
                stats=None,
                source_type=self.name(),
            )

        if self._should_use_index_for_point_lookup():
            yield _python_factory_func_scan_task(fragment_ids=None, num_rows=None, size_bytes=None)
            return

        if self._fragment_group_size is None or self._fragment_group_size <= 1:
            # default behavior: one fragment per task
            for fragment in fragments:
                num_rows = fragment.count_rows(pushed_expr)
                if num_rows == 0:
                    continue

                yield _python_factory_func_scan_task(
                    [fragment.fragment_id],
                    num_rows=num_rows,
                    size_bytes=sum(file.file_size_bytes for file in fragment.metadata.files),
                )
        else:
            # Group fragments
            fragment_groups = []
            current_group = []

            group_num_rows = 0
            group_size_bytes = 0
            for fragment in fragments:
                num_rows = fragment.count_rows(pushed_expr)
                if num_rows == 0:
                    continue

                current_group.append(fragment)
                group_num_rows += num_rows
                group_size_bytes += sum(file.file_size_bytes for file in fragment.metadata.files)
                if len(current_group) >= self._fragment_group_size:
                    fragment_groups.append((current_group, group_num_rows, group_size_bytes))
                    current_group = []
                    group_num_rows = 0
                    group_size_bytes = 0

            # Add the last group if it has any fragments
            if current_group:
                fragment_groups.append((current_group, group_num_rows, group_size_bytes))

            # Create scan tasks for each fragment group
            for fragment_group, num_rows, size_bytes in fragment_groups:
                fragment_ids = [fragment.fragment_id for fragment in fragment_group]
                yield _python_factory_func_scan_task(fragment_ids, num_rows=num_rows, size_bytes=size_bytes)

    def _combine_filters_to_arrow(self) -> Optional["pa.compute.Expression"]:
        if self._pushed_filters is not None and len(self._pushed_filters) > 0:
            combined_filter = self._pushed_filters[0]
            for filter_expr in self._pushed_filters[1:]:
                combined_filter = combined_filter & filter_expr
            return Expression._from_pyexpr(combined_filter).to_arrow_expr()
        return None

    def _compute_limit_pushdown_with_filter(self, pushdowns: PyPushdowns) -> Union[int, None]:
        """Decide whether to push down `limit` when filters are present."""
        if not self._enable_strict_filter_pushdown and pushdowns.filters is not None:
            return None

        if self._enable_strict_filter_pushdown and self._remaining_filters is not None:
            return None

        return pushdowns.limit

    def _should_use_index_for_point_lookup(self) -> bool:
        """Use index-driven scan only when all point-lookup columns have BTREE.

        Otherwise fall back to fragment enumeration. Passing fragment_ids=None signals
        index-driven scan; factory omits fragments so Lance selects them using indices.
        """
        if not self._pushed_filters:
            return False

        try:
            point_columns = detect_point_lookup_columns(
                [Expression._from_pyexpr(expr) for expr in self._pushed_filters]
            )
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning("Failed to analyze filters for point lookup: %s", e, exc_info=True)
            return False

        if not point_columns:
            return False

        point_column_set = set(point_columns)

        try:
            indices = self._ds.list_indices()
        except Exception:
            logger.warning("Unable to fetch Lance indices for dataset %s", self._ds.uri, exc_info=True)
            return False

        if not indices:
            return False

        # Decision: point-lookup uses index only if each column in the predicate has a BTREE index.
        # Rationale: avoid partial/non-exact indices (e.g., bitmap/bloom) and Lance lacks composite-prefix semantics.
        btree_indexed_columns: set[str] = set()
        for index in indices:
            index_type = str(index.get("type") or "").upper()
            if index_type != "BTREE":
                continue
            fields = index.get("fields")
            if not fields:
                continue
            for field in fields:
                btree_indexed_columns.add(field)
        # Use index-driven scan only if every point-lookup column has a BTREE index.
        if point_column_set and point_column_set.issubset(btree_indexed_columns):
            return True
        return False
