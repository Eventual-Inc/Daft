# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Optional, Union

from daft.daft import CountMode, PyCountAggregation, PyExpr, PyPartitionField, PyPushdowns, PyRecordBatch, ScanTask
from daft.dependencies import pa
from daft.io.scan import ScanOperator
from daft.logical.schema import Schema
from daft.recordbatch import RecordBatch

from ..pushdowns import SupportsPushdownFilters

if TYPE_CHECKING:
    import lance


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
    ds: "lance.LanceDataset", count_mode: CountMode, filters: Optional[list[Any]] = None
) -> RecordBatch:
    """Use LanceDB's API to count rows and return a record batch with the count result."""
    count = 0

    # 根据count_mode和filters计算行数
    if count_mode == CountMode.All:
        if filters is None:
            # 如果没有过滤器，可以直接使用元数据
            for fragment in ds.get_fragments():
                count += fragment.count_rows()
        else:
            # 如果有过滤器，需要应用过滤器后计数
            # 这可能需要实际扫描数据，效率可能不如直接使用元数据
            scanner = ds.scanner(filter=filters)
            count = sum(1 for _ in scanner.to_table().iterrows())
    elif count_mode == CountMode.Valid:
        # 计算非空值的数量
        # 这可能需要实际扫描数据
        # ...
        raise NotImplementedError("CountMode.Valid is not implemented yet.")
    elif count_mode == CountMode.Null:
        # 计算空值的数量
        # 这可能需要实际扫描数据
        # ...
        raise NotImplementedError("CountMode.Null is not implemented yet.")

    # 创建包含count结果的记录批次
    arrow_schema = pa.schema([pa.field("count", pa.uint64())])
    return RecordBatch.from_arrow_record_batches(
        [pa.RecordBatch.from_arrays([pa.array([count], type=pa.uint64())], ["count"])], arrow_schema
    )


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

    def can_absorb_aggregation(self) -> bool:
        """Returns whether this scan operator can absorb aggregation operations."""
        return True

    def supports_count_pushdown(self) -> bool:
        """Returns whether this scan operator supports count pushdown."""
        return True

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
            required_columns = (
                pushdowns.columns
                if filter_required_column_names is None
                else pushdowns.columns + filter_required_column_names
            )
        # 检查是否有count聚合下推
        if pushdowns.aggregation is not None and isinstance(pushdowns.aggregation, PyCountAggregation):
            # 处理count聚合下推
            count_mode = pushdowns.aggregation.mode

            # 处理过滤器（如果有）
            filters = None
            if self._pushed_filters is not None:
                # 将过滤器转换为Arrow表达式
                from daft.expressions import Expression

                filters = [Expression._from_pyexpr(f).to_arrow_expr() for f in self._pushed_filters]

            # 创建返回count结果的扫描任务
            yield ScanTask.python_factory_func_scan_task(
                module=_lancedb_count_result_function.__module__,
                func_name=_lancedb_count_result_function.__name__,
                func_args=(self._ds, count_mode, filters),
                schema=self.schema()._schema,
                num_rows=1,  # 结果只有一行
                size_bytes=8,  # 一个UInt64值的大小
                pushdowns=pushdowns,
                stats=None,
            )
        else:
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
