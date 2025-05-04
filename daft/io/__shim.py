from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from daft.daft import (
    PartitionField,
    PyRecordBatch,
    ScanOperatorHandle,
    ScanTask,
)
from daft.dataframe import DataFrame
from daft.io.pushdowns import Pushdowns
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.daft import (
        Pushdowns as PyPushdowns,
    )
    from daft.io.source import DataFrameSource, DataFrameSourceTask
    from daft.schema import Schema


def _to_dataframe(self) -> DataFrame:
    scan = __DataFrameSourceShim(self)
    handle = ScanOperatorHandle.from_python_scan_operator(scan)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


class __DataFrameSourceShim(ScanOperator):
    """!! INTERNAL ONLY .. SHIM TO REUSE EXISTING BACKED WORK !!"""

    _source: DataFrameSource

    def __init__(self, source: DataFrameSource) -> None:
        self._source = source

    def schema(self) -> Schema:
        return self._source.schema

    def name(self) -> str:
        return self._source.name

    def display_name(self) -> str:
        return f"DataFrameSource({self.name()})"

    def partitioning_keys(self) -> list[PartitionField]:
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
        pds = Pushdowns._from_pypushdowns(pushdowns, self.schema())
        for task in self._source.get_tasks(pds):
            yield ScanTask.python_factory_func_scan_task(
                module=_get_record_batches.__module__,
                func_name=_get_record_batches.__name__,
                func_args=(task,),
                schema=task.schema._schema,
                num_rows=None,
                size_bytes=None,
                pushdowns=pushdowns,
                stats=None,
            )


def _get_record_batches(task: DataFrameSourceTask) -> Iterator[PyRecordBatch]:
    """The task instance has been pickled then sent to this stateless method."""
    return (batch._table for batch in task.get_record_batches())
