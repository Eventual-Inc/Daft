from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from daft.daft import (
    PyPartitionField,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
)
from daft.io.pushdowns import Pushdowns, SupportsPushdownFilters
from daft.io.scan import ScanOperator

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.io.source import DataSource, DataSourceTask
    from daft.schema import Schema


class _DataSourceShim(ScanOperator):
    """!! INTERNAL ONLY .. SHIM TO REUSE EXISTING BACKED WORK !!"""

    _source: DataSource

    def __init__(self, source: DataSource) -> None:
        self._source = source

    def schema(self) -> Schema:
        return self._source.schema

    def name(self) -> str:
        return self._source.name

    def display_name(self) -> str:
        return f"{self.name()}(Python)"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return [pf._partition_field for pf in self._source.get_partition_fields()]

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
        pds = Pushdowns._from_pypushdowns(pushdowns)
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
                source_type=self.name(),
            )

    def as_pushdown_filter(self) -> SupportsPushdownFilters | None:
        return None


def _get_record_batches(task: DataSourceTask) -> Iterator[PyRecordBatch]:
    """The task instance has been pickled then sent to this stateless method."""
    yield from (rb._recordbatch for mp in task.get_micro_partitions() for rb in mp.get_record_batches())
