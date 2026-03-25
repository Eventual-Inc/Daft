from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar

from daft.daft import (
    PyPartitionField,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
)
from daft.io.pushdowns import Pushdowns, SupportsPushdownFilters
from daft.io.scan import ScanOperator
from daft.io.source import _PyDataSourceTask

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from daft.io.source import DataSource, DataSourceTask
    from daft.schema import Schema

_T = TypeVar("_T")


def _drain_async_iter(async_iter: AsyncIterator[_T]) -> Iterator[_T]:
    """Synchronously drain an async iterator using a fresh event loop.

    Uses ``asyncio.new_event_loop()`` instead of ``asyncio.run()`` to avoid
    nesting issues when called from within an existing event loop.
    """
    loop = asyncio.new_event_loop()
    try:
        while True:
            try:
                yield loop.run_until_complete(async_iter.__anext__())
            except StopAsyncIteration:
                break
    finally:
        loop.close()


class _DataSourceShim(ScanOperator):
    """Wraps a Python DataSource as a legacy ScanOperator for the execution backend."""

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
        for task in _drain_async_iter(self._source.get_tasks(pds)):
            if isinstance(task, _PyDataSourceTask):
                # Unwrap the native ScanTask from the DataSourceTask and yield it directly.
                yield task.unwrap()
            else:
                # This is a Python-based DataSourceTask, so we need to convert it to a ScanTask.
                yield ScanTask.python_factory_func_scan_task(
                    module=_get_record_batches.__module__,
                    func_name=_get_record_batches.__name__,
                    func_args=(task,),
                    schema=task.schema._schema,
                    num_rows=None,
                    size_bytes=None,
                    pushdowns=pushdowns,
                    stats=None,
                    source_name=self.display_name(),
                )

    def as_pushdown_filter(self) -> SupportsPushdownFilters | None:
        return None


def _get_record_batches(task: DataSourceTask) -> Iterator[PyRecordBatch]:
    """The task instance has been pickled then sent to this stateless method."""
    yield from (rb._recordbatch for rb in _drain_async_iter(task.read()))
