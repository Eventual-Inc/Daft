from __future__ import annotations

import asyncio
import inspect
import queue
import warnings
from typing import TYPE_CHECKING, TypeVar

from daft.daft import (
    PyPartitionField,
    PyPushdowns,
    PyRecordBatch,
    ScanTask,
)
from daft.event_loop import get_or_init_event_loop
from daft.io.pushdowns import Pushdowns, SupportsPushdownFilters
from daft.io.scan import ScanOperator
from daft.io.source import _PyDataSourceTask

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from daft.io.source import DataSource, DataSourceTask
    from daft.schema import Schema

_T = TypeVar("_T")


class _Sentinel:
    """Sentinel value for the queue to indicate the end of the iterator."""

    pass


_SENTINEL: _Sentinel = _Sentinel()


def _drain_async_iter(async_iter: AsyncIterator[_T]) -> Iterator[_T]:
    """Synchronously drain an async iterator.

    Uses the shared ``BackgroundEventLoop`` (a persistent daemon thread running
    ``loop.run_forever()``) so that this works regardless of whether an event
    loop is already running in the calling thread (e.g., pytest-asyncio,
    Jupyter).  Items are streamed through a ``queue.Queue`` so memory is
    bounded even for large iterators.
    """
    results: queue.Queue[_T | BaseException | _Sentinel] = queue.Queue()
    event_loop = get_or_init_event_loop()

    async def _produce() -> None:
        try:
            async for item in async_iter:
                results.put(item)
        except BaseException as ex:
            results.put(ex)
        finally:
            results.put(_SENTINEL)

    # Submit without blocking so the background loop produces while we consume.
    asyncio.run_coroutine_threadsafe(_produce(), event_loop.loop)

    while True:
        item = results.get()
        if isinstance(item, _Sentinel):
            break
        if isinstance(item, BaseException):
            raise item
        yield item


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

    def _get_tasks(self, pushdowns: PyPushdowns) -> Iterator[DataSourceTask]:
        """Resolve get_tasks, handling both sync (deprecated) and async implementations."""
        pds = Pushdowns._from_pypushdowns(pushdowns)
        result = self._source.get_tasks(pds)
        if inspect.isasyncgen(result):
            yield from _drain_async_iter(result)
        elif inspect.iscoroutine(result):
            raise TypeError(
                f"{type(self._source).__name__}.get_tasks() must be an async generator "
                "(use 'async def ... yield ...' rather than returning an AsyncIterator)."
            )
        else:
            warnings.warn(
                "Sync get_tasks() is deprecated — use 'async def get_tasks()'.",
                DeprecationWarning,
                stacklevel=2,
            )
            yield from result  # type: ignore[misc]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        for task in self._get_tasks(pushdowns):
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
