from __future__ import annotations

import asyncio
import queue
from typing import TYPE_CHECKING, TypeVar

from daft.event_loop import get_or_init_event_loop

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator, Mapping

    from daft.daft import PyRecordBatch
    from daft.io.source import DataSourceTask

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


class _TaskRecordBatchIterator:
    """Iterator over a task's record batches that also exposes the task's I/O stats.

    The Rust executor polls ``stats()`` after each batch and once the iterator is
    exhausted, folding the deltas into the scan node's runtime stats so that a
    Python ``DataSourceTask`` reports ``bytes.read`` the same way native readers do.
    """

    __slots__ = ("_inner", "_task")

    def __init__(self, task: DataSourceTask) -> None:
        self._task = task
        self._inner = (rb._recordbatch for rb in _drain_async_iter(task.read()))

    def __iter__(self) -> _TaskRecordBatchIterator:
        return self

    def __next__(self) -> PyRecordBatch:
        return next(self._inner)

    def stats(self) -> Mapping[str, int]:
        return self._task.stats()


def _get_record_batches(task: DataSourceTask) -> Iterator[PyRecordBatch]:
    """Called by the Rust python_factory_func_scan_task at execution time.

    The task instance has been pickled then sent to this stateless method.
    The returned iterator exposes a ``stats()`` method that the executor uses
    to fold the task's I/O counters into the scan operator's runtime stats.
    """
    return _TaskRecordBatchIterator(task)
