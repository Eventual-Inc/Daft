from __future__ import annotations

from typing import TYPE_CHECKING

from daft.io.source import DataSourceTask
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from daft.daft import PyDataSourceTask
    from daft.recordbatch import RecordBatch


class _RustDataSourceTask(DataSourceTask):
    """Wraps a Rust-backed ``PyDataSourceTask`` as a Python ``DataSourceTask``.

    This follows the same pattern as ``_RustTable(Table)`` in ``daft.catalog``:
    the Python ABC subclass delegates to the Rust object so that
    ``isinstance(task, DataSourceTask)`` holds.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: PyDataSourceTask) -> None:
        self._inner = inner

    @property
    def schema(self) -> Schema:
        return Schema._from_pyschema(self._inner.schema())

    async def read(self) -> AsyncIterator[RecordBatch]:
        """We do not yet support reading native scan tasks via Python."""
        raise NotImplementedError("Native scan tasks are executed by the Rust engine, not via read().")
        yield  # pragma: no cover
