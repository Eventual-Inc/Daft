"""Bridge helpers for driving the `opendal` Python package from Rust.

The Rust `PythonOpenDALSource` backend (see `src/daft-io/src/opendal_python.rs`)
calls into this module to run `opendal.AsyncOperator` coroutines on a fresh
asyncio event loop. The pyo3-async-runtimes machinery requires a running event
loop at coroutine-creation time, so coroutines must be created *inside*
`run_until_complete` — hence every operation is wrapped in a small `async def`
here.

Each function is synchronous: it creates an event loop owned by the calling
(spawn-blocking) thread, runs the coroutine to completion, and closes the loop.
"""

from __future__ import annotations

import asyncio
from typing import Any


def _run(awaitable_factory):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(awaitable_factory())
    finally:
        loop.close()


def call(operator: Any, method: str, args: tuple, kwargs: dict) -> Any:
    """Run `await getattr(operator, method)(*args, **kwargs)` and return the result."""

    async def _inner():
        return await getattr(operator, method)(*args, **kwargs)

    return _run(_inner)


def list_entries(operator: Any, path: str, recursive: bool) -> list:
    """Fully drain `operator.list(...)` into a list of (path, is_dir, size)."""

    async def _inner():
        entries = []
        async for entry in await operator.list(path, recursive=recursive):
            meta = entry.metadata
            is_dir = meta.mode.is_dir()
            size = None if is_dir else meta.content_length
            entries.append((entry.path, is_dir, size))
        return entries

    return _run(_inner)
