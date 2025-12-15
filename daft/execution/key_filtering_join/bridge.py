"""Rust ↔ Python bridge for the key-filtering anti-join.

Provides ``initialize_key_filter`` and ``teardown_key_filter`` —
called from Rust's ``KeyFilteringJoinNode`` via ``execute_python_coroutine``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, cast

from daft.daft import FileFormat

from .actors import (
    KeySpec,
    build_key_filter_predicate,
    build_key_filter_resources,
    cleanup_key_filter_resources,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from daft import DataFrame
    from daft.daft import SkipExistingSpec

logger = logging.getLogger("daft.execution.key_filtering_join")


def _initialize_key_filter_sync(
    spec: SkipExistingSpec,
) -> tuple[Any, list[Any], Any] | None:
    """Synchronous core of key filter initialization.

    Creates Ray actors, loads existing keys, and builds a filter predicate.
    This is blocking (calls ``df.collect()`` internally) and MUST NOT be called
    from a tokio worker thread.
    """
    existing_path = spec.existing_path
    file_format = spec.file_format
    key_column = spec.key_column
    io_config = spec.io_config
    num_workers = spec.num_workers
    cpus_per_worker = spec.cpus_per_worker
    read_kwargs = spec.read_kwargs
    keys_load_batch_size = spec.keys_load_batch_size
    max_concurrency_per_worker = spec.max_concurrency_per_worker

    if num_workers is None:
        raise RuntimeError("[key_filtering_join] num_workers must be provided")
    if cpus_per_worker is None:
        raise RuntimeError("[key_filtering_join] cpus_per_worker must be provided")
    if keys_load_batch_size is None:
        raise RuntimeError("[key_filtering_join] keys_load_batch_size must be provided")
    if max_concurrency_per_worker is None:
        raise RuntimeError("[key_filtering_join] max_concurrency_per_worker must be provided")

    key_spec = KeySpec(key_column)
    key_expr = key_spec.to_expr()

    read_fn: Callable[..., DataFrame]
    if file_format == FileFormat.Parquet:
        from daft.io._parquet import read_parquet

        read_fn = cast("Callable[..., DataFrame]", read_parquet)
    elif file_format == FileFormat.Csv:
        from daft.io._csv import read_csv

        read_fn = cast("Callable[..., DataFrame]", read_csv)
    elif file_format == FileFormat.Json:
        from daft.io._json import read_json

        read_fn = cast("Callable[..., DataFrame]", read_json)
    else:
        raise ValueError(f"[key_filtering_join] Unsupported file format: {file_format}")

    actor_handles, placement_group = build_key_filter_resources(
        existing_path=cast("Any", existing_path),
        io_config=io_config,
        key_spec=key_spec,
        num_workers=num_workers,
        cpus_per_worker=cpus_per_worker,
        read_fn=read_fn,
        read_kwargs=read_kwargs,
        keys_load_batch_size=keys_load_batch_size,
        max_concurrency_per_worker=max_concurrency_per_worker,
    )

    if not actor_handles:
        return None

    key_filter_expr = build_key_filter_predicate(
        num_workers,
        actor_handles,
        key_spec,
    )(key_expr)

    # Return the internal PyExpr (Expression._expr) for Rust to use directly
    return (key_filter_expr._expr, actor_handles, placement_group)


async def initialize_key_filter(
    spec: SkipExistingSpec,
) -> list[Any]:
    """Async wrapper for key filter initialization.

    Called from Rust via ``execute_python_coroutine``. Delegates the blocking
    work (actor creation, key ingestion via ``df.collect()``) to a separate
    thread via ``asyncio.to_thread`` so the tokio runtime is not re-entered.

    Returns:
        Empty list if no existing data, or ``[filter_expr, actor_handles, placement_group]``.
        We use a list (not tuple/None) so Rust can extract it as ``Vec<Py<PyAny>>``.
    """
    result = await asyncio.to_thread(_initialize_key_filter_sync, spec)
    if result is None:
        return []
    return list(result)


def teardown_key_filter(
    actor_handles: list[Any] | Any,
    placement_group: Any,
) -> None:
    """Cleanup Ray actors and placement group.

    Called from Rust during ``KeyFilterActors::teardown()``.
    """
    handles = actor_handles if isinstance(actor_handles, list) else []
    pg = placement_group if placement_group is not None else None
    cleanup_key_filter_resources(handles, pg)
