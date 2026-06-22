"""Rust ↔ Python bridge for the key-filtering anti-join.

Provides ``create_key_filter_actors`` and ``teardown_key_filter`` —
called from Rust's ``KeyFilteringJoinNode`` via ``execute_python_coroutine``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from daft import col

from .actors import (
    KeySpec,
    build_key_filter_predicate,
    build_key_ingest_expression,
    cleanup_key_filter_resources,
    create_key_filter_actors,
)

if TYPE_CHECKING:
    from daft.daft import KeyFilteringConfig

logger = logging.getLogger("daft.execution.key_filtering_join")


def _create_actors_sync(
    config: KeyFilteringConfig,
) -> tuple[Any, Any, list[Any], Any]:
    """Synchronous core of actor creation.

    Creates Ray actors + placement group and builds the ingest and filter
    UDF expressions.  Does NOT load any keys — that happens when the
    scheduler materialises the right-side pipeline with the ingest UDF
    appended to each task.

    The ingest expression references right-side columns but aliases them
    to left-side names so that hash routing and set lookups are
    consistent between ingest and filter.
    """
    left_key_columns = config.left_key_columns
    right_key_columns = config.right_key_columns
    num_workers = config.num_workers
    cpus_per_worker = config.cpus_per_worker
    max_concurrency_per_worker = config.max_concurrency_per_worker

    if num_workers is None:
        raise RuntimeError("[key_filtering_join] num_workers must be provided")
    if cpus_per_worker is None:
        raise RuntimeError("[key_filtering_join] cpus_per_worker must be provided")
    if max_concurrency_per_worker is None:
        raise RuntimeError("[key_filtering_join] max_concurrency_per_worker must be provided")

    # Use left key column names everywhere for consistent hashing/lookups.
    key_spec = KeySpec(left_key_columns)
    key_expr = key_spec.to_expr()

    actor_handles, placement_group = create_key_filter_actors(
        key_spec=key_spec,
        num_workers=num_workers,
        cpus_per_worker=cpus_per_worker,
        max_concurrency_per_worker=max_concurrency_per_worker,
    )

    # Build the ingest expression.
    # The ingest UDF input expression references right-side columns but
    # aliases them to left-side names so the UDF sees left-compatible
    # column names — matching both the hash routing and set lookups.
    if key_spec.is_composite:
        from daft.functions.struct import to_struct

        ingest_key_expr = to_struct(
            **{left_name: col(right_name) for left_name, right_name in zip(left_key_columns, right_key_columns)}
        )
    else:
        left_name = left_key_columns[0]
        right_name = right_key_columns[0]
        ingest_key_expr = col(right_name).alias(left_name) if left_name != right_name else col(right_name)

    ingest_expr = build_key_ingest_expression(
        num_workers,
        actor_handles,
        key_spec,
    )(ingest_key_expr)

    # Build the filter expression — directly uses left-side column names.
    filter_expr = build_key_filter_predicate(
        num_workers,
        actor_handles,
        key_spec,
    )(key_expr)

    # Return internal PyExpr objects for Rust to use directly
    return (ingest_expr._expr, filter_expr._expr, actor_handles, placement_group)


async def create_key_filter(
    config: KeyFilteringConfig,
) -> list[Any]:
    """Async wrapper for actor creation.

    Called from Rust via ``execute_python_coroutine``.  Delegates the
    blocking work (placement group allocation, actor creation) to a
    separate thread via ``asyncio.to_thread``.

    Returns:
        ``[ingest_expr, filter_expr, actor_handles, placement_group]``.
        We use a list (not tuple) so Rust can extract it as ``Vec<Py<PyAny>>``.
    """
    result = await asyncio.to_thread(_create_actors_sync, config)
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
