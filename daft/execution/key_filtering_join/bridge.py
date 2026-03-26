"""Rust ↔ Python bridge for the key-filtering anti-join.

Provides ``initialize_key_filter`` and ``teardown_key_filter`` —
called from Rust's ``KeyFilteringJoinNode`` via ``execute_python_coroutine``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from daft import col
from daft.logical.builder import LogicalPlanBuilder

from .actors import (
    KeySpec,
    build_key_filter_predicate,
    build_key_filter_resources,
    cleanup_key_filter_resources,
)

if TYPE_CHECKING:
    from daft.daft import KeyFilteringConfig

logger = logging.getLogger("daft.execution.key_filtering_join")


def _initialize_key_filter_sync(
    config: KeyFilteringConfig,
    right_builder: Any,
) -> tuple[Any, list[Any], Any] | None:
    """Synchronous core of key filter initialization.

    Creates Ray actors, loads right-side join keys, and builds a filter predicate.
    This is blocking (calls ``df.collect()`` internally) and MUST NOT be called
    from a tokio worker thread.
    """
    left_key_columns = config.left_key_columns
    right_key_columns = config.right_key_columns
    num_workers = config.num_workers
    cpus_per_worker = config.cpus_per_worker
    keys_load_batch_size = config.keys_load_batch_size
    max_concurrency_per_worker = config.max_concurrency_per_worker

    if num_workers is None:
        raise RuntimeError("[key_filtering_join] num_workers must be provided")
    if cpus_per_worker is None:
        raise RuntimeError("[key_filtering_join] cpus_per_worker must be provided")
    if keys_load_batch_size is None:
        raise RuntimeError("[key_filtering_join] keys_load_batch_size must be provided")
    if max_concurrency_per_worker is None:
        raise RuntimeError("[key_filtering_join] max_concurrency_per_worker must be provided")

    key_spec = KeySpec(left_key_columns)
    key_expr = key_spec.to_expr()

    try:
        from daft.dataframe.dataframe import DataFrame

        right_df = DataFrame(LogicalPlanBuilder(right_builder))
        df_keys = right_df.select(
            *[
                col(right_name).alias(left_name) if left_name != right_name else col(right_name)
                for left_name, right_name in zip(left_key_columns, right_key_columns)
            ]
        )
    except Exception as e:
        raise RuntimeError(f"[key_filtering_join] Unable to prepare right-side key plan: {e}") from e

    actor_handles, placement_group = build_key_filter_resources(
        df_keys=df_keys,
        key_spec=key_spec,
        num_workers=num_workers,
        cpus_per_worker=cpus_per_worker,
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
    config: KeyFilteringConfig,
    right_builder: Any,
) -> list[Any]:
    """Async wrapper for key filter initialization.

    Called from Rust via ``execute_python_coroutine``. Delegates the blocking
    work (actor creation, key ingestion via ``df.collect()``) to a separate
    thread via ``asyncio.to_thread`` so the tokio runtime is not re-entered.

    Returns:
        Empty list if no existing data, or ``[filter_expr, actor_handles, placement_group]``.
        We use a list (not tuple/None) so Rust can extract it as ``Vec<Py<PyAny>>``.
    """
    result = await asyncio.to_thread(_initialize_key_filter_sync, config, right_builder)
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
