"""Key-filtering anti-join: Ray actor infrastructure.

Provides ``KeyFilterActor`` (stores keys and filters against them),
``build_key_filter_predicate`` (creates a UDF that distributes filtering
across actors), and ``build_key_filter_resources`` / ``cleanup_key_filter_resources``
(lifecycle management for Ray actors and placement groups).
"""

from __future__ import annotations

import logging
import os
import warnings
from typing import TYPE_CHECKING, Any, cast

from daft.datatype import DataType
from daft.expressions import col
from daft.runners import get_or_create_runner
from daft.series import Series
from daft.udf import func

if TYPE_CHECKING:
    from collections.abc import Callable

    from ray.actor import ActorHandle
    from ray.util.placement_group import PlacementGroup

    from daft import DataFrame
    from daft.dependencies import np
    from daft.expressions import Expression

logger = logging.getLogger("daft.execution.key_filtering_join")

# ---------------------------------------------------------------------------
# Constants (configurable via environment variables)
# ---------------------------------------------------------------------------
PLACEMENT_GROUP_READY_TIMEOUT_SECONDS = int(
    os.environ.get("DAFT_KEY_FILTERING_PLACEMENT_GROUP_READY_TIMEOUT_SECONDS", "50")
)
ASYNC_AWAIT_TIMEOUT_SECONDS = int(os.environ.get("DAFT_KEY_FILTERING_ASYNC_AWAIT_TIMEOUT_SECONDS", "1000"))

# ---------------------------------------------------------------------------
# KeySpec: key column specification
# ---------------------------------------------------------------------------


class KeySpec:
    def __init__(self, key_columns: list[str]):
        if not key_columns or any((not isinstance(c, str)) or c == "" for c in key_columns):
            raise ValueError(
                "[key_filtering_join] key_column must be a non-empty column name or list of non-empty column names"
            )
        self.key_columns = key_columns
        self.is_composite = len(key_columns) > 1
        self.composite_key_fields = tuple(key_columns) if self.is_composite else ()

    def to_expr(self) -> Expression:
        if self.is_composite:
            from daft.functions.struct import to_struct

            return to_struct(**{k: col(k) for k in self.key_columns})
        return col(self.key_columns[0])

    def to_numpy(self, input: Series) -> "np.ndarray":  # noqa: UP037
        if self.is_composite:
            keys = input.to_pylist()
            return _composite_keys_to_numpy(keys, self.composite_key_fields)
        return input.to_arrow().to_numpy(zero_copy_only=False)


def _composite_keys_to_numpy(keys: list[Any], composite_key_fields: tuple[str, ...]) -> "np.ndarray":  # noqa: UP037
    from daft.dependencies import np

    keys_as_tuples: list[Any] = []
    for item in keys:
        if not isinstance(item, dict):
            raise TypeError(f"Expected composite key rows to be dicts, found: {type(item)}")
        keys_as_tuples.append(tuple(item.get(k) for k in composite_key_fields))
    keys_np = np.empty(len(keys_as_tuples), dtype=object)
    keys_np[:] = keys_as_tuples
    return keys_np


# ---------------------------------------------------------------------------
# KeyFilterActor: Ray actor storing keys and filtering against them
# ---------------------------------------------------------------------------


class KeyFilterActor:
    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.key_set: set[Any] = set()

    def add_keys(self, input_keys: list[Any]) -> None:
        self.key_set.update(input_keys)

    def filter(self, input_keys: "np.ndarray") -> "np.ndarray":  # noqa: UP037
        from daft.dependencies import np

        input_list = input_keys.tolist()
        bool_result = np.array([input_key not in self.key_set for input_key in input_list], dtype=bool)
        return np.packbits(bool_result)


# ---------------------------------------------------------------------------
# Hash-based row distribution
# ---------------------------------------------------------------------------


def _group_row_indices_by_worker(input: Series, num_workers: int) -> list[tuple[int, "np.ndarray"]]:  # noqa: UP037
    from daft.dependencies import np

    hash_arr = input.hash().to_arrow()
    hash_np = hash_arr.to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
    worker_ids = (hash_np % np.uint64(num_workers)).astype(np.int64, copy=False)
    del hash_arr
    del hash_np

    row_order = np.argsort(worker_ids, kind="stable")
    worker_sorted = worker_ids[row_order]
    run_starts = np.flatnonzero(np.r_[True, worker_sorted[1:] != worker_sorted[:-1]])
    run_ends = np.r_[run_starts[1:], len(worker_sorted)]
    workers_present = worker_sorted[run_starts]
    return [
        (int(worker_id), row_order[int(start) : int(end)])
        for worker_id, start, end in zip(workers_present, run_starts, run_ends)
    ]


# ---------------------------------------------------------------------------
# build_key_filter_predicate: creates a UDF that filters rows via actors
# ---------------------------------------------------------------------------


def build_key_filter_predicate(
    num_workers: int,
    actor_handles: list[ActorHandle],
    key_spec: KeySpec,
) -> Callable[[Expression], Expression]:
    @func.batch(return_dtype=DataType.bool())
    def key_filter(input: Series) -> Series:
        import os

        import ray

        from daft.dependencies import np

        num_rows = len(input)
        if num_rows > 0 and np.random.random() < 0.01:
            logger.debug("[PID=%s] KeyFilter Batch Size: %d", os.getpid(), num_rows)

        if num_rows == 0:
            return Series.from_numpy(np.empty(0, dtype=bool))

        keys_np = key_spec.to_numpy(input)

        futures = []
        row_indices_list: list[np.ndarray] = []

        for worker_id, row_indices in _group_row_indices_by_worker(input, num_workers):
            actor = actor_handles[worker_id]
            keys_subset = keys_np[row_indices]

            futures.append(actor.filter.remote(keys_subset))
            row_indices_list.append(row_indices)

        try:
            packed_results = ray.get(futures)
        except Exception as e:
            raise RuntimeError(f"KeyFilterActor filter failed: {e}") from e
        finally:
            del futures
            del keys_np

        final_result = np.full(num_rows, True, dtype=bool)

        for row_indices, packed_subset in zip(row_indices_list, packed_results):
            subset_len = len(row_indices)
            subset_mask = np.unpackbits(packed_subset)[:subset_len].astype(bool)
            final_result[row_indices] = subset_mask

        return Series.from_numpy(final_result)

    return key_filter


# ---------------------------------------------------------------------------
# Key ingestion UDF (loads right-side join keys into actors)
# ---------------------------------------------------------------------------


@func.batch(return_dtype=DataType.null())
async def _ingest_keys_udf(
    input: Series,
    *,
    actors_by_worker: dict[int, Any],
    num_workers: int,
    key_spec: KeySpec,
) -> Series:
    import asyncio

    from daft.dependencies import pa

    num_rows = len(input)
    if num_rows == 0:
        return Series.from_arrow(pa.nulls(0))

    keys_np = key_spec.to_numpy(input)

    futures = []
    for worker_id, row_indices in _group_row_indices_by_worker(input, num_workers):
        actor = actors_by_worker[worker_id]
        subset = keys_np[row_indices].tolist()
        futures.append(actor.add_keys.remote(subset))
    del keys_np

    if futures:
        await asyncio.wait_for(asyncio.gather(*futures), timeout=ASYNC_AWAIT_TIMEOUT_SECONDS)

    return Series.from_arrow(pa.nulls(num_rows))


def _ingest_keys_to_actors(
    df_keys: DataFrame,
    key_spec: KeySpec,
    *,
    actors_by_worker: dict[int, Any],
    num_workers: int,
    keys_load_batch_size: int,
) -> None:
    key_expr = key_spec.to_expr()
    expr = cast(
        "Expression",
        _ingest_keys_udf(
            key_expr,
            actors_by_worker=actors_by_worker,
            num_workers=num_workers,
            key_spec=key_spec,
        ),
    )
    df_keys.into_batches(keys_load_batch_size).select(expr).collect()


# ---------------------------------------------------------------------------
# Resource management: build and cleanup
# ---------------------------------------------------------------------------


def build_key_filter_resources(
    df_keys: DataFrame,
    key_spec: KeySpec,
    num_workers: int,
    cpus_per_worker: float,
    keys_load_batch_size: int,
    max_concurrency_per_worker: int,
) -> tuple[list[ActorHandle], PlacementGroup | None]:
    if get_or_create_runner().name != "ray":
        raise RuntimeError("key_filtering_join is only supported on Ray runner")

    key_columns = key_spec.key_columns

    logger.info(
        "Preparing key filter key_columns=%s num_workers=%s cpus_per_worker=%s",
        key_columns,
        num_workers,
        cpus_per_worker,
    )

    import ray
    from ray.exceptions import GetTimeoutError
    from ray.util.placement_group import placement_group

    pg = placement_group([{"CPU": cpus_per_worker} for _ in range(num_workers)], strategy="SPREAD")
    try:
        ray.get(pg.ready(), timeout=PLACEMENT_GROUP_READY_TIMEOUT_SECONDS)
        logger.info("[key_filtering_join] Key filter placement group ready")
    except GetTimeoutError as timeout_err:
        try:
            ray.util.remove_placement_group(pg)
        except Exception as e:
            warnings.warn(f"Unable to remove placement group {pg}: {e}")
        raise RuntimeError(
            "[key_filtering_join] Key filter resource reservation timed out. Try reducing 'num_workers' and/or 'cpus_per_worker', "
            "or ensure your Ray cluster has sufficient resources. "
            f"Error message: {timeout_err}"
        ) from timeout_err

    actor_handles: list[ActorHandle] = []
    actors_by_worker: dict[int, ActorHandle] = {}
    try:
        for i in range(num_workers):
            actor = (
                ray.remote(max_concurrency=max_concurrency_per_worker)(KeyFilterActor)
                .options(
                    num_cpus=cpus_per_worker,
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=i,
                    ),
                )
                .remote(i)
            )
            actor_handles.append(actor)
            actors_by_worker[i] = actor

        try:
            _ingest_keys_to_actors(
                df_keys,
                key_spec,
                actors_by_worker=actors_by_worker,
                num_workers=num_workers,
                keys_load_batch_size=keys_load_batch_size,
            )
        except Exception as e:
            raise RuntimeError(f"[key_filtering_join] Unable to read keys from right side: {e}") from e
    except RuntimeError:
        cleanup_key_filter_resources(actor_handles, pg)
        raise
    except Exception as e:
        cleanup_key_filter_resources(actor_handles, pg)
        raise RuntimeError(f"[key_filtering_join] Failed to create all key filter actors: {e}") from e

    return actor_handles, pg


def cleanup_key_filter_resources(actor_handles: list[ActorHandle] | None, pg: PlacementGroup | None) -> None:
    import ray

    if actor_handles:
        for actor in actor_handles:
            try:
                ray.kill(actor)
            except Exception as e:
                warnings.warn(f"[key_filtering_join] Unable to cleanup resources: ray.kill failed: {e}")

    if pg:
        try:
            ray.util.remove_placement_group(pg)
        except Exception as e:
            warnings.warn(f"[key_filtering_join] Unable to cleanup resources: remove_placement_group failed: {e}")
