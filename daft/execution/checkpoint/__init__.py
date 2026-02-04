from __future__ import annotations

from daft.udf import cls, func, method
from typing import TYPE_CHECKING, Any
from daft.datatype import DataType
from daft.series import Series
from daft.runners import get_or_create_runner
from daft.expressions import col
import logging
import warnings

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from daft.expressions import Expression
    from collections.abc import Callable
    from daft import DataFrame
    from daft.daft import IOConfig
    from ray.util.placement_group import PlacementGroup
    from ray.actor import ActorHandle
    import pathlib
    import numpy as np

PLACEMENT_GROUP_READY_TIMEOUT_SECONDS = 50
ACTOR_READY_TIMEOUT_SECONDS = 7200
ASYNC_AWAIT_TIMEOUT_SECONDS = 1000


def _composite_keys_to_numpy(keys: list[Any], composite_key_fields: tuple[str, ...]) -> "np.ndarray":  # noqa: UP037
    import numpy as np

    keys_as_tuples: list[Any] = []
    for item in keys:
        if not isinstance(item, dict):
            raise TypeError(f"Expected composite key rows to be dicts, found: {type(item)}")
        keys_as_tuples.append(tuple(item.get(k) for k in composite_key_fields))
    keys_np = np.empty(len(keys_as_tuples), dtype=object)
    keys_np[:] = keys_as_tuples
    return keys_np


def _iter_bucket_row_groups(input: Series, num_buckets: int) -> list[tuple[int, "np.ndarray"]]:  # noqa: UP037
    import numpy as np

    hash_arr = input.hash().to_arrow()
    hash_np = hash_arr.to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
    bucket_ids = (hash_np % np.uint64(num_buckets)).astype(np.int64, copy=False)

    row_order = np.argsort(bucket_ids, kind="stable")
    bucket_sorted = bucket_ids[row_order]
    run_starts = np.flatnonzero(np.r_[True, bucket_sorted[1:] != bucket_sorted[:-1]])
    run_ends = np.r_[run_starts[1:], len(bucket_sorted)]
    buckets_present = bucket_sorted[run_starts]
    return [
        (int(bucket), row_order[int(start) : int(end)])
        for bucket, start, end in zip(buckets_present, run_starts, run_ends)
    ]


class CheckpointActor:
    def __init__(self, bucket_id: int):
        self.bucket_id = bucket_id
        self.key_set: set[Any] = set()

    def add_keys(self, input_keys: list[Any]) -> None:
        self.key_set.update(input_keys)

    def filter(self, input_keys: "np.ndarray") -> "np.ndarray":  # noqa: UP037
        import numpy as np

        # Convert numpy array to list for efficient set lookup
        # This avoids iterating over numpy array elements which can be slower due to unboxing overhead
        input_list = input_keys.tolist()
        bool_result = np.array([input_key not in self.key_set for input_key in input_list], dtype=bool)

        # Pack boolean array into bits (uint8) to reduce serialization overhead by 8x
        # This allows efficient bitwise operations on the driver side
        return np.packbits(bool_result)


# TODO: support native mode in future if needed
def create_checkpoint_filter_udf(
    num_buckets: int,
    actor_handles: list[ActorHandle] | None,
    composite_key_fields: tuple[str, ...] = (),
    resume_filter_batch_size: int | None = None,
) -> Callable[[Expression], Expression]:
    @func.batch(return_dtype=DataType.bool(), batch_size=resume_filter_batch_size)
    async def checkpoint_filter(input: Series) -> Series:
        import numpy as np
        import os
        import asyncio

        if actor_handles is None:
            return Series.from_numpy(np.full(len(input), True, dtype=bool))

        num_rows = len(input)
        # Log batch size occasionally (approx 1% of calls) to avoid log spam
        if num_rows > 0 and np.random.random() < 0.01:
            print(f"[PID={os.getpid()}] CheckpointFilter Batch Size: {num_rows}")

        if num_rows == 0:
            return Series.from_numpy(np.empty(0, dtype=bool))

        # Convert Input to NumPy (Zero Copy if possible)
        if not composite_key_fields:
            keys_np = input.to_arrow().to_numpy(zero_copy_only=False)
        else:
            keys = input.to_pylist()
            keys_np = _composite_keys_to_numpy(keys, composite_key_fields)

        futures = []
        row_indices_list: list[np.ndarray] = []

        # Dispatch to Actors
        for bucket, row_indices in _iter_bucket_row_groups(input, num_buckets):
            actor = actor_handles[bucket]
            keys_subset = keys_np[row_indices]

            futures.append(actor.filter.remote(keys_subset))
            row_indices_list.append(row_indices)

        try:
            # Results are list of PACKED uint8 arrays
            packed_results = await asyncio.wait_for(
                asyncio.gather(*[asyncio.wrap_future(ref.future()) for ref in futures]),
                timeout=ASYNC_AWAIT_TIMEOUT_SECONDS,
            )
        except Exception as e:
            raise RuntimeError(f"CheckpointActor filter failed: {e}") from e
        finally:
            # Explicitly release memory for large intermediate arrays
            del futures
            del keys_np

        # Reconstruct Result
        final_result = np.full(num_rows, True, dtype=bool)

        for row_indices, packed_subset in zip(row_indices_list, packed_results):
            # Unpack bits for this subset
            subset_len = len(row_indices)
            subset_mask = np.unpackbits(packed_subset)[:subset_len].astype(bool)
            final_result[row_indices] = subset_mask

        return Series.from_numpy(final_result)

    return checkpoint_filter


def _prepare_checkpoint_filter(
    root_dir: str | pathlib.Path | list[str | pathlib.Path],
    io_config: IOConfig | None,
    key_column: str | list[str],
    num_buckets: int,
    num_cpus: float,
    read_fn: Callable[..., DataFrame],
    checkpoint_loading_batch_size: int,
    checkpoint_actor_max_concurrency: int,
    read_kwargs: dict[str, Any] | None = None,
) -> tuple[list[ActorHandle], PlacementGroup | None]:
    """Build and return checkpoint resources.

    Returns:
        tuple[list[ActorHandle], PlacementGroup | None]:
            - actor_handles: created Ray actors for checkpoint filtering
            - placement_group: PG used to reserve/spread actor resources

    Notes:
        - If no existing partitions are found at `root_dir`, returns ([], None).
        - Raises RuntimeError if runner is not Ray.
    """
    if get_or_create_runner().name != "ray":
        raise RuntimeError("Checkpointing is only supported on Ray runner")
    if checkpoint_actor_max_concurrency <= 0:
        raise ValueError("resume checkpoint_actor_max_concurrency must be > 0")

    root_dirs = root_dir if isinstance(root_dir, list) else [root_dir]
    root_dirs_str = [str(p) for p in root_dirs]

    key_columns = [key_column] if isinstance(key_column, str) else key_column
    if not key_columns or any((not isinstance(c, str)) or c == "" for c in key_columns):
        raise ValueError("resume key_column must be a non-empty column name or list of non-empty column names")

    logger.info(
        "Preparing checkpoint filter root_dirs=%s key_columns=%s num_buckets=%s num_cpus=%s",
        root_dirs_str,
        key_columns,
        num_buckets,
        num_cpus,
    )

    # Build df_keys lazily; execution config for scan split/merge is applied at collect-time.
    df_keys = None
    try:
        df_keys = read_fn(path=root_dirs_str, io_config=io_config, **(read_kwargs or {}))
        df_keys = df_keys.select(*key_columns)
    except FileNotFoundError as e:
        raise RuntimeError(f"Resume checkpoint not found at {root_dirs_str}: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unable to read checkpoint at {root_dirs_str}: {e}") from e

    if df_keys is None:
        return [], None

    # Create placement group and actors
    import ray
    from ray.exceptions import GetTimeoutError
    from ray.util.placement_group import placement_group

    pg = placement_group([{"CPU": num_cpus} for _ in range(num_buckets)], strategy="SPREAD")
    try:
        # Wait for placement group to be ready with a timeout (seconds)
        ray.get(pg.ready(), timeout=PLACEMENT_GROUP_READY_TIMEOUT_SECONDS)
        logger.info("Checkpoint placement group ready")
    except GetTimeoutError as timeout_err:
        # Best effort cleanup to avoid leaking PG
        try:
            ray.util.remove_placement_group(pg)
        except Exception as e:
            warnings.warn(f"Unable to remove placement group {pg}: {e}")
        raise RuntimeError(
            "Checkpoint resource reservation timed out. Try reducing 'num_buckets' and/or 'num_cpus', "
            "or ensure your Ray cluster has sufficient resources. "
            f"Error message: {timeout_err}"
        ) from timeout_err

    actor_handles: list[ActorHandle] = []
    actors_by_bucket: dict[int, ActorHandle] = {}
    try:
        composite_key_fields = tuple(key_columns) if len(key_columns) > 1 else ()
        for i in range(num_buckets):
            actor = (
                ray.remote(max_concurrency=checkpoint_actor_max_concurrency)(CheckpointActor)
                .options(
                    num_cpus=num_cpus,
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=i,
                    ),
                )
                .remote(i)
            )
            actor_handles.append(actor)
            actors_by_bucket[i] = actor

        @func.batch(return_dtype=DataType.null())
        async def ingest_keys(
            input: Series,
            *,
            actors_by_bucket: dict[int, ActorHandle] = actors_by_bucket,
            num_buckets: int = num_buckets,
            composite_key_fields: tuple[str, ...] = composite_key_fields,
        ) -> Series:
            import asyncio
            import numpy as np
            import pyarrow as pa

            num_rows = len(input)
            if num_rows == 0:
                return Series.from_arrow(pa.nulls(0))

            keys = input.to_pylist()
            if not composite_key_fields:
                keys_np = np.asarray(keys, dtype=object)
            else:
                keys_np = _composite_keys_to_numpy(keys, composite_key_fields)

            futures = []
            for bucket, row_indices in _iter_bucket_row_groups(input, num_buckets):
                actor = actors_by_bucket[bucket]
                subset = keys_np[row_indices].tolist()
                futures.append(actor.add_keys.remote(subset))

            if futures:
                await asyncio.wait_for(asyncio.gather(*futures), timeout=ASYNC_AWAIT_TIMEOUT_SECONDS)

            return Series.from_arrow(pa.nulls(num_rows))

        if len(key_columns) == 1:
            key_expr = col(key_columns[0])
        else:
            from daft.functions.struct import to_struct

            key_expr = to_struct(**{k: col(k) for k in key_columns})
        df_keys.into_batches(checkpoint_loading_batch_size).select(ingest_keys(key_expr)).collect()
    except Exception as e:
        _cleanup_checkpoint_resources(actor_handles, pg)
        raise RuntimeError(f"Failed to create all checkpoint actors: {e}") from e
    finally:
        del df_keys

    return actor_handles, pg


def _cleanup_checkpoint_resources(actor_handles: list[ActorHandle] | None, pg: PlacementGroup | None) -> None:
    """Cleanup checkpoint resources: terminate actors and remove placement group.

    Args:
        actor_handles: List of Ray ActorHandles to terminate.
        placement_group: The Ray placement group to remove.
    """
    import ray

    if actor_handles:
        for actor in actor_handles:
            try:
                ray.kill(actor)
            except Exception as e:
                warnings.warn(f"Unable to cleanup checkpoint resources: ray.kill failed: {e}")

    if pg:
        try:
            ray.util.remove_placement_group(pg)
        except Exception as e:
            warnings.warn(f"Unable to cleanup checkpoint resources: remove_placement_group failed: {e}")
