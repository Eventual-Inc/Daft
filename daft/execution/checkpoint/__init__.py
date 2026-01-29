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
    import ray

PLACEMENT_GROUP_READY_TIMEOUT_SECONDS = 10
ACTOR_READY_TIMEOUT_SECONDS = 7200


class CheckpointActor:
    def __init__(self, bucket_id: int):
        self.bucket_id = bucket_id
        self.key_set: set[Any] = set()

    def add_keys(self, input_keys: list[Any]) -> None:
        self.key_set.update(input_keys)

    def filter(self, input_keys: list[Any]) -> "np.ndarray":  # noqa: UP037
        import numpy as np

        return np.array([input_key not in self.key_set for input_key in input_keys], dtype=bool)


# TODO: support native mode in future if needed
@cls(max_concurrency=1)
class CheckpointFilter:
    def __init__(self, num_buckets: int, actors_by_bucket: dict[int, ray.ActorHandle] | None = None):
        self.num_buckets = num_buckets
        self.actors_by_bucket: dict[int, ray.ActorHandle] = {}
        if actors_by_bucket is None:
            self._enabled = False
        else:
            self._enabled = True
            for idx in range(num_buckets):
                try:
                    self.actors_by_bucket[idx] = actors_by_bucket[idx]
                except KeyError as e:
                    raise RuntimeError(
                        f"CheckpointActor_{idx} not found. Please create actors before initializing CheckpointManager."
                    ) from e

    @method.batch(return_dtype=DataType.bool())
    def __call__(self, input: Series) -> Series:
        import numpy as np
        import ray

        if not self._enabled:
            return Series.from_numpy(np.full(len(input), True, dtype=bool))

        num_rows = len(input)
        if num_rows == 0:
            return Series.from_numpy(np.empty(0, dtype=bool))
        hash_arr = input.hash().to_arrow()
        hash_np = hash_arr.to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
        bucket_ids = (hash_np % np.uint64(self.num_buckets)).astype(np.int64, copy=False)

        futures = []
        row_indices_list: list[np.ndarray] = []
        row_order = np.argsort(bucket_ids, kind="stable")
        bucket_sorted = bucket_ids[row_order]
        run_starts = np.flatnonzero(np.r_[True, bucket_sorted[1:] != bucket_sorted[:-1]])
        run_ends = np.r_[run_starts[1:], len(bucket_sorted)]
        buckets_present = bucket_sorted[run_starts]

        # row_indices 是 input 里的行号（0-based），表示哪些 key 属于这个 bucket
        for bucket, start, end in zip(buckets_present, run_starts, run_ends):
            actor = self.actors_by_bucket[int(bucket)]
            row_indices = row_order[int(start) : int(end)]
            keys_subset = input.take(Series.from_numpy(row_indices, name="idx")).to_pylist()  # TODO:耗时多
            futures.append(actor.filter.remote(keys_subset))  # TODO：耗时多
            row_indices_list.append(row_indices)
        try:
            results = ray.get(futures, timeout=300)  # TODO：耗时多
        except Exception as e:
            raise RuntimeError(f"CheckpointActor filter failed: {e}") from e

        final_result = np.full(num_rows, True, dtype=bool)
        for row_indices, subset_mask in zip(row_indices_list, results):
            final_result[row_indices] = subset_mask
        return Series.from_numpy(final_result)


def _split_partitions_evenly(total: int, buckets: int) -> tuple[int, int]:
    """Return (base_len, remainder) for splitting total items into buckets."""
    base = total // buckets
    rem = total - buckets * base
    return base, rem


def _prepare_checkpoint_filter(
    root_dir: str | pathlib.Path | list[str | pathlib.Path],
    io_config: IOConfig | None,
    key_column: str,
    num_buckets: int,
    num_cpus: float,
    read_fn: Callable[..., DataFrame],
    read_kwargs: dict[str, Any] | None = None,
) -> tuple[list[ActorHandle], PlacementGroup | None, Expression | None]:
    """Build and return checkpoint resources.

    Returns:
        tuple[list[ActorHandle], PlacementGroup | None, Expression | None]:
            - actor_handles: created Ray actors for checkpoint filtering
            - placement_group: PG used to reserve/spread actor resources
            - checkpoint_filter_callable: Daft Expression used to filter input

    Notes:
        - Raises RuntimeError if the checkpoint path cannot be read.
        - Raises RuntimeError if runner is not Ray.
    """
    if get_or_create_runner().name != "ray":
        raise RuntimeError("Checkpointing is only supported on Ray runner")

    root_dirs = root_dir if isinstance(root_dir, list) else [root_dir]
    root_dirs_str = [str(p) for p in root_dirs]

    logger.info(
        "Preparing checkpoint filter root_dirs=%s key_column=%s num_buckets=%s num_cpus=%s",
        root_dirs_str,
        key_column,
        num_buckets,
        num_cpus,
    )

    # Build df_keys lazily; execution config for scan split/merge is applied at collect-time.
    df_keys = None
    try:
        df_keys = read_fn(path=root_dirs_str, io_config=io_config, **(read_kwargs or {}))
        if key_column:
            df_keys = df_keys.select(key_column)
    except FileNotFoundError as e:
        raise RuntimeError(f"Resume checkpoint not found at {root_dirs_str}: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unable to read checkpoint at {root_dirs_str}: {e}") from e

    if df_keys is None:
        return [], None, None

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
        for i in range(num_buckets):
            actor = (
                ray.remote(max_concurrency=10)(CheckpointActor)
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
        ) -> Series:
            import asyncio
            import numpy as np
            import pyarrow as pa

            num_rows = len(input)
            if num_rows == 0:
                return Series.from_arrow(pa.nulls(0))

            keys = input.to_pylist()
            keys_np = np.asarray(keys, dtype=object)

            hash_arr = input.hash().to_arrow()
            hash_np = hash_arr.to_numpy(zero_copy_only=False).astype(np.uint64, copy=False)
            bucket_ids = (hash_np % np.uint64(num_buckets)).astype(np.int64, copy=False)

            row_order = np.argsort(bucket_ids, kind="stable")
            bucket_sorted = bucket_ids[row_order]
            run_starts = np.flatnonzero(np.r_[True, bucket_sorted[1:] != bucket_sorted[:-1]])
            run_ends = np.r_[run_starts[1:], len(bucket_sorted)]
            buckets_present = bucket_sorted[run_starts]

            futures = []
            for bucket, start, end in zip(buckets_present, run_starts, run_ends):
                actor = actors_by_bucket[int(bucket)]
                subset = keys_np[row_order[int(start) : int(end)]].tolist()
                print(f"in ingest_keys: bucket={bucket} start={start} end={end} num_keys={len(subset)}")
                futures.append(actor.add_keys.remote(subset))

            if futures:
                await asyncio.wait_for(asyncio.gather(*futures), timeout=300)

            return Series.from_arrow(pa.nulls(num_rows))

        df_keys.select(ingest_keys(col(key_column))).collect()
    except Exception as e:
        _cleanup_checkpoint_resources(actor_handles, pg)
        raise RuntimeError(f"Failed to create all checkpoint actors: {e}") from e
    finally:
        del df_keys

    checkpoint_filter = CheckpointFilter(num_buckets=num_buckets, actors_by_bucket=actors_by_bucket)
    checkpoint_filter_callable = checkpoint_filter(col(key_column))  # type: ignore
    return actor_handles, pg, checkpoint_filter_callable  # type: ignore


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
