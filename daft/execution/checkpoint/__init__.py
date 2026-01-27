from __future__ import annotations

from daft.udf import cls, method
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

RAY_RESOURCE_READY_TIMEOUT_SECONDS = 10


class CheckpointActor:
    def __init__(self, partition_list: list[ray.ObjectRef], key_column_name: str):
        import ray

        partitions = ray.get(partition_list)
        key_col = [partition.to_pydict()[key_column_name] for partition in partitions]
        self.key_set = set([item for sublist in key_col for item in sublist])
        del partitions, key_col, partition_list

    def filter(self, input_keys: list[Any]) -> "np.ndarray":  # noqa: UP037
        import numpy as np

        return np.array([input_key not in self.key_set for input_key in input_keys], dtype=bool)


# TODO: support native mode in future if needed
@cls(max_concurrency=1)
class CheckpointFilter:
    def __init__(self, num_buckets: int, actor_handles: list[ActorHandle]):
        if not actor_handles:
            raise Exception(
                "CheckpointFilter disabled because no actor_handles are provided",
            )

        self._actors: list[ActorHandle] = []

        logger.info(
            "Start Initializing CheckpointFilter num_actors=%d",
            len(actor_handles),
        )

        for idx in range(num_buckets):
            try:
                actor = actor_handles[idx]
                self._actors.append(actor)
            except Exception as e:
                raise RuntimeError(
                    f"CheckpointActor_{idx} not found. Please create actors before initializing CheckpointManager."
                ) from e

        logger.info(
            "Initialized CheckpointFilter num_actors=%d",
            len(self._actors),
        )

    @method.batch(return_dtype=DataType.bool())
    def __call__(self, input: Series) -> Series:
        import numpy as np
        import ray

        input_keys = input.to_pylist()
        filter_futures = [actor.filter.remote(input_keys) for actor in self._actors]

        try:
            filter_results = ray.get(filter_futures, timeout=300)
        except Exception as e:
            raise RuntimeError(f"CheckpointActor filter failed: {e}") from e

        final_result = np.logical_and.reduce(filter_results)

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

    df_keys = None
    partition_list: list[Any] = []
    try:
        df_keys = read_fn(path=root_dirs_str, io_config=io_config, **(read_kwargs or {}))
        if key_column:
            df_keys = df_keys.select(key_column)
        partition_list = list(df_keys.iter_partitions())
    except FileNotFoundError as e:
        raise RuntimeError(f"Resume checkpoint not found at {root_dirs_str}: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unable to read checkpoint at {root_dirs_str}: {e}") from e
    finally:
        del df_keys

    if not partition_list:
        warnings.warn(f"Resume checkpoint has no existing data at {root_dirs_str}.")
        return [], None, None

    if len(partition_list) < num_buckets:
        num_buckets = len(partition_list)
        warnings.warn(
            f"num_buckets is reduced to {num_buckets} because of insufficient partitions {len(partition_list)}."
        )

    # Create placement group and actors
    import ray
    from ray.exceptions import GetTimeoutError
    from ray.util.placement_group import placement_group

    pg = placement_group([{"CPU": num_cpus} for _ in range(num_buckets)], strategy="SPREAD")
    try:
        # Wait for placement group to be ready with a timeout (seconds)
        ray.get(pg.ready(), timeout=RAY_RESOURCE_READY_TIMEOUT_SECONDS)
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

    base_len, remainder = _split_partitions_evenly(len(partition_list), num_buckets)

    actor_handles: list[ActorHandle] = []
    start = 0
    try:
        for i in range(num_buckets):
            end = start + base_len + (1 if i < remainder else 0)
            actor = (
                ray.remote(CheckpointActor)
                .options(
                    num_cpus=num_cpus,
                    scheduling_strategy=ray.util.scheduling_strategies.PlacementGroupSchedulingStrategy(
                        placement_group=pg,
                        placement_group_bundle_index=i,
                    ),
                )
                .remote(partition_list[start:end], key_column)
            )
            actor_handles.append(actor)
            start = end

        ray.get(
            [actor.__ray_ready__.remote() for actor in actor_handles],
            timeout=RAY_RESOURCE_READY_TIMEOUT_SECONDS,
        )
        logger.info("Checkpoint actors ready num_actors=%d", len(actor_handles))
    except Exception as e:
        _cleanup_checkpoint_resources(actor_handles, pg)
        raise RuntimeError(f"Failed to create all checkpoint actors: {e}") from e
    checkpoint_filter = CheckpointFilter(num_buckets=num_buckets, actor_handles=actor_handles)
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
