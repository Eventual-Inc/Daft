from __future__ import annotations

from daft.udf import cls, method
from typing import TYPE_CHECKING, Any
from daft.datatype import DataType
from daft.series import Series
from daft.runners import get_or_create_runner
from daft.expressions import col
import warnings

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

NUM_BUCKETS = "num_buckets"
KEY_COLUMN = "key_column"
NUM_CPUS = "num_cpus"
PLACEMENT_GROUP_READY_TIMEOUT_SECONDS = 10


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
    def __init__(self, num_buckets: int, actor_handles: list[ray.ActorHandle] | None = None):
        self.actors = []
        if actor_handles is None:
            self._enabled = False
        else:
            self._enabled = True
            self.actor_handles = actor_handles
            for idx in range(num_buckets):
                try:
                    actor = actor_handles[idx]
                    self.actors.append(actor)
                except ValueError as e:
                    raise RuntimeError(
                        f"CheckpointActor_{idx} not found. Please create actors before initializing CheckpointManager."
                    ) from e

    @method.batch(return_dtype=DataType.bool())
    def __call__(self, input: Series) -> Series:
        import numpy as np
        import ray

        if not self._enabled:
            return Series.from_numpy(np.full(len(input), True, dtype=bool))

        input_keys = input.to_pylist()
        filter_futures = [actor.filter.remote(input_keys) for actor in self.actors]

        try:
            filter_results = ray.get(filter_futures, timeout=300)
        except Exception as e:
            raise RuntimeError(f"CheckpointActor filter failed: {e}") from e

        final_result = np.logical_and.reduce(filter_results)

        return Series.from_numpy(final_result)


# TODO: support native mode in future if needed
def try_apply_filter_and_collect(
    write_df: DataFrame,
    root_dir: str | pathlib.Path,
    io_config: IOConfig | None,
    checkpoint_config: dict[str, Any] | None,
    read_fn: Callable[..., DataFrame],
) -> DataFrame:
    """Apply checkpoint filter and collect, return collected DataFrame.

    Args:
        write_df: DataFrame built for writing.
        root_dir: Destination root directory.
        io_config: IO configuration for reading existing keys.
        checkpoint_config: Dict with key_column/num_buckets and optional num_cpus, or None.
        read_fn: Callable to read existing data (e.g., read_parquet/read_csv/read_json).

    Returns:
        collected DataFrame: Collected DataFrame(with filter inserted if any).
    """
    try:
        actor_handles: list[ActorHandle] | None = None
        placement_group: PlacementGroup | None = None

        if checkpoint_config is not None:
            key_column, num_buckets, num_cpus = _validate_checkpoint_config(checkpoint_config)
            actor_handles, placement_group, checkpoint_filter = _prepare_checkpoint_filter(
                root_dir=root_dir,
                io_config=io_config,
                key_column=key_column,
                num_buckets=num_buckets,
                num_cpus=num_cpus,
                read_fn=read_fn,
            )
            if checkpoint_filter is not None:
                write_df = write_df._insert_filter_after_source(checkpoint_filter)

        write_df.collect()
    finally:
        try:
            _cleanup_checkpoint_resources(actor_handles, placement_group)
        except Exception as e:
            warnings.warn(f"Unable to cleanup checkpoint resources: {e}")

    return write_df


# -----------------------------------------------------------------------------
# Internal helper function: prepare checkpoint filtering for write methods
# -----------------------------------------------------------------------------
def _validate_checkpoint_config(config: dict[str, Any]) -> tuple[str, int, float]:
    """Validates checkpoint configuration contains required fields with correct types.

    Args:
        config: Checkpoint configuration dictionary

    Returns:
        Tuple of (key_column, num_buckets, nums_cpu) with validated values

    Raises:
        ValueError: If required keys are missing or values are invalid
    """
    if not isinstance(config, dict):
        raise ValueError("checkpoint_config must be a dict")
    if KEY_COLUMN not in config:
        raise ValueError(f"checkpoint_config_dict must contain '{KEY_COLUMN}' key")
    key_column = config[KEY_COLUMN]
    if not isinstance(key_column, str):
        raise ValueError(f"'{KEY_COLUMN}' must be a string, got {type(key_column).__name__}")

    # Optional nums_buckets (int > 0), default to 4 if not provided
    num_buckets_obj = config.get(NUM_BUCKETS, 4)
    try:
        nb_float = float(num_buckets_obj)
    except Exception:
        raise ValueError(f"'{NUM_BUCKETS}' must be numeric (int/float), got {num_buckets_obj}")
    if not nb_float.is_integer() or nb_float <= 0:
        raise ValueError(f"'{NUM_BUCKETS}' must be a positive integer, got {num_buckets_obj}")
    num_buckets = int(nb_float)

    # Optional nums_cpu (float > 0), default to 1 if not provided
    num_cpus_obj = config.get(NUM_CPUS, 1)
    try:
        num_cpus = float(num_cpus_obj)
    except Exception:
        raise ValueError(f"'{NUM_CPUS}' must be numeric (int/float), got {num_cpus_obj}")
    if num_cpus <= 0:
        raise ValueError(f"'{NUM_CPUS}' must be > 0, got {num_cpus}")

    return key_column, num_buckets, num_cpus


def _split_partitions_evenly(total: int, buckets: int) -> tuple[int, int]:
    """Return (base_len, remainder) for splitting total items into buckets."""
    base = total // buckets
    rem = total - buckets * base
    return base, rem


def _prepare_checkpoint_filter(
    root_dir: str | pathlib.Path,
    io_config: IOConfig | None,
    key_column: str,
    num_buckets: int,
    num_cpus: float,
    read_fn: Callable[..., DataFrame],
) -> tuple[list[ActorHandle], PlacementGroup | None, Expression | None]:
    """Build and return checkpoint resources.

    Returns:
        tuple[list[ActorHandle], PlacementGroup | None, Expression | None]:
            - actor_handles: created Ray actors for checkpoint filtering
            - placement_group: PG used to reserve/spread actor resources
            - checkpoint_filter_callable: Daft Expression used to filter input

    Notes:
        - If no existing partitions are found at `root_dir`, returns ([], None, None).
        - Raises RuntimeError if runner is not Ray.
    """
    if get_or_create_runner().name != "ray":
        raise RuntimeError("Checkpointing is only supported on Ray runner")

    # Read existing keys dataframe and extract partitions
    df_keys = None
    try:
        df_keys = read_fn(path=str(root_dir), io_config=io_config)
        if key_column:
            df_keys = df_keys.select(key_column)
        partition_list = list(df_keys.iter_partitions())
    except FileNotFoundError as e:
        warnings.warn(
            f"{root_dir} not found, checkpointing will not be supported because it's unnecessary. message: {e}"
        )
        partition_list = []
    except Exception as e:
        raise RuntimeError(f"Unable to read checkpoint at {root_dir}: {e}") from e

    if not partition_list:
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
        ray.get(pg.ready(), timeout=PLACEMENT_GROUP_READY_TIMEOUT_SECONDS)
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

    ray.get([actor.__ray_ready__.remote() for actor in actor_handles])
    del df_keys

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
            ray.kill(actor)

    if pg:
        ray.util.remove_placement_group(pg)
