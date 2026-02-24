from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, Any, cast

from daft.datatype import DataType
from daft.runners import get_or_create_runner
from daft.series import Series
from daft.udf import func

from .constants import ASYNC_AWAIT_TIMEOUT_SECONDS, PLACEMENT_GROUP_READY_TIMEOUT_SECONDS
from .key_filter_ray_mode import KeyFilterActor, _group_row_indices_by_worker

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable

    from ray.actor import ActorHandle
    from ray.util.placement_group import PlacementGroup

    from daft import DataFrame
    from daft.daft import IOConfig
    from daft.expressions import Expression

    from .key_spec import KeySpec

logger = logging.getLogger("daft.execution.skip_existing")


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


def build_key_filter_resources(
    existing_path: str | pathlib.Path | list[str | pathlib.Path],
    io_config: IOConfig | None,
    key_spec: KeySpec,
    num_workers: int,
    cpus_per_worker: float,
    read_fn: Callable[..., DataFrame],
    keys_load_batch_size: int,
    max_concurrency_per_worker: int,
    strict_path_check: bool = False,
    read_kwargs: dict[str, Any] | None = None,
) -> tuple[list[ActorHandle], PlacementGroup | None]:
    if get_or_create_runner().name != "ray":
        raise RuntimeError("skip_existing is only supported on Ray runner")

    existing_path = existing_path if isinstance(existing_path, list) else [existing_path]
    existing_path_str = [str(p) for p in existing_path]
    key_columns = key_spec.key_columns

    logger.info(
        "Preparing key filter existing_path=%s key_columns=%s num_workers=%s cpus_per_worker=%s",
        existing_path_str,
        key_columns,
        num_workers,
        cpus_per_worker,
    )

    df_keys = None
    try:
        df_keys = read_fn(path=existing_path_str, io_config=io_config, **(read_kwargs or {}))
        df_keys = df_keys.select(*key_columns)
    except FileNotFoundError as e:
        if strict_path_check:
            raise RuntimeError(f"[skip_existing] keys not found at {existing_path_str}: {e}") from e
        logger.warning(
            "[skip_existing] No existing data found at %s, processing all rows. "
            "Set strict_path_check=True to raise an error.",
            existing_path_str,
        )
        return [], None
    except Exception as e:
        raise RuntimeError(f"[skip_existing] Unable to read keys at {existing_path_str}: {e}") from e

    if df_keys is None:
        return [], None

    import ray
    from ray.exceptions import GetTimeoutError
    from ray.util.placement_group import placement_group

    pg = placement_group([{"CPU": cpus_per_worker} for _ in range(num_workers)], strategy="SPREAD")
    try:
        ray.get(pg.ready(), timeout=PLACEMENT_GROUP_READY_TIMEOUT_SECONDS)
        logger.info("[skip_existing] Key filter placement group ready")
    except GetTimeoutError as timeout_err:
        try:
            ray.util.remove_placement_group(pg)
        except Exception as e:
            warnings.warn(f"Unable to remove placement group {pg}: {e}")
        raise RuntimeError(
            "[skip_existing] Key filter resource reservation timed out. Try reducing 'num_workers' and/or 'cpus_per_worker', "
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

        _ingest_keys_to_actors(
            df_keys,
            key_spec,
            actors_by_worker=actors_by_worker,
            num_workers=num_workers,
            keys_load_batch_size=keys_load_batch_size,
        )
    except Exception as e:
        cleanup_key_filter_resources(actor_handles, pg)
        raise RuntimeError(f"[skip_existing] Failed to create all key filter actors: {e}") from e
    finally:
        del df_keys

    return actor_handles, pg


def cleanup_key_filter_resources(actor_handles: list[ActorHandle] | None, pg: PlacementGroup | None) -> None:
    import ray

    if actor_handles:
        for actor in actor_handles:
            try:
                ray.kill(actor)
            except Exception as e:
                warnings.warn(f"[skip_existing] Unable to cleanup key_filter resources: ray.kill failed: {e}")

    if pg:
        try:
            ray.util.remove_placement_group(pg)
        except Exception as e:
            warnings.warn(f"[skip_existing] Unable to cleanup key_filter resources: remove_placement_group failed: {e}")
