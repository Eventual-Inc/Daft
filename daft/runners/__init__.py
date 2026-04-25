from __future__ import annotations

import os

from typing import TYPE_CHECKING
from daft.daft import get_runner as _get_runner_internal
from daft.daft import get_or_create_runner as _get_or_create_runner
from daft.daft import get_or_infer_runner_type as _get_or_infer_runner_type
from daft.daft import set_runner_native as _set_runner_native
from daft.daft import set_runner_ray as _set_runner_ray

if TYPE_CHECKING:
    from daft.runners.runner import Runner
    from daft.runners.partitioning import PartitionT


def _get_runner() -> Runner[PartitionT] | None:
    """Internal testing function to check the currently set runner."""
    return _get_runner_internal()


def get_or_create_runner() -> Runner[PartitionT]:
    """Get or create the current runner instance.

    If a runner has already been set, returns it. Otherwise, creates a new
    runner using the default configuration (native) and locks it in.

    Returns:
        Runner[PartitionT]: The current runner instance.

    Note:
        After calling this function, the runner cannot be changed for the
        lifetime of the process. Use ``get_or_infer_runner_type`` to check the
        runner type without this side effect.
    """
    return _get_or_create_runner()


def get_or_infer_runner_type() -> str:
    """Get or infer the runner type.

    This API will get or infer the currently used runner type according to the following strategies:
    1. If the `runner` has been set, return its type directly;
    2. Try to determine whether it's currently running on a ray cluster. If so, consider it to be a ray type;
    3. Try to determine based on `DAFT_RUNNER` env variable.

    Returns:
        str: The runner type ("native" or "ray").
    """
    return _get_or_infer_runner_type()


def set_runner_native(num_threads: int | None = None) -> Runner[PartitionT]:
    """Configure Daft to execute dataframes using native multi-threaded processing.

    This is the default execution mode for Daft.

    Returns:
        Runner[PartitionT]: A runner object with the native runner's configuration.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=native
    """
    return _set_runner_native(num_threads)


def set_runner_ray(
    address: str | None = None,
    noop_if_initialized: bool = False,
    force_client_mode: bool = False,
    *,
    downscale_enabled: bool | None = None,
    downscale_idle_seconds: int | None = None,
    min_survivor_workers: int | None = None,
    pending_release_exclude_seconds: int | None = None,
) -> Runner[PartitionT]:
    """Configure Daft to execute dataframes using the Ray distributed computing framework.

    Args:
        address: Ray cluster address to connect to. If None, connects to or starts a local Ray instance.
        noop_if_initialized: If True, skip initialization if Ray is already running.
        force_client_mode: If True, forces Ray to run in client mode.
        downscale_enabled: Enable/disable retiring idle Ray workers (scale-in). If not provided,
            falls back to the ``DAFT_AUTOSCALING_DOWNSCALE_ENABLED`` environment variable (default: False).
        downscale_idle_seconds: Minimum number of seconds a worker must be idle before it becomes eligible
            for retirement. If not provided, falls back to ``DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS``
            (default: 60).
        min_survivor_workers: Minimum number of Ray workers to keep alive even if they are idle.
            If not provided, falls back to ``DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS`` (default: 1).
        pending_release_exclude_seconds: Grace period (TTL) for recently-released worker IDs during
            worker discovery, to prevent the autoscaler from immediately respawning them. If not
            provided, falls back to ``DAFT_AUTOSCALING_PENDING_RELEASE_EXCLUDE_SECONDS`` (default: 120).

    Returns:
        Runner[PartitionT]: A runner object with the Ray runner's configurations.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=ray
    """

    # Allow programmatic configuration of autoscaling/downscaling behavior via `daft.set_runner_ray`.
    # These settings are still backed by environment variables so they can propagate to the Rust
    # scheduler/worker-manager components without threading configuration throughout the stack.
    if downscale_enabled is not None:
        os.environ["DAFT_AUTOSCALING_DOWNSCALE_ENABLED"] = "1" if downscale_enabled else "0"
    if downscale_idle_seconds is not None:
        if downscale_idle_seconds < 0:
            raise ValueError("downscale_idle_seconds must be >= 0")
        os.environ["DAFT_AUTOSCALING_DOWNSCALE_IDLE_SECONDS"] = str(downscale_idle_seconds)
    if min_survivor_workers is not None:
        if min_survivor_workers < 0:
            raise ValueError("min_survivor_workers must be >= 0")
        os.environ["DAFT_AUTOSCALING_MIN_SURVIVOR_WORKERS"] = str(min_survivor_workers)
    if pending_release_exclude_seconds is not None:
        if pending_release_exclude_seconds < 0:
            raise ValueError("pending_release_exclude_seconds must be >= 0")
        os.environ["DAFT_AUTOSCALING_PENDING_RELEASE_EXCLUDE_SECONDS"] = str(
            pending_release_exclude_seconds
        )

    return _set_runner_ray(
        address=address,
        noop_if_initialized=noop_if_initialized,
        force_client_mode=force_client_mode,
    )
