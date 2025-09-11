from __future__ import annotations

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
    return _get_or_create_runner()


def get_or_infer_runner_type() -> str:
    """Get or infer the runner type.

    This API will get or infer the currently used runner type according to the following strategies:
    1. If the `runner` has been set, return its type directly;
    2. Try to determine whether it's currently running on a ray cluster. If so, consider it to be a ray type;
    3. Try to determine based on `DAFT_RUNNER` env variable.

    :return: runner type string ("native" or "ray")
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
    max_task_backlog: int | None = None,
    force_client_mode: bool = False,
) -> Runner[PartitionT]:
    """Configure Daft to execute dataframes using the Ray distributed computing framework.

    Args:
        address: Ray cluster address to connect to. If None, connects to or starts a local Ray instance.
        noop_if_initialized: If True, skip initialization if Ray is already running.
        max_task_backlog: Maximum number of tasks that can be queued. None means Daft will automatically determine a good default.
        force_client_mode: If True, forces Ray to run in client mode.

    Returns:
        Runner[PartitionT]: A runner object with the Ray runner's configurations.

    Note:
        Can also be configured via environment variable: DAFT_RUNNER=ray
    """
    return _set_runner_ray(
        address=address,
        noop_if_initialized=noop_if_initialized,
        max_task_backlog=max_task_backlog,
        force_client_mode=force_client_mode,
    )
