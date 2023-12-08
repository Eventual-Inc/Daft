from __future__ import annotations

import dataclasses
import logging
import os
import warnings
from typing import TYPE_CHECKING, ClassVar

from daft.daft import PyDaftConfig

if TYPE_CHECKING:
    from daft.runners.runner import Runner

logger = logging.getLogger(__name__)


class _RunnerConfig:
    name = ClassVar[str]


@dataclasses.dataclass(frozen=True)
class _PyRunnerConfig(_RunnerConfig):
    name = "py"
    use_thread_pool: bool | None


@dataclasses.dataclass
class _RayRunnerConfig(_RunnerConfig):
    name = "ray"
    address: str | None
    max_task_backlog: int | None


def _get_runner_config_from_env() -> _RunnerConfig:
    """Retrieves the appropriate RunnerConfig from environment variables

    To use:

    1. PyRunner: set DAFT_RUNNER=py
    2. RayRunner: set DAFT_RUNNER=ray and optionally DAFT_RAY_ADDRESS=ray://...
    """
    runner = os.getenv("DAFT_RUNNER") or "PY"
    if runner.upper() == "RAY":
        task_backlog_env = os.getenv("DAFT_DEVELOPER_RAY_MAX_TASK_BACKLOG")
        return _RayRunnerConfig(
            address=os.getenv("DAFT_RAY_ADDRESS"),
            max_task_backlog=int(task_backlog_env) if task_backlog_env else None,
        )
    elif runner.upper() == "PY":
        use_thread_pool_env = os.getenv("DAFT_DEVELOPER_USE_THREAD_POOL")
        use_thread_pool = bool(int(use_thread_pool_env)) if use_thread_pool_env is not None else None
        return _PyRunnerConfig(use_thread_pool=use_thread_pool)
    raise ValueError(f"Unsupported DAFT_RUNNER variable: {runner}")


@dataclasses.dataclass
class DaftContext:
    """Global context for the current Daft execution environment"""

    daft_config: PyDaftConfig = PyDaftConfig()
    runner_config: _RunnerConfig = dataclasses.field(default_factory=_get_runner_config_from_env)
    disallow_set_runner: bool = False
    _runner: Runner | None = None

    def runner(self) -> Runner:
        if self._runner is not None:
            return self._runner

        if self.runner_config.name == "ray":
            from daft.runners.ray_runner import RayRunner

            assert isinstance(self.runner_config, _RayRunnerConfig)
            self._runner = RayRunner(
                daft_config=self.daft_config,
                address=self.runner_config.address,
                max_task_backlog=self.runner_config.max_task_backlog,
            )
        elif self.runner_config.name == "py":
            from daft.runners.pyrunner import PyRunner

            try:
                import ray

                if ray.is_initialized():
                    logger.warning(
                        "WARNING: Daft is NOT using Ray for execution!\n"
                        "Daft is using the PyRunner but we detected an active Ray connection. "
                        "If you intended to use the Daft RayRunner, please first run `daft.context.set_runner_ray()` "
                        "before executing Daft queries."
                    )
            except ImportError:
                pass

            assert isinstance(self.runner_config, _PyRunnerConfig)
            self._runner = PyRunner(daft_config=self.daft_config, use_thread_pool=self.runner_config.use_thread_pool)

        else:
            raise NotImplementedError(f"Runner config implemented: {self.runner_config.name}")

        # Mark DaftContext as having the runner set, which prevents any subsequent setting of the config
        # after the runner has been initialized once
        self.disallow_set_runner = True

        return self._runner

    @property
    def is_ray_runner(self) -> bool:
        return isinstance(self.runner_config, _RayRunnerConfig)


_DaftContext = DaftContext()


def get_context() -> DaftContext:
    return _DaftContext


def set_context(ctx: DaftContext) -> DaftContext:
    global _DaftContext

    pop_context()
    _DaftContext = ctx

    return _DaftContext


def pop_context() -> DaftContext:
    """Helper used in tests and test fixtures to clear the global runner and allow for re-setting of configs."""
    global _DaftContext

    old_daft_context = _DaftContext
    _DaftContext = DaftContext()

    return old_daft_context


def set_runner_ray(
    address: str | None = None,
    noop_if_initialized: bool = False,
    max_task_backlog: int | None = None,
) -> DaftContext:
    """Set the runner for executing Daft dataframes to a Ray cluster

    Alternatively, users can set this behavior via environment variables:

    1. DAFT_RUNNER=ray
    2. Optionally, DAFT_RAY_ADDRESS=ray://...

    **This function will throw an error if called multiple times in the same process.**

    Args:
        address: Address to head node of the Ray cluster. Defaults to None.
        noop_if_initialized: If set to True, only the first call to this function will have any effect in setting the Runner.
            Subsequent calls will have no effect at all. Defaults to False, which throws an error if this function is called
            more than once per process.

    Returns:
        DaftContext: Daft context after setting the Ray runner
    """
    ctx = get_context()
    if ctx.disallow_set_runner:
        if noop_if_initialized:
            warnings.warn(
                "Calling daft.context.set_runner_ray(noop_if_initialized=True) multiple times has no effect beyond the first call."
            )
            return ctx
        raise RuntimeError("Cannot set runner more than once")

    ctx.runner_config = _RayRunnerConfig(
        address=address,
        max_task_backlog=max_task_backlog,
    )
    ctx.disallow_set_runner = True
    return ctx


def set_runner_py(use_thread_pool: bool | None = None) -> DaftContext:
    """Set the runner for executing Daft dataframes to your local Python interpreter - this is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftContext: Daft context after setting the Py runner
    """
    ctx = get_context()
    if ctx.disallow_set_runner:
        raise RuntimeError("Cannot set runner more than once")

    ctx.runner_config = _PyRunnerConfig(use_thread_pool=use_thread_pool)
    ctx.disallow_set_runner = True
    return ctx


def set_config(
    config: PyDaftConfig | None = None,
    merge_scan_tasks_min_size_bytes: int | None = None,
    merge_scan_tasks_max_size_bytes: int | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution

    Args:
        config: A PyDaftConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        merge_scan_tasks_min_size_bytes: Minimum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will make Daft perform more merging of files into a single partition before yielding,
            which leads to bigger but fewer partitions. (Defaults to 64MB)
        merge_scan_tasks_max_size_bytes: Maximum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will increase the upper bound of the size of merged ScanTasks, which leads to bigger but
            fewer partitions. (Defaults to 512MB)
    """
    ctx = get_context()
    if ctx.disallow_set_runner:
        raise RuntimeError(
            "Cannot call `set_config` after the runner has already been created. "
            "Please call `set_config` before any calls to set the runner and before any dataframe creation or execution."
        )

    # Replace values in the DaftConfig with user-specified overrides
    old_daft_config = ctx.daft_config if config is None else config
    new_daft_config = old_daft_config.with_config_values(
        merge_scan_tasks_min_size_bytes=merge_scan_tasks_min_size_bytes,
        merge_scan_tasks_max_size_bytes=merge_scan_tasks_max_size_bytes,
    )

    ctx.daft_config = new_daft_config
    return ctx
