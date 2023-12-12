from __future__ import annotations

import dataclasses
import logging
import os
import warnings
from typing import TYPE_CHECKING, ClassVar

from daft.daft import IOConfig, PyDaftExecutionConfig, PyDaftPlanningConfig

if TYPE_CHECKING:
    from daft.runners.runner import Runner

logger = logging.getLogger(__name__)


class _RunnerConfig:
    name = ClassVar[str]


@dataclasses.dataclass(frozen=True)
class _PyRunnerConfig(_RunnerConfig):
    name = "py"
    use_thread_pool: bool | None


@dataclasses.dataclass(frozen=True)
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

    # When a dataframe is executed, this config is copied into the Runner
    # which then keeps track of a per-unique-execution-ID copy of the config, using it consistently throughout the execution
    daft_execution_config: PyDaftExecutionConfig = PyDaftExecutionConfig()

    # Non-execution calls (e.g. creation of a dataframe, logical plan building etc) directly reference values in this config
    daft_planning_config: PyDaftPlanningConfig = PyDaftPlanningConfig()

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
            self._runner = PyRunner(use_thread_pool=self.runner_config.use_thread_pool)

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


def set_planning_config(
    config: PyDaftPlanningConfig | None = None,
    default_io_config: IOConfig | None = None,
) -> DaftContext:
    """Globally sets varioous configuration parameters which control Daft plan construction behavior. These configuration values
    are used when a Dataframe is being constructed (e.g. calls to create a Dataframe, or to build on an existing Dataframe)

    Args:
        config: A PyDaftPlanningConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        default_io_config: A default IOConfig to use in the absence of one being explicitly passed into any Expression (e.g. `.url.download()`)
            or Dataframe operation (e.g. `daft.read_parquet()`).
    """
    # Replace values in the DaftPlanningConfig with user-specified overrides
    ctx = get_context()
    old_daft_planning_config = ctx.daft_planning_config if config is None else config
    new_daft_planning_config = old_daft_planning_config.with_config_values(
        default_io_config=default_io_config,
    )

    ctx.daft_planning_config = new_daft_planning_config
    return ctx


def set_execution_config(
    config: PyDaftExecutionConfig | None = None,
    merge_scan_tasks_min_size_bytes: int | None = None,
    merge_scan_tasks_max_size_bytes: int | None = None,
    broadcast_join_size_bytes_threshold: int | None = None,
) -> DaftContext:
    """Globally sets various configuration parameters which control various aspects of Daft execution. These configuration values
    are used when a Dataframe is executed (e.g. calls to `.write_*`, `.collect()` or `.show()`)

    Args:
        config: A PyDaftExecutionConfig object to set the config to, before applying other kwargs. Defaults to None which indicates
            that the old (current) config should be used.
        merge_scan_tasks_min_size_bytes: Minimum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will make Daft perform more merging of files into a single partition before yielding,
            which leads to bigger but fewer partitions. (Defaults to 64 MiB)
        merge_scan_tasks_max_size_bytes: Maximum size in bytes when merging ScanTasks when reading files from storage.
            Increasing this value will increase the upper bound of the size of merged ScanTasks, which leads to bigger but
            fewer partitions. (Defaults to 512 MiB)
        broadcast_join_size_bytes_threshold: If one side of a join is smaller than this threshold, a broadcast join will be used.
            Default is 10 MiB.
    """
    # Replace values in the DaftExecutionConfig with user-specified overrides
    ctx = get_context()
    old_daft_execution_config = ctx.daft_execution_config if config is None else config
    new_daft_execution_config = old_daft_execution_config.with_config_values(
        merge_scan_tasks_min_size_bytes=merge_scan_tasks_min_size_bytes,
        merge_scan_tasks_max_size_bytes=merge_scan_tasks_max_size_bytes,
        broadcast_join_size_bytes_threshold=broadcast_join_size_bytes_threshold,
    )

    ctx.daft_execution_config = new_daft_execution_config
    return ctx
