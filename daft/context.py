from __future__ import annotations

import dataclasses
import os
import warnings
from typing import TYPE_CHECKING, ClassVar

from loguru import logger

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.runner import Runner


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


# Global Runner singleton, initialized when accessed through the DaftContext
_RUNNER: Runner | None = None


def _get_planner_from_env() -> bool:
    """Returns whether or not to use the new query planner."""
    return bool(int(os.getenv("DAFT_NEW_QUERY_PLANNER", default="1")))


@dataclasses.dataclass(frozen=True)
class DaftContext:
    """Global context for the current Daft execution environment"""

    runner_config: _RunnerConfig = dataclasses.field(default_factory=_get_runner_config_from_env)
    disallow_set_runner: bool = False
    use_rust_planner: bool = dataclasses.field(default_factory=_get_planner_from_env)

    def runner(self) -> Runner:
        global _RUNNER
        if _RUNNER is not None:
            return _RUNNER
        if self.runner_config.name == "ray":
            from daft.runners.ray_runner import RayRunner

            logger.info("Using RayRunner")
            assert isinstance(self.runner_config, _RayRunnerConfig)
            _RUNNER = RayRunner(
                address=self.runner_config.address,
                max_task_backlog=self.runner_config.max_task_backlog,
            )
        elif self.runner_config.name == "py":
            from daft.runners.pyrunner import PyRunner

            logger.info("Using PyRunner")
            assert isinstance(self.runner_config, _PyRunnerConfig)
            _RUNNER = PyRunner(use_thread_pool=self.runner_config.use_thread_pool)

        else:
            raise NotImplementedError(f"Runner config implemented: {self.runner_config.name}")

        # Mark DaftContext as having the runner set, which prevents any subsequent setting of the config
        # after the runner has been initialized once
        global _DaftContext
        _DaftContext = dataclasses.replace(
            _DaftContext,
            disallow_set_runner=True,
        )

        return _RUNNER

    @property
    def is_ray_runner(self) -> bool:
        return isinstance(self.runner_config, _RayRunnerConfig)

    def logical_plan_builder_class(self) -> type[LogicalPlanBuilder]:
        from daft.logical.logical_plan import PyLogicalPlanBuilder
        from daft.logical.rust_logical_plan import RustLogicalPlanBuilder

        if self.use_rust_planner:
            return RustLogicalPlanBuilder
        else:
            return PyLogicalPlanBuilder


_DaftContext = DaftContext()


def get_context() -> DaftContext:
    return _DaftContext


def _set_context(ctx: DaftContext):
    global _DaftContext

    _DaftContext = ctx


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
    old_ctx = get_context()
    if old_ctx.disallow_set_runner:
        if noop_if_initialized:
            warnings.warn(
                "Calling daft.context.set_runner_ray(noop_if_initialized=True) multiple times has no effect beyond the first call."
            )
            return old_ctx
        raise RuntimeError("Cannot set runner more than once")
    new_ctx = dataclasses.replace(
        old_ctx,
        runner_config=_RayRunnerConfig(
            address=address,
            max_task_backlog=max_task_backlog,
        ),
        disallow_set_runner=True,
    )
    _set_context(new_ctx)
    return new_ctx


def set_runner_py(use_thread_pool: bool | None = None) -> DaftContext:
    """Set the runner for executing Daft dataframes to your local Python interpreter - this is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftContext: Daft context after setting the Py runner
    """
    old_ctx = get_context()
    if old_ctx.disallow_set_runner:
        raise RuntimeError("Cannot set runner more than once")
    new_ctx = dataclasses.replace(
        old_ctx,
        runner_config=_PyRunnerConfig(use_thread_pool=use_thread_pool),
        disallow_set_runner=True,
    )
    _set_context(new_ctx)
    return new_ctx


def set_new_planner() -> DaftContext:
    """Enable the new query planner.

    WARNING: The new query planner is currently experimental and only partially implemented.

    Alternatively, users can set this behavior via an environment variable: DAFT_NEW_QUERY_PLANNER=1

    Returns:
        DaftContext: Daft context after enabling the new query planner.
    """
    old_ctx = get_context()
    new_ctx = dataclasses.replace(
        old_ctx,
        use_rust_planner=True,
    )
    _set_context(new_ctx)
    return new_ctx


def set_old_planner() -> DaftContext:
    """Enable the old query planner.

    Alternatively, users can set this behavior via an environment variable: DAFT_NEW_QUERY_PLANNER=0

    Returns:
        DaftContext: Daft context after enabling the old query planner.
    """
    old_ctx = get_context()
    new_ctx = dataclasses.replace(
        old_ctx,
        use_rust_planner=False,
    )
    _set_context(new_ctx)
    return new_ctx
