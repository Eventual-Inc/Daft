from __future__ import annotations

import dataclasses
import os
import warnings
from typing import TYPE_CHECKING, ClassVar

from loguru import logger

if TYPE_CHECKING:
    from daft.runners.runner import Runner


class _RunnerConfig:
    name = ClassVar[str]


@dataclasses.dataclass(frozen=True)
class _PyRunnerConfig(_RunnerConfig):
    name = "py"


@dataclasses.dataclass(frozen=True)
class _DynamicRunnerConfig(_RunnerConfig):
    name = "dynamic"


@dataclasses.dataclass(frozen=True)
class _RayRunnerConfig(_RunnerConfig):
    name = "ray"
    address: str | None


@dataclasses.dataclass(frozen=True)
class _DynamicRayRunnerConfig(_RunnerConfig):
    name = "dynamicray"
    address: str | None


def _get_runner_config_from_env() -> _RunnerConfig:
    """Retrieves the appropriate RunnerConfig from environment variables

    To use:

    1. PyRunner: set DAFT_RUNNER=py
    2. RayRunner: set DAFT_RUNNER=ray and optionally DAFT_RAY_ADDRESS=ray://...
    """
    if "DAFT_RUNNER" in os.environ:
        runner = os.environ["DAFT_RUNNER"]
        if runner.upper() == "RAY":
            return _RayRunnerConfig(address=os.getenv("DAFT_RAY_ADDRESS"))
        elif runner.upper() == "PY":
            return _PyRunnerConfig()
        elif runner.upper() == "DYNAMIC":
            return _DynamicRunnerConfig()
        elif runner.upper() == "DYNAMICRAY":
            return _DynamicRayRunnerConfig(address=os.getenv("DAFT_RAY_ADDRESS"))
        raise ValueError(f"Unsupported DAFT_RUNNER variable: {os.environ['DAFT_RUNNER']}")
    return _PyRunnerConfig()


@dataclasses.dataclass()
class DaftContext:
    """Global context for the current Daft execution environment"""

    _runner: Runner | None
    runner_config: _RunnerConfig

    def runner(self) -> Runner:
        if self._runner is not None:
            return self._runner
        if self.runner_config.name == "ray":
            from daft.runners.ray_runner import RayRunner

            logger.info("Using RayRunner")
            assert isinstance(self.runner_config, _RayRunnerConfig)
            self._runner = RayRunner(address=self.runner_config.address)
        elif self.runner_config.name == "py":
            from daft.runners.pyrunner import PyRunner

            logger.info("Using PyRunner")
            self._runner = PyRunner()

        elif self.runner_config.name == "dynamic":
            from daft.runners.dynamic_runner import DynamicRunner

            logger.info("Using DynamicRunner")
            self._runner = DynamicRunner()

        elif self.runner_config.name == "dynamicray":
            from daft.runners.ray_runner import DynamicRayRunner

            logger.info("Using DynamicRayRunner")
            assert isinstance(self.runner_config, _DynamicRayRunnerConfig)
            self._runner = DynamicRayRunner(address=self.runner_config.address)
        else:
            raise NotImplementedError(f"Runner config implemented: {self.runner_config.name}")

        return self._runner

    def reset_runner(self) -> None:
        del self._runner
        self._runner = None


_DaftContext = DaftContext(_runner=None, runner_config=_get_runner_config_from_env())


def get_context() -> DaftContext:
    return _DaftContext


def _set_runner(config: _RunnerConfig):
    if _DaftContext._runner is not None:
        warnings.warn(
            f"The current active runner {_DaftContext._runner} with config {_DaftContext.runner_config} will be replaced."
            " It is not recommended to set the runner more than once in a single Python session."
        )
    _DaftContext.runner_config = config
    _DaftContext.reset_runner()


def set_runner_ray(address: str | None = None) -> DaftContext:
    """Set the runner for executing Daft dataframes to a Ray cluster

    Alternatively, users can set this behavior via environment variables:

    1. DAFT_RUNNER=ray
    2. Optionally, DAFT_RAY_ADDRESS=ray://...

    Args:
        address: Address to head node of the Ray cluster. Defaults to None.

    Returns:
        DaftContext: Daft context after setting the Ray runner
    """
    _set_runner(_RayRunnerConfig(address=address))
    return _DaftContext


def set_runner_dynamic_ray(address: str | None = None) -> DaftContext:
    """[Experimental] Sets the runner for executing Daft dataframes to the DynamicRayRunner."""
    _set_runner(_DynamicRayRunnerConfig(address=address))
    return _DaftContext


def set_runner_py() -> DaftContext:
    """Set the runner for executing Daft dataframes to your local Python interpreter - this is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftContext: Daft context after setting the Py runner
    """
    _set_runner(_PyRunnerConfig())
    return _DaftContext


def set_runner_dynamic() -> DaftContext:
    """[Experimental] Sets the runner for executing Daft dataframes to the DynamicRunner."""
    _set_runner(_DynamicRunnerConfig())
    return _DaftContext
