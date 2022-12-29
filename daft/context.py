from __future__ import annotations

import dataclasses
import os
import pathlib
import tempfile
from typing import TYPE_CHECKING, ClassVar

from loguru import logger

if TYPE_CHECKING:
    from daft.runners.runner import Runner


DEFAULT_DAFT_CACHE_LOCATION = pathlib.Path(tempfile.gettempdir()) / "daft"


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


def _get_cache_location_from_env() -> pathlib.Path | None:
    envvar = os.getenv("DAFT_CACHE")
    if envvar is None:
        return DEFAULT_DAFT_CACHE_LOCATION
    elif envvar == "":
        return None
    return pathlib.Path(envvar)


# Global Runner singleton, initialized when accessed through the DaftContext
_RUNNER: Runner | None = None


@dataclasses.dataclass(frozen=True)
class DaftContext:
    """Global context for the current Daft execution environment"""

    runner_config: _RunnerConfig = dataclasses.field(default_factory=_get_runner_config_from_env)
    disallow_set_runner: bool = False
    cache_location: pathlib.Path | None = dataclasses.field(default_factory=_get_cache_location_from_env)

    def runner(self) -> Runner:
        global _RUNNER
        if _RUNNER is not None:
            return _RUNNER
        if self.runner_config.name == "ray":
            from daft.runners.ray_runner import RayRunner

            logger.info("Using RayRunner")
            assert isinstance(self.runner_config, _RayRunnerConfig)
            _RUNNER = RayRunner(address=self.runner_config.address)
        elif self.runner_config.name == "py":
            from daft.runners.pyrunner import PyRunner

            logger.info("Using PyRunner")
            _RUNNER = PyRunner()

        elif self.runner_config.name == "dynamic":
            from daft.runners.dynamic_runner import DynamicRunner

            logger.info("Using DynamicRunner")
            _RUNNER = DynamicRunner()

        elif self.runner_config.name == "dynamicray":
            from daft.runners.ray_runner import DynamicRayRunner

            logger.info("Using DynamicRayRunner")
            assert isinstance(self.runner_config, _DynamicRayRunnerConfig)
            _RUNNER = DynamicRayRunner(address=self.runner_config.address)
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


_DaftContext = DaftContext()


def get_context() -> DaftContext:
    return _DaftContext


def set_daft_cache(cache_location: pathlib.Path | None) -> DaftContext:
    """Sets the location of the Daft cache

    By default Daft will use /tmp/daft as the cache location, but it may be useful for applications
    to set a better location for example to take advantage of SSD scratch space.

    Args:
        cache_location (Optional[pathlib.Path]): Location for caching Daft data

    Returns:
        DaftContext: Daft context after setting the cache location
    """
    global _DaftContext
    _DaftContext = dataclasses.replace(
        _DaftContext,
        cache_location=cache_location,
    )
    return _DaftContext


def set_runner_ray(address: str | None = None) -> DaftContext:
    """Sets the runner for executing Daft dataframes to the RayRunner

    Alternatively, users can set this behavior via environment variables:

    1. DAFT_RUNNER=ray
    2. Optionally, DAFT_RAY_ADDRESS=ray://...

    Args:
        address: Address to head node of the Ray cluster. Defaults to None.

    Returns:
        DaftContext: Daft context after setting the Ray runner
    """
    global _DaftContext
    if _DaftContext.disallow_set_runner:
        raise RuntimeError("Cannot set runner more than once")
    _DaftContext = dataclasses.replace(
        _DaftContext,
        runner_config=_RayRunnerConfig(address=address),
        disallow_set_runner=True,
    )
    return _DaftContext


def set_runner_dynamic_ray(address: str | None = None) -> DaftContext:
    global _DaftContext
    if _DaftContext.disallow_set_runner:
        raise RuntimeError("Cannot set runner more than once")
    _DaftContext = dataclasses.replace(
        _DaftContext,
        runner_config=_DynamicRayRunnerConfig(address=address),
        disallow_set_runner=True,
    )
    return _DaftContext


def set_runner_py() -> DaftContext:
    """Sets the runner for executing Daft dataframes to the PyRunner. This is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftContext: Daft context after setting the Py runner
    """
    global _DaftContext
    if _DaftContext.disallow_set_runner:
        raise RuntimeError("Cannot set runner more than once")
    _DaftContext = dataclasses.replace(
        _DaftContext,
        runner_config=_PyRunnerConfig(),
        disallow_set_runner=True,
    )
    return _DaftContext


def set_runner_dynamic() -> DaftContext:
    global _DaftContext
    if _DaftContext.disallow_set_runner:
        raise RuntimeError("Cannot set runner more than once")
    _DaftContext = dataclasses.replace(
        _DaftContext,
        runner_config=_DynamicRunnerConfig(),
        disallow_set_runner=True,
    )
    return _DaftContext
