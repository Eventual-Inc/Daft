import dataclasses
import os
from typing import ClassVar, Optional


class _RunnerConfig:
    name = ClassVar[str]


@dataclasses.dataclass(frozen=True)
class _PyRunnerConfig(_RunnerConfig):
    name = "py"


@dataclasses.dataclass(frozen=True)
class _RayRunnerConfig(_RunnerConfig):
    name = "ray"
    address: Optional[str]


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
        raise ValueError(f"Unsupported DAFT_RUNNER variable: {os.environ['DAFT_RUNNER']}")
    return _PyRunnerConfig()


@dataclasses.dataclass(frozen=True)
class DaftSettings:

    runner_config: _RunnerConfig = dataclasses.field(default_factory=_get_runner_config_from_env)
    runner_set_called: bool = False

    def ray_runner_config(self) -> _RayRunnerConfig:
        assert isinstance(self.runner_config, _RayRunnerConfig)
        return self.runner_config


_GlobalDaftSettings = DaftSettings()


def get_daft_settings() -> DaftSettings:
    return _GlobalDaftSettings


def set_runner_ray(address: Optional[str] = None) -> DaftSettings:
    """Sets the runner for executing Daft dataframes to the RayRunner

    Alternatively, users can set this behavior via environment variables:

    1. DAFT_RUNNER=ray
    2. Optionally, DAFT_RAY_ADDRESS=ray://...

    Args:
        address: Address to head node of the Ray cluster. Defaults to None.

    Returns:
        DaftSettings: Daft settings after setting the Ray runner
    """
    global _GlobalDaftSettings
    if _GlobalDaftSettings.runner_set_called:
        raise RuntimeError("Cannot set runner more than once")

    runner_config = _RayRunnerConfig(address=address)
    _GlobalDaftSettings = dataclasses.replace(
        _GlobalDaftSettings,
        runner_config=runner_config,
        runner_set_called=True,
    )
    return _GlobalDaftSettings


def set_runner_py() -> DaftSettings:
    """Sets the runner for executing Daft dataframes to the PyRunner. This is the default behavior.

    Alternatively, users can set this behavior via an environment variable: DAFT_RUNNER=py

    Returns:
        DaftSettings: Daft settings after setting the Py runner
    """
    global _GlobalDaftSettings
    if _GlobalDaftSettings.runner_set_called:
        raise RuntimeError("Cannot set runner more than once")

    runner_config = _PyRunnerConfig()
    _GlobalDaftSettings = dataclasses.replace(
        _GlobalDaftSettings,
        runner_config=runner_config,
        runner_set_called=True,
    )
    return _GlobalDaftSettings
