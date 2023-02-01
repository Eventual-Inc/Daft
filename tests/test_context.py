from __future__ import annotations

import daft
from daft.context import (
    _DynamicRayRunnerConfig,
    _get_runner_config_from_env,
    _RayRunnerConfig,
)


def test_reset_runner():
    """Test resetting the runner"""
    config = _get_runner_config_from_env()
    if config.name == "py":
        daft.context.set_runner_py()
    elif config.name == "ray":
        assert isinstance(config, _RayRunnerConfig)
        daft.context.set_runner_ray(address=config.address)
    elif config.name == "dynamic":
        daft.context.set_runner_dynamic()
    elif config.name == "dynamicray":
        assert isinstance(config, _DynamicRayRunnerConfig)
        daft.context.set_runner_dynamic_ray(address=config.address)
    else:
        raise NotImplementedError(f"Test not implemented for runner: {config.name}")

    # Run some daft code to trigger runner creation
    df = daft.DataFrame.from_pydict({"a": [1, 2, 3]})
    df.collect()
