from typing import Any, Dict, Optional

import ray

from daft import config


class _DaftContext:
    def __init__(
        self,
        ray_address: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self._ray_context = ray.init(
            address=ray_address,
            runtime_env=runtime_env,
            **kwargs,
        )


_DEFAULT_CONTEXT: Optional[_DaftContext] = None


def init(
    ray_address: Optional[str] = None,
    runtime_env: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> _DaftContext:
    """Inititialize the Daft Context

    Args:
        ray_address (Optional[str], optional): The address of the ray cluster to connect to. If not provided, Daft
            will attempt to detect the address from the `DAFT_CLUSTER_HEAD_ADDR` environment variable or default
            to creating a local cluster if not found.
        runtime_env (Optional[Dict[str, Any]], optional): Ray-compatible runtime env. If not provided, Daft
            will attempt to construct one using the `DAFT_WORKDIR` and `DAFT_REQUIREMENTS_TXT` environment
            variables, or default to None if not found.

    Returns:
        _DaftContext: context object
    """
    global _DEFAULT_CONTEXT
    daft_settings = config.DaftSettings()
    if _DEFAULT_CONTEXT is None:

        if ray_address is None:
            ray_address = daft_settings.DAFT_CLUSTER_HEAD_ADDR
        if runtime_env is None:
            if daft_settings.DAFT_REQUIREMENTS_TXT and daft_settings.DAFT_WORKDIR:
                runtime_env = {
                    "working_dir": daft_settings.DAFT_WORKDIR,
                    "pip": daft_settings.DAFT_REQUIREMENTS_TXT,
                    "excludes": [".cache/**"],
                }

        _DEFAULT_CONTEXT = _DaftContext(
            ray_address=ray_address,
            runtime_env=runtime_env,
            **kwargs,
        )
    return _DEFAULT_CONTEXT
