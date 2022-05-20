from typing import Any, Dict, Optional

import ray

from daft import config


class DaftContext:
    def __init__(
        self,
        ray_address: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
    ):
        self._ray_context = ray.init(
            address=ray_address,
            runtime_env=runtime_env,
        )


_DEFAULT_CONTEXT: Optional[DaftContext] = None


def init(
    ray_address: Optional[str] = None,
    runtime_env: Optional[Dict[str, Any]] = None,
) -> DaftContext:
    """Inititialize the Daft Context

    Args:
        ray_address (Optional[str], optional): The address of the ray cluster to connect to. If not provided, Daft
            will attempt to detect the address from the `DAFT_CLUSTER_HEAD_ADDR` environment variable or default
            to creating a local cluster if not found.
        runtime_env (Optional[Dict[str, Any]], optional): Ray-compatible runtime env. Defaults to None.

    Returns:
        DaftContext: context object
    """
    global _DEFAULT_CONTEXT
    daft_settings = config.DaftSettings()
    if _DEFAULT_CONTEXT is None:
        if ray_address is None:
            ray_address = daft_settings.DAFT_CLUSTER_HEAD_ADDR
        _DEFAULT_CONTEXT = DaftContext(ray_address=ray_address, runtime_env=runtime_env)
    return _DEFAULT_CONTEXT
