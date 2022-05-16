import ray

from typing import Any, Dict, Optional

from daft.config import DaftSettings

class DaftContext:
    def __init__(
        self,
        num_cpus: Optional[int] = DaftSettings.NUM_CPUS,
        ray_address: Optional[str] = DaftSettings.RAY_ADDRESS,
        runtime_env: Optional[Dict[str, Any]] = None,
    ):
        self._ray_context = ray.init(
            num_cpus=num_cpus,
            address=ray_address,
            runtime_env=runtime_env,
        )

_DEFAULT_CONTEXT: Optional[DaftContext] = None

def init(runtime_env: Optional[Dict[str, Any]] = None) -> DaftContext:
    global _DEFAULT_CONTEXT
    if _DEFAULT_CONTEXT is None:
        _DEFAULT_CONTEXT = DaftContext(runtime_env=runtime_env)
    return _DEFAULT_CONTEXT
