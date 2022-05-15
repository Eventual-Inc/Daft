import ray

from typing import Optional

from daft.config import DaftSettings

class DaftContext:
    def __init__(
        self,
        num_cpus: int = DaftSettings.NUM_CPUS,
        ray_address: Optional[str] = DaftSettings.RAY_ADDRESS,
    ):
        self._ray_context = ray.init(
            num_cpus=num_cpus,
            address=ray_address,
        )

_DEFAULT_CONTEXT: Optional[DaftContext] = None

def init() -> DaftContext:
    global _DEFAULT_CONTEXT
    if _DEFAULT_CONTEXT is None:
        _DEFAULT_CONTEXT = DaftContext()
    return _DEFAULT_CONTEXT
