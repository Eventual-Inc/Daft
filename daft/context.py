import ray

from typing import Optional

class DaftContext:
    def __init__(self, num_cpus: int = 8):
        self._ray_context = ray.init(num_cpus=num_cpus)

_DEFAULT_CONTEXT: Optional[DaftContext] = None

def init() -> DaftContext:
    global _DEFAULT_CONTEXT
    if _DEFAULT_CONTEXT is None:
        _DEFAULT_CONTEXT = DaftContext()
    return _DEFAULT_CONTEXT
