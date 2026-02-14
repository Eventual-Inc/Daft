from __future__ import annotations

from typing import Any


def is_complex_ray_options(ray_options: dict[str, Any] | None) -> bool:
    """Check if ray_options contains complex configurations that require an actor pool.

    Standard resource requests (num_cpus, num_gpus, memory) can be handled by regular tasks
    in some contexts, but complex options like runtime_env, scheduling_strategy, etc.,
    always require dedicated actors in Daft's Flotilla runner.
    """
    if not ray_options:
        return False

    # These are considered "simple" resource-only options.
    # Note: num_cpus and memory might be merged from ResourceRequest.
    simple_keys = {"num_cpus", "num_gpus", "memory"}

    complex_keys = set(ray_options.keys()) - simple_keys
    return len(complex_keys) > 0
