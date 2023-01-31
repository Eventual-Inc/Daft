from __future__ import annotations

import contextlib
import dataclasses
import functools


@dataclasses.dataclass(frozen=True)
class ResourceRequest:

    num_cpus: int | float | None
    num_gpus: int | float | None
    memory_bytes: int | float | None

    @classmethod
    def default(cls) -> ResourceRequest:
        return ResourceRequest(num_cpus=None, num_gpus=None, memory_bytes=None)

    @staticmethod
    def max_resources(resource_requests: list[ResourceRequest]) -> ResourceRequest:
        """Gets the maximum of all resources in a list of ResourceRequests, including self, as a new ResourceRequest"""
        return functools.reduce(
            lambda acc, req: acc._max_for_each_resource(req),
            resource_requests,
            ResourceRequest(num_cpus=None, num_gpus=None, memory_bytes=None),
        )

    def _max_for_each_resource(self, other: ResourceRequest) -> ResourceRequest:
        """Get a new ResourceRequest that consists of the maximum requests for each resource"""
        resource_names = [f.name for f in dataclasses.fields(ResourceRequest)]
        max_resources = {}
        for name in resource_names:
            if getattr(self, name) is None:
                max_resources[name] = getattr(other, name)
            elif getattr(other, name) is None:
                max_resources[name] = getattr(self, name)
            else:
                max_resources[name] = max(getattr(self, name), getattr(other, name))
        return ResourceRequest(**max_resources)


# Global singleton tracking the current context's resource request
_CURRENT_RESOURCE_REQUEST = ResourceRequest.default()


def _get_current_resource_request() -> ResourceRequest:
    """Gets the current context's resource request"""
    return _CURRENT_RESOURCE_REQUEST


@contextlib.contextmanager
def resources(num_cpus: int | float = 1.0, num_gpus: int | float = 0, memory_bytes: int | None = None):
    """Sets the resources required to execute the Daft dataframe operations that it wraps

    Example:

        >>> with daft.resources(num_cpus=2, num_gpus=1):
        >>>     df = df.with_column("model_results", run_model(df["features"]))

    Args:
        num_cpus: Number of CPUs required to execute the Daft dataframe operations that it wraps
        num_gpus: Number of GPUs required to execute the Daft dataframe operations that it wraps
        memory_bytes: Number of bytes of memory required to execute the Daft dataframe operations that it wraps
    """
    global _CURRENT_RESOURCE_REQUEST
    old_resource_request = _CURRENT_RESOURCE_REQUEST
    new_resource_request = ResourceRequest(
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory_bytes=memory_bytes,
    )
    try:
        _CURRENT_RESOURCE_REQUEST = new_resource_request
    finally:
        _CURRENT_RESOURCE_REQUEST = old_resource_request
