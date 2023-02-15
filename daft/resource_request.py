from __future__ import annotations

import dataclasses
import functools


@dataclasses.dataclass(frozen=True)
class ResourceRequest:

    num_cpus: int | float | None = None
    num_gpus: int | float | None = None
    memory_bytes: int | float | None = None

    @staticmethod
    def max_resources(resource_requests: list[ResourceRequest]) -> ResourceRequest:
        """Gets the maximum of all resources in a list of ResourceRequests as a new ResourceRequest"""
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

    def __add__(self, other: ResourceRequest) -> ResourceRequest:
        return ResourceRequest(
            num_cpus=add_optional_numeric(self.num_cpus, other.num_cpus),
            num_gpus=add_optional_numeric(self.num_gpus, other.num_gpus),
            memory_bytes=add_optional_numeric(self.memory_bytes, other.memory_bytes),
        )


def add_optional_numeric(a: int | float | None, b: int | float | None) -> int | float | None:
    """
    Add a and b together, treating None as 0.
    If a and b are both None, then returns None (i.e. preserves None if all inputs are None).
    """
    if a is None and b is None:
        return None

    return (a or 0) + (b or 0)
