from __future__ import annotations

import dataclasses
import functools


@dataclasses.dataclass(frozen=True)
class ResourceRequest:

    num_cpus: int | float | None = None
    num_gpus: int | float | None = None
    memory_bytes: int | float | None = None

    @staticmethod
    def max_resources(resource_requests: list[ResourceRequest | None]) -> ResourceRequest:
        """Gets the maximum of all resources in a list of ResourceRequests as a new ResourceRequest"""
        return functools.reduce(
            lambda acc, req: acc._max_for_each_resource(req),
            resource_requests,
            ResourceRequest(num_cpus=None, num_gpus=None, memory_bytes=None),
        )

    def _max_for_each_resource(self, other: ResourceRequest | None) -> ResourceRequest:
        """Get a new ResourceRequest that consists of the maximum requests for each resource"""
        if other is None:
            return self
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
