from __future__ import annotations

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
