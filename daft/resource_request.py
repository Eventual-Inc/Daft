from __future__ import annotations

import dataclasses
import functools


@dataclasses.dataclass(frozen=True)
class ResourceRequest:

    num_cpus: float
    num_gpus: float

    @classmethod
    def default(cls) -> ResourceRequest:
        """Returns the default ResourceRequest"""
        return ResourceRequest(
            num_cpus=1.0,
            num_gpus=0.0,
        )

    def max_resources(self, *resource_requests: ResourceRequest) -> ResourceRequest:
        """Gets the maximum of all resources in a list of ResourceRequests, including self, as a new ResourceRequest"""
        return functools.reduce(
            lambda acc, req: acc._max_for_each_resource(req),
            resource_requests,
            self,
        )

    def _max_for_each_resource(self, other: ResourceRequest) -> ResourceRequest:
        """Get a new ResourceRequest that consists of the maximum requests for each resource"""
        resource_names = [f.name for f in dataclasses.fields(ResourceRequest)]
        max_resources = {name: max(getattr(self, name), getattr(other, name)) for name in resource_names}
        return ResourceRequest(**max_resources)
