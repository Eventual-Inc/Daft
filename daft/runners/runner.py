from __future__ import annotations

import contextlib
from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, Iterator

from daft.runners.partitioning import (
    MaterializedResult,
    PartitionCacheEntry,
    PartitionSet,
    PartitionSetCache,
    PartitionT,
)

if TYPE_CHECKING:
    from daft.daft import ResourceRequest
    from daft.expressions import ExpressionsProjection
    from daft.logical.builder import LogicalPlanBuilder
    from daft.runners.runner_io import RunnerIO
    from daft.table import MicroPartition


class Runner(Generic[PartitionT]):
    def __init__(self) -> None:
        self._part_set_cache = PartitionSetCache()

    def get_partition_set_from_cache(self, pset_id: str) -> PartitionCacheEntry:
        return self._part_set_cache.get_partition_set(pset_id=pset_id)

    def put_partition_set_into_cache(self, pset: PartitionSet) -> PartitionCacheEntry:
        return self._part_set_cache.put_partition_set(pset=pset)

    @abstractmethod
    def runner_io(self) -> RunnerIO: ...

    @abstractmethod
    def run(self, builder: LogicalPlanBuilder) -> PartitionCacheEntry: ...

    @abstractmethod
    def run_iter(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MaterializedResult[PartitionT]]:
        """Similar to run(), but yield the individual partitions as they are completed.

        Args:
            builder: the builder for the LogicalPlan that is to be executed
            results_buffer_size: if the plan is executed asynchronously, this is the maximum size of the number of results
                that can be buffered before execution should pause and wait.
        """
        ...

    @abstractmethod
    def run_iter_tables(
        self, builder: LogicalPlanBuilder, results_buffer_size: int | None = None
    ) -> Iterator[MicroPartition]:
        """Similar to run_iter(), but always dereference and yield MicroPartition objects.

        Args:
            builder: the builder for the LogicalPlan that is to be executed
            results_buffer_size: if the plan is executed asynchronously, this is the maximum size of the number of results
                that can be buffered before execution should pause and wait.
        """
        ...

    @abstractmethod
    @contextlib.contextmanager
    def actor_pool_context(
        self,
        name: str,
        actor_resource_request: ResourceRequest,
        task_resource_request: ResourceRequest,
        num_actors: int,
        projection: ExpressionsProjection,
    ) -> Iterator[str]:
        """Creates a pool of actors which can execute work, and yield a context in which the pool can be used.

        Also yields a `str` ID which clients can use to refer to the actor pool when submitting tasks.

        Note that attempting to do work outside this context will result in errors!

        Args:
            name: Name of the actor pool for debugging/observability
            resource_request: Requested amount of resources for each actor
            num_actors: Number of actors to spin up
            projection: Projection to be run on the incoming data (contains Stateful UDFs as well as other stateless expressions such as aliases)
        """
        ...
