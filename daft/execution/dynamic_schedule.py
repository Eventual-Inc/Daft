from __future__ import annotations

import math
from abc import abstractmethod
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Callable, Generic, TypeVar

from daft.execution import dynamic_construction
from daft.execution.dynamic_construction import (
    BaseConstruction,
    OpenConstruction,
    ExecutionRequest,
    ExecutionResult,
    FanoutInstruction,
    Instruction,
    PartitionWithInfo,
    ReduceInstruction,
)
from daft.logical import logical_plan
from daft.runners.partitioning import PartID, PartitionSet

PartitionT = TypeVar("PartitionT")
_PartitionT = TypeVar("_PartitionT")


class DynamicSchedule(Iterator, Generic[PartitionT]):
    """Recursively dynamically generate a sequence of execution steps to compute some target.

    - The execution steps (Constructions) are exposed via the iterator interface.
    - If the Construction is marked for materialization, it should be materialized and reported as completed.
    - If None is returned, it means that the next execution step is waiting for some previous Construction to materialize.
    """

    @dataclass
    class Materializations(Generic[_PartitionT]):
        dependencies: list[ExecutionRequest[_PartitionT] | None] = field(default_factory=list)

    def __init__(self) -> None:
        # Materialized partitions that this dynamic schedule needs to do its job.
        # `None` denotes a partition whose materialization is in-progress.
        self._materializations: DynamicSchedule.Materializations = self.Materializations[PartitionT]()
        self._completed = False

    def __next__(self) -> BaseConstruction[PartitionT] | None:
        if self._completed:
            raise StopIteration

        try:
            return self._next_impl()

        except StopIteration:
            self._completed = True
            # Drop references to dependencies.
            self._materializations = self.Materializations[PartitionT]()
            raise

    @abstractmethod
    def _next_impl(self) -> BaseConstruction[PartitionT] | None:
        """
        Raises StopIteration if there are no further instructions to give for this schedule.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        name = self.__class__.__name__
        return f"{name}: self._completed={self._completed}, self._materializations={self._materializations}"


def enumerate_open_constructions(
    schedule: Iterator[None | BaseConstruction[PartitionT]]
) -> Iterator[tuple[int, None | BaseConstruction[PartitionT]]]:
    """Like enumerate() on an iterator, but only counts up if the result is an OpenConstruction.

    Useful for counting the number of OpenConstructions returned by the iterator.
    """
    index = 0
    yield from (
        (((index := index + 1) - 1), item)  # aka (index++, item)
        if isinstance(item, OpenConstruction)
        else (index, item)
        for item in schedule
    )

"""
A "Construction" describes some partition(s) to be built.
A "Schedule" is an iterator of constructions. It gives you the sequence, or schedule, of things to build.
"""

def schedule_partition_read(partitions: Iterator[PartitionT]) -> Iterator[None | BaseConstruction[PartitionT]]:
    yield from (OpenConstruction[PartitionT]([partition]) for partition in partitions)

def schedule_file_read(
    source: Iterator[None | BaseConstruction[PartitionT]],
    scan_node: logical_plan.TabularFilesScan,
) -> Iterator[None | BaseConstruction[PartitionT]]:

    for index, construct in enumerate_open_constructions(source):
        if not isinstance(construct, OpenConstruction):
            yield construct

        elif index < scan_node.num_partitions():
            construct.add_instruction(
                dynamic_construction.ReadFile(partition_id=index, logplan=scan_node)
            )
            yield construct

        else:
            return


def schedule_file_write(
    source: Iterator[None | BaseConstruction[PartitionT]],
    write_node: logical_plan.FileWrite,
) -> Iterator[None | BaseConstruction[PartitionT]]:

    yield from (
        construct.add_instruction(partition_id=index, logplan=write_node)
        if isinstance(construct, OpenConstruction)
        else construct
        for index, construct in enumerate_open_constructions(source)
    )


def schedule_pipeline_instruction(
    source: Iterator[None | BaseConstruction[PartitionT]],
    pipeable_instruction: Instruction,
) -> Iterator[None | BaseConstruction[PartitionT]]:

    yield from (
        construct.add_instruction(pipeable_instruction)
        if isinstance(construct, OpenConstruction)
        else construct
        for construct in source
    )



def schedule_join(
    left_source: Iterator[None | BaseConstruction[PartitionT]],
    right_source: Iterator[None | BaseConstruction[PartitionT]],
    join: logical_plan.Join,
) -> Iterator[None | BaseConstruction[PartitionT]]:

    # We will execute the constructions from the left and right sources to get partitions,
    # and then create new constructions which will each join a left and right partition.
    left_requests: deque[ExecutionRequest] = deque()
    right_requests: deque[ExecutionRequest] = deque()

    while True:
        # Emit join constructions if we have left and right partitions ready.
        while (
            left_ready := len(left_requests) > 0 and left_requests[0].result is not None
        ) and (
            right_ready := len(right_requests) > 0 and right_requests[0].result is not None
        ):
            next_left = left_requests.popleft()
            next_right = right_requests.popleft()
            construct_join = OpenConstruction[PartitionT]([next_left.partition, next_right.partition])
            construct_join.add_instruction(dynamic_construction.Join(join))
            yield construct_join

        # Exhausted all ready inputs; execute a single source construction to get more join inputs.
        # Choose whether to execute from left child or right child (whichever one is more behind),
        if len(left_requests) <= len(right_requests):
            next_source, next_requests = left_source, left_requests
        else:
            next_source, next_requests = right_source, right_requests

            for construct in next_source:
                if not isinstance(construct, OpenConstruction):
                    yield construct
                else:
                    execution_request = construct.as_execution_request()
                    next_requests.append(execution_request)
                    yield execution_request
                    break

        # If there are still no pending join inputs, then we have exhausted our sources as well and are done.
        if len(left_requests) + len(right_requests) == 0:
            return


class ScheduleGlobalLimit(DynamicSchedule[PartitionT]):
    def __init__(self, child_schedule: DynamicSchedule[PartitionT], global_limit: logical_plan.GlobalLimit) -> None:
        super().__init__()
        self._child_schedule = child_schedule
        self._global_limit = global_limit
        self._remaining_limit = self._global_limit._num
        self._continue_from_partition = 0

    def _next_impl(self) -> Construction[PartitionT] | None:

        dependencies = self._materializations.dependencies

        # Evaluate progress so far.
        # Are we done with the limit?
        if self._remaining_limit == 0:

            # We are done with the limit,
            # but the current implementation of LogicalPlan still requires
            # a mandated number of partitions to be returned.
            # Return empty partitions until we have returned enough.
            if not dependencies:
                # We need at least one materialized partition to get the correct schema to return.
                # Pass through and materialize a dependency.
                pass

            elif self._continue_from_partition < self._global_limit.num_partitions():
                if dependencies[0] is None:
                    # We need at least one materialized partition to get the correct schema to return.
                    # Wait for the dependency to materialize.
                    return None

                new_construct = Construction[PartitionT]([dependencies[0].partition])
                new_construct.add_instruction(dynamic_construction.LocalLimit(0))

                self._continue_from_partition += 1
                return new_construct

            else:
                raise StopIteration

        # We're not done; check to see if we can return a global limit partition.
        # Is the next local limit partition materialized?
        # If so, update global limit progress, and return the local limit.
        if self._continue_from_partition < len(dependencies):
            next_partition_info = dependencies[self._continue_from_partition]
            if next_partition_info is not None:
                num_rows = next_partition_info.metadata(next_partition_info.partition).num_rows
                next_limit = min(self._remaining_limit, num_rows)
                self._remaining_limit -= next_limit

                new_construct = Construction[PartitionT]([next_partition_info.partition])
                if next_limit < num_rows:
                    new_construct.add_instruction(dynamic_construction.LocalLimit(next_limit))

                self._continue_from_partition += 1
                return new_construct

        # We cannot return a global limit partition,
        # so return instructions to materialize the next local limit partition.
        construct = None
        try:
            construct = next(self._child_schedule)
        except StopIteration:
            if self._continue_from_partition >= len(dependencies):
                raise

        if construct is None or construct.is_marked_for_materialization():
            return construct

        construct.add_instruction(dynamic_construction.LocalLimit(self._remaining_limit))

        construct.mark_for_materialization(dependencies)
        return construct


class ScheduleCoalesce(DynamicSchedule[PartitionT]):
    def __init__(self, child_schedule: DynamicSchedule[PartitionT], coalesce: logical_plan.Coalesce) -> None:
        super().__init__()
        self._child_schedule = child_schedule

        coalesce_from = coalesce._children()[0].num_partitions()
        coalesce_to = coalesce.num_partitions()
        self._num_emitted = 0

        assert (
            coalesce_to <= coalesce_from
        ), f"Cannot coalesce upwards from {coalesce_from} to {coalesce_to} partitions."

        starts = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(coalesce_to)]
        stops = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(1, coalesce_to + 1)]

        self._coalesce_boundaries = list(zip(starts, stops))

    def _next_impl(self) -> Construction[PartitionT] | None:

        if self._num_emitted == len(self._coalesce_boundaries):
            raise StopIteration

        # See if we can emit a coalesced partition.
        dependencies = self._materializations.dependencies
        next_start, next_stop = self._coalesce_boundaries[self._num_emitted]
        if next_stop <= len(dependencies):
            to_coalesce = dependencies[next_start:next_stop]
            # for mypy
            ready_to_coalesce = [_ for _ in to_coalesce if _ is not None]
            if len(ready_to_coalesce) == len(to_coalesce):
                construct_merge = Construction[PartitionT]([_.partition for _ in ready_to_coalesce])
                construct_merge.add_instruction(dynamic_construction.ReduceMerge())
                self._num_emitted += 1
                return construct_merge

        # We cannot emit a coalesced partition;
        # try materializing more dependencies.
        try:
            construct = next(self._child_schedule)
        except StopIteration:
            construct = None

        if construct is None or construct.is_marked_for_materialization():
            return construct

        construct.mark_for_materialization(dependencies)
        return construct


class ScheduleSort(DynamicSchedule[PartitionT]):
    @dataclass
    class Materializations(DynamicSchedule.Materializations[PartitionT]):
        samples: list[PartitionWithInfo[PartitionT] | None] = field(default_factory=list)
        boundaries: list[PartitionWithInfo[PartitionT] | None] = field(default_factory=list)

    def __init__(self, child_schedule: DynamicSchedule[PartitionT], sort: logical_plan.Sort) -> None:
        super().__init__()
        self._materializations: ScheduleSort.Materializations = self.Materializations[PartitionT]()
        self._sort = sort
        self._child_schedule = child_schedule

        # The final step of the sort.
        self._fanout_reduce: ScheduleFanoutReduce | None = None

    def _next_impl(self) -> Construction[PartitionT] | None:

        # First, materialize the child node.
        try:
            construct = next(self._child_schedule)
            if construct is None or construct.is_marked_for_materialization():
                return construct

            construct.mark_for_materialization(self._materializations.dependencies)

            return construct

        except StopIteration:
            # Child node fully materialized.
            pass

        # Sample partitions to get sort boundaries.
        source_partitions = self._materializations.dependencies
        sample_partitions = self._materializations.samples
        if len(sample_partitions) < len(source_partitions):
            source = source_partitions[len(sample_partitions)]
            if source is None:
                return None
            construct_sample = Construction[PartitionT]([source.partition])
            construct_sample.add_instruction(dynamic_construction.Sample(sort_by=self._sort._sort_by))
            construct_sample.mark_for_materialization(sample_partitions)
            return construct_sample

        # Sample partitions are done, reduce to get quantiles.
        if not len(self._materializations.boundaries):
            finished_samples = [_ for _ in sample_partitions if _ is not None]
            if len(finished_samples) != len(sample_partitions):
                return None

            construct_boundaries = Construction[PartitionT]([_.partition for _ in finished_samples])
            construct_boundaries.add_instruction(
                dynamic_construction.ReduceToQuantiles(
                    num_quantiles=self._sort.num_partitions(),
                    sort_by=self._sort._sort_by,
                    descending=self._sort._descending,
                )
            )
            construct_boundaries.mark_for_materialization(self._materializations.boundaries)
            return construct_boundaries

        [boundaries_partition] = self._materializations.boundaries
        if boundaries_partition is None:
            return None

        # Boundaries are ready; execute fanout-reduce.
        finished_dependencies = [_ for _ in source_partitions if _ is not None]  # for mypy; these are all done
        assert len(finished_dependencies) == len(source_partitions)

        if self._fanout_reduce is None:
            from_partitions = SchedulePartitionRead[PartitionT](finished_dependencies)
            fanout_ins = dynamic_construction.FanoutRange[PartitionT](
                num_outputs=self._sort.num_partitions(),
                sort_by=self._sort._sort_by,
                descending=self._sort._descending,
                boundaries=boundaries_partition.partition,
            )
            reduce_ins = dynamic_construction.ReduceMergeAndSort(
                sort_by=self._sort._sort_by,
                descending=self._sort._descending,
            )
            self._fanout_reduce = ScheduleFanoutReduce[PartitionT](
                child_schedule=from_partitions,
                num_outputs=self._sort.num_partitions(),
                fanout_ins=fanout_ins,
                reduce_ins=reduce_ins,
            )

        return next(self._fanout_reduce)


class ScheduleFanoutReduce(DynamicSchedule[PartitionT]):
    def __init__(
        self,
        child_schedule: DynamicSchedule[PartitionT],
        num_outputs: int,
        fanout_ins: FanoutInstruction,
        reduce_ins: ReduceInstruction,
    ) -> None:
        super().__init__()
        self._child_schedule = child_schedule
        self._num_outputs = num_outputs
        self._fanout_ins = fanout_ins
        self._reduce_ins = reduce_ins

        self._reduces_emitted = 0

    def _next_impl(self) -> Construction[PartitionT] | None:
        # Dispatch shufflemaps.
        try:
            construct = next(self._child_schedule)
            if construct is None or construct.is_marked_for_materialization():
                return construct

            construct.add_instruction(self._fanout_ins)

            construct.mark_for_materialization(self._materializations.dependencies, num_results=self._num_outputs)
            return construct

        except StopIteration:
            # No more shufflemaps to dispatch, continue to reduce.
            pass

        # All shufflemaps have been dispatched; see if we can dispatch the next ("kth") reduce.
        if self._reduces_emitted == self._num_outputs:
            raise StopIteration

        # The dependencies here are the 2d matrix unrolled into a 1d list.
        kth_reduce_dependencies = [
            partition
            for i, partition in enumerate(self._materializations.dependencies)
            if i % self._num_outputs == self._reduces_emitted
        ]
        finished_reduce_dependencies = [_ for _ in kth_reduce_dependencies if _ is not None]
        if len(finished_reduce_dependencies) != len(kth_reduce_dependencies):
            # Need to wait for some shufflemaps to complete.
            return None

        construct_reduce = Construction[PartitionT]([_.partition for _ in finished_reduce_dependencies])
        construct_reduce.add_instruction(self._reduce_ins)
        self._reduces_emitted += 1
        return construct_reduce


class ScheduleMaterialize(DynamicSchedule[PartitionT]):
    """Materializes its dependencies and does nothing else."""

    def __init__(self, child_schedule: DynamicSchedule[PartitionT]) -> None:
        super().__init__()
        self.child_schedule = child_schedule
        self._materializing_results: list[PartitionWithInfo[PartitionT] | None] = []
        self._returned = False

    def _next_impl(self) -> Construction[PartitionT] | None:
        construct = next(self.child_schedule)
        if construct is not None and not construct.is_marked_for_materialization():
            construct.mark_for_materialization(self._materializing_results)
        return construct

    def result_partition_set(
        self, pset_class: Callable[[dict[PartID, PartitionT]], PartitionSet[PartitionT]]
    ) -> PartitionSet[PartitionT]:
        """Return the materialized partitions as a ResultPartitionSet.

        This can only be called once. After the partitions are handed off, this schedule will drop its references to the partitions.
        """
        assert not self._returned, "The partitions have already been returned."

        # Ensure that the plan has finished executing.
        assert self._completed, "The plan has not finished executing yet."

        # Ensure that all partitions have finished materializing.
        finished_partitions = [p for p in self._materializing_results if p is not None]
        assert len(finished_partitions) == len(
            self._materializing_results
        ), f"Not all partitions have finished materializing yet in results: {self._materializing_results}"

        # Return the result partition set.
        result = pset_class({})
        for i, partition_info in enumerate(finished_partitions):
            result.set_partition(i, partition_info.partition)

        self._returned = True
        self._materializing_results.clear()
        return result
