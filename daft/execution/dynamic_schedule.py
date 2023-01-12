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

        try:
            construct = next(next_source)
            if isinstance(construct, OpenConstruction):
                construct = construct.as_execution_request()
                next_requests.append(construct)
            yield construct

        except StopIteration:
            # Sources are dry.
            # If there are no pending executions either, then we have exhausted our sources as well and are done.
            if len(left_requests) + len(right_requests) == 0:
                return

            # Otherwise, we're still waiting for an execution result.
            yield None


def schedule_local_limit(
    source: Iterator[None | BaseConstruction[PartitionT]],
    limit: int,
    num_partitions: None | int = None,
) -> Generator[None | BaseConstruction[PartitionT], int, None]:
    """Apply a limit instruction to each partition in the source.

    limit:
        The value of the limit to apply to the first partition.
        For subsequent partitions, send the value of the limit to apply back into this generator.

    num_partitions:
    """
    for construction in source:
        if not isinstance(construct, OpenConstruction):
            yield construct
        else:
            limit = yield construction.add_instruction(dynamic_construction.LocalLimit(limit))


def schedule_global_limit(
    source: Iterator[None | BaseConstruction[PartitionT]],
    global_limit: logical_plan.GlobalLimit,
) -> Iterator[None | BaseConstruction[PartitionT]]:
    """Return the first n rows from the source partitions."""

    remaining_rows = global_limit._num
    remaining_partitions = global_limit.num_partitions()

    requests: deque[ExecutionRequest] = deque()


    # To dynamically schedule the global limit, we need to apply an appropriate limit to each incoming pending partition.
    # We don't know their exact sizes since they are pending execution, so we will have to iteratively execute them,
    # count their rows, and then apply and update the remaining limit.

    # The incoming pending partitions to execute.
    # As an optimization, push down a limit to reduce what gets materialized,
    # since we will never take more than the first k anyway.
    source = schedule_local_limit(source=source, limit=remaining_rows)
    started = False

    while True:
        # Check if any inputs finished executing.
        # Apply and deduct the rolling global limit.
        while len(requests) > 0 and requests[0].result is not None:
            result = requests.popleft().result
            limit = remaining_rows and min(remaining_rows, result.metadata().num_rows)

            new_construction = OpenConstruction[PartitionT]([result.partition()])
            new_construction.add_instruction(dynamic_construction.LocalLimit(limit))
            yield new_construction
            remaining_partitions -= 1
            remaining_rows -= limit

            if remaining_rows == 0:
                # We only need to return empty partitions now.
                # Instead of computing new ones and applying limit(0),
                # we can just reuse an existing computed partition.

                # Cancel all remaining results; we won't need them.
                for _ in range(len(requests)):
                    result_to_cancel = requests.popright().result
                    if result_to_cancel is not None:
                        result_to_cancel.cancel()

                yield from (
                    OpenConstruction[PartitionT]([result.partition()]).add_instruction(dynamic_construction.LocalLimit(0))
                    for _ in range(remaining_partitions)
                )
                return

        # (If we are doing limit(0) and already have a partition executing to use for it, just wait.)
        if remaining_rows == 0 and len(requests) > 0:
            yield None
            continue

        # Execute a single incoming partition.
        try:
            next_construction = source.send(remaining_rows if started else None)
            started = True
            if isinstance(next_construction, OpenConstruction):
                next_construction = next_construction.as_execution_request()
                requests.append(next_construction)
            yield next_construction

        except StopIteration:
            if len(requests) == 0:
                return
            yield None


def schedule_coalesce(
    source_schedule: Iterator[None | BaseConstruction[PartitionT]],
    coalesce: logical_plan.Coalesce,
) -> Iterator[None | BaseConstruction[PartitionT]]:

    coalesce_from = coalesce._children()[0].num_partitions()
    coalesce_to = coalesce.num_partitions()
    assert (
        coalesce_to <= coalesce_from
    ), f"Cannot coalesce upwards from {coalesce_from} to {coalesce_to} partitions."

    starts = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(coalesce_to)]
    stops = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(1, coalesce_to + 1)]
    # For each output partition, the number of input partitions to coalesce.
    num_partitions_per_result = deque([stop - start for start, stop in zip(starts, stops)])

    materializations = deque()
    while True:

        # See if we can emit a coalesced partition.
        num_partitions_to_merge = num_paritions_per_result[0]
        if len(materializations) >= num_partitions_to_merge:
            ready_to_coalesce = [
                result for i in range(num_partitions_to_merge)
                if (result := materializations[i].result) is not None
            ]
            if len(ready_to_coalesce) == num_partitions_to_merge:
                # Coalesce the partition and emit it.
                construct_merge = OpenConstruction[PartitionT]([_.partition() for _ in ready_to_coalesce])
                construct_merge.add_instruction(dynamic_construction.ReduceMerge())
                [materializations.popleft() for _ in range(num_partitions_to_merge)]
                num_partitions_per_result.popleft()
                yield construct_merge

        # Cannot emit a coalesced partition.
        # Materialize a single dependency.
        try:
            construction = next(source_schedule)
            if isinstance(construction, OpenConstruction):
                construction = construction.as_materialization_request()
                materializations.append(construction)
            yield construction

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def schedule_reduce(
    fanout_schedule: Iterator[None | BaseConstruction[PartitionT]],
    reduce_instruction: ReduceInstruction,
) -> Iterator[None | BaseConstruction[PartitionT]]:

    materializations = list()

    # Dispatch all fanouts.
    for construction in fanout_schedule:
        if isinstance(construction, OpenConstruction):
            construction = construction.as_materialization_request_multi()
            materializations.append(construction)
        yield construction

    # All fanouts dispatched. Wait for all of them to materialize
    # (since we need all of them to emit even a single reduce).
    while any(_.results is None for _ in materializations):
        yield None

    # Yield all the reduces in order.
    yield from (
        OpenConstruction[PartitionT]([
            result.partition()
            for result in (_.results[reduce_index] for _ in materializations)
        ]).add_instruction(reduce_instruction)
        for reduce_index in range(len(materializations[0].results))
    )


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
