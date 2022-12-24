from __future__ import annotations

import math
from abc import abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Callable, Generic, TypeVar

from daft.execution.dynamic_construction import Construction, InstructionFactory
from daft.logical import logical_plan
from daft.runners.partitioning import PartID, PartitionSet, vPartition

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
        dependencies: list[_PartitionT | None] = field(default_factory=list)

    def __init__(self) -> None:
        # Materialized partitions that this dynamic schedule needs to do its job.
        # `None` denotes a partition whose materialization is in-progress.
        self._materializations: DynamicSchedule.Materializations = self.Materializations[PartitionT]()
        self._completed = False

    def __next__(self) -> Construction[PartitionT] | None:
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
    def _next_impl(self) -> Construction[PartitionT] | None:
        """
        Raises StopIteration if there are no further instructions to give for this schedule.
        Returns None if there is nothing we can do for now (i.e. must wait for other partitions to finish).
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        name = self.__class__.__name__
        return f"{name}: self._completed={self._completed}, self._materializations={self._materializations}"


class SchedulePartitionRead(DynamicSchedule[PartitionT]):
    def __init__(self, partitions: list[PartitionT]) -> None:
        super().__init__()
        self._materializations.dependencies += partitions
        self._num_dispatched = 0

    def _next_impl(self) -> Construction[PartitionT] | None:

        dependencies = self._materializations.dependencies
        if self._num_dispatched == len(dependencies):
            raise StopIteration

        partition = dependencies[self._num_dispatched]
        self._num_dispatched += 1

        assert partition is not None  # for mypy; already enforced at __init__
        new_construct = Construction[PartitionT]([partition])
        return new_construct


class ScheduleFileRead(DynamicSchedule[PartitionT]):
    def __init__(self, child_schedule: DynamicSchedule, scan_node: logical_plan.TabularFilesScan) -> None:
        super().__init__()
        self._child_schedule = child_schedule
        self._scan_node = scan_node
        self._next_index = 0

    def _next_impl(self) -> Construction[PartitionT] | None:
        construct = next(self._child_schedule)
        if construct is None or construct.is_marked_for_materialization():
            return construct

        if self._next_index < self._scan_node.num_partitions():
            construct.add_instruction(InstructionFactory.read_file(self._scan_node, self._next_index))

            self._next_index += 1
            return construct

        else:
            raise StopIteration


class ScheduleFileWrite(DynamicSchedule[PartitionT]):
    def __init__(self, child_schedule: DynamicSchedule[PartitionT], write_node: logical_plan.FileWrite) -> None:
        super().__init__()
        self._child_schedule = child_schedule
        self._write_node = write_node
        self._writes_so_far = 0

    def _next_impl(self) -> Construction[PartitionT] | None:
        construct = next(self._child_schedule)
        if construct is None or construct.is_marked_for_materialization():
            return construct

        construct.add_instruction(InstructionFactory.write(node=self._write_node, index=self._writes_so_far))
        self._writes_so_far += 1

        return construct


class SchedulePipelineInstruction(DynamicSchedule[PartitionT]):
    def __init__(
        self,
        child_schedule: DynamicSchedule[PartitionT],
        pipeable_instruction: Callable[[list[vPartition]], list[vPartition]],
    ) -> None:
        super().__init__()
        self._child_schedule = child_schedule
        self._instruction = pipeable_instruction

    def _next_impl(self) -> Construction[PartitionT] | None:
        construct = next(self._child_schedule)
        if construct is None or construct.is_marked_for_materialization():
            return construct

        construct.add_instruction(self._instruction)

        return construct


class ScheduleJoin(DynamicSchedule[PartitionT]):
    @dataclass
    class Materializations(DynamicSchedule.Materializations[PartitionT]):
        lefts: list[PartitionT | None] = field(default_factory=list)
        rights: list[PartitionT | None] = field(default_factory=list)

    def __init__(
        self,
        left_source: DynamicSchedule[PartitionT],
        right_source: DynamicSchedule[PartitionT],
        join: logical_plan.Join,
    ) -> None:
        super().__init__()
        self._materializations: ScheduleJoin.Materializations = self.Materializations[PartitionT]()
        self._left_source = left_source
        self._right_source = right_source
        self._join_node = join
        self._next_to_emit = 0

    def _next_impl(self) -> Construction[PartitionT] | None:
        lefts = self._materializations.lefts
        rights = self._materializations.rights

        # Try emitting a join.
        if self._next_to_emit < len(lefts) and self._next_to_emit < len(rights):
            next_left = lefts[self._next_to_emit]
            next_right = rights[self._next_to_emit]
            if next_left is not None and next_right is not None:
                construct_join = Construction[PartitionT]([next_left, next_right])
                construct_join.add_instruction(InstructionFactory.join(self._join_node))

                self._next_to_emit += 1
                return construct_join

        # Can't emit a join just yet; materialize more dependencies.
        # Choose whether to materialize from left child or right child (whichever one is more behind).
        if len(lefts) <= len(rights):
            next_deps = lefts
            next_source = self._left_source
        else:
            next_deps = rights
            next_source = self._right_source

        construct = None
        try:
            construct = next(next_source)

        except StopIteration:
            # Source is dry; have we emitted all join results?
            if self._next_to_emit >= len(next_deps):
                raise

        if construct is None or construct.is_marked_for_materialization():
            return construct

        construct.mark_for_materialization(next_deps)
        return construct


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
                pass

            elif self._continue_from_partition < self._global_limit.num_partitions():
                # We need at least one materialized partition to get the correct schema to return.
                if dependencies[0] is None:
                    return None

                new_construct = Construction[PartitionT]([dependencies[0]])
                new_construct.add_instruction(InstructionFactory.local_limit(0))

                self._continue_from_partition += 1
                return new_construct

            else:
                raise StopIteration

        # We're not done; check to see if we can return a global limit partition.
        # Is the next local limit partition materialized?
        # If so, update global limit progress, and return the local limit.
        if self._continue_from_partition < len(dependencies):
            next_partition = dependencies[self._continue_from_partition]
            if next_partition is not None:
                next_limit = min(self._remaining_limit, len(next_partition))
                self._remaining_limit -= next_limit

                new_construct = Construction[PartitionT]([next_partition])
                if next_limit < len(next_partition):
                    new_construct.add_instruction(InstructionFactory.local_limit(next_limit))

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

        construct.add_instruction(InstructionFactory.local_limit(self._remaining_limit))

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
                construct_merge = Construction[PartitionT](ready_to_coalesce)
                construct_merge.add_instruction(InstructionFactory.merge())
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
        samples: list[PartitionT | None] = field(default_factory=list)
        boundaries: list[PartitionT | None] = field(default_factory=list)

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
            construct_sample = Construction[PartitionT]([source])
            construct_sample.add_instruction(InstructionFactory.map_to_samples(self._sort._sort_by))
            construct_sample.mark_for_materialization(sample_partitions)
            return construct_sample

        # Sample partitions are done, reduce to get quantiles.
        if not len(self._materializations.boundaries):
            finished_samples = [_ for _ in sample_partitions if _ is not None]
            if len(finished_samples) != len(sample_partitions):
                return None

            construct_boundaries = Construction[PartitionT](finished_samples)
            construct_boundaries.add_instruction(
                InstructionFactory.reduce_to_quantiles(
                    sort_by=self._sort._sort_by,
                    descending=self._sort._descending,
                    num_quantiles=self._sort.num_partitions(),
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
            fanout_fn = InstructionFactory.fanout_range(
                sort_by=self._sort._sort_by,
                descending=self._sort._descending,
                boundaries=boundaries_partition,
                num_outputs=self._sort.num_partitions(),
            )
            reduce_fn = InstructionFactory.merge_and_sort(
                sort_by=self._sort._sort_by,
                descending=self._sort._descending,
            )
            self._fanout_reduce = ScheduleFanoutReduce[PartitionT](
                child_schedule=from_partitions,
                num_outputs=self._sort.num_partitions(),
                fanout_fn=fanout_fn,
                reduce_fn=reduce_fn,
            )

        return next(self._fanout_reduce)


class ScheduleFanoutReduce(DynamicSchedule[PartitionT]):
    def __init__(
        self,
        child_schedule: DynamicSchedule[PartitionT],
        num_outputs: int,
        fanout_fn: Callable[[list[vPartition]], list[vPartition]],
        reduce_fn: Callable[[list[vPartition]], list[vPartition]],
    ) -> None:
        super().__init__()
        self._child_schedule = child_schedule
        self._num_outputs = num_outputs
        self._fanout_fn = fanout_fn
        self._reduce_fn = reduce_fn

        self._reduces_emitted = 0

    def _next_impl(self) -> Construction[PartitionT] | None:
        # Dispatch shufflemaps.
        try:
            construct = next(self._child_schedule)
            if construct is None or construct.is_marked_for_materialization():
                return construct

            construct.add_instruction(self._fanout_fn)

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

        construct_reduce = Construction[PartitionT](finished_reduce_dependencies)
        construct_reduce.add_instruction(self._reduce_fn)
        self._reduces_emitted += 1
        return construct_reduce


class ScheduleMaterialize(DynamicSchedule[PartitionT]):
    """Materializes its dependencies and does nothing else."""

    def __init__(self, child_schedule: DynamicSchedule[PartitionT]) -> None:
        super().__init__()
        self.child_schedule = child_schedule
        self._materializing_results: list[PartitionT | None] = []
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
        for i, partition in enumerate(finished_partitions):
            result.set_partition(i, partition)

        self._returned = True
        self._materializing_results = list()
        return result
