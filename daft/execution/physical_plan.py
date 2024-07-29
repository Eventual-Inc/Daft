"""
This file contains physical plan building blocks.
To get a physical plan for a logical plan, see physical_plan_factory.py.

Conceptually, a physical plan decides what steps, and the order of steps, to run to build some target.
Physical plans are closely related to logical plans. A logical plan describes "what you want", and a physical plan figures out "what to do" to get it.
They are not exact analogues, especially due to the ability of a physical plan to dynamically decide what to do next.

Physical plans are implemented here as an iterator of PartitionTask | None.
When a physical plan returns None, it means it cannot tell you what the next step is,
because it is waiting for the result of a previous PartitionTask to can decide what to do next.
"""

from __future__ import annotations

import collections
import itertools
import logging
import math
import pathlib
from collections import deque
from typing import (
    TYPE_CHECKING,
    Generator,
    Generic,
    Iterable,
    Iterator,
    TypeVar,
    Union,
)

from daft.context import get_context
from daft.daft import FileFormat, IOConfig, JoinType, ResourceRequest
from daft.execution import execution_step
from daft.execution.execution_step import (
    Instruction,
    MultiOutputPartitionTask,
    PartitionTask,
    PartitionTaskBuilder,
    ReduceInstruction,
    SingleOutputPartitionTask,
)
from daft.expressions import ExpressionsProjection
from daft.logical.schema import Schema
from daft.runners.partitioning import (
    MaterializedResult,
    PartitionT,
)
from daft.table.micropartition import MicroPartition

logger = logging.getLogger(__name__)

T = TypeVar("T")

if TYPE_CHECKING:
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import TableProperties as IcebergTableProperties


# A PhysicalPlan that is still being built - may yield both PartitionTaskBuilders and PartitionTasks.
InProgressPhysicalPlan = Iterator[Union[None, PartitionTask[PartitionT], PartitionTaskBuilder[PartitionT]]]

# A PhysicalPlan that is complete and will only yield PartitionTasks or final PartitionTs.
MaterializedPhysicalPlan = Iterator[Union[None, PartitionTask[PartitionT], MaterializedResult[PartitionT]]]


def _stage_id_counter():
    counter = 0
    while True:
        counter += 1
        yield counter


stage_id_counter = _stage_id_counter()


def partition_read(
    materialized_results: Iterator[MaterializedResult[PartitionT]],
) -> InProgressPhysicalPlan[PartitionT]:
    """Instantiate a (no-op) physical plan from existing partitions."""
    yield from (
        PartitionTaskBuilder[PartitionT](inputs=[mat_result.partition()], partial_metadatas=[mat_result.metadata()])
        for mat_result in materialized_results
    )


def file_write(
    child_plan: InProgressPhysicalPlan[PartitionT],
    file_format: FileFormat,
    schema: Schema,
    root_dir: str | pathlib.Path,
    compression: str | None,
    partition_cols: ExpressionsProjection | None,
    io_config: IOConfig | None,
) -> InProgressPhysicalPlan[PartitionT]:
    """Write the results of `child_plan` into files described by `write_info`."""

    yield from (
        step.add_instruction(
            execution_step.WriteFile(
                file_format=file_format,
                schema=schema,
                root_dir=root_dir,
                compression=compression,
                partition_cols=partition_cols,
                io_config=io_config,
            ),
        )
        if isinstance(step, PartitionTaskBuilder)
        else step
        for step in child_plan
    )


def iceberg_write(
    child_plan: InProgressPhysicalPlan[PartitionT],
    base_path: str,
    iceberg_schema: IcebergSchema,
    iceberg_properties: IcebergTableProperties,
    spec_id: int,
    io_config: IOConfig | None,
) -> InProgressPhysicalPlan[PartitionT]:
    """Write the results of `child_plan` into pyiceberg data files described by `write_info`."""

    yield from (
        step.add_instruction(
            execution_step.WriteIceberg(
                base_path=base_path,
                iceberg_schema=iceberg_schema,
                iceberg_properties=iceberg_properties,
                spec_id=spec_id,
                io_config=io_config,
            ),
        )
        if isinstance(step, PartitionTaskBuilder)
        else step
        for step in child_plan
    )


def deltalake_write(
    child_plan: InProgressPhysicalPlan[PartitionT],
    base_path: str,
    large_dtypes: bool,
    version: int,
    io_config: IOConfig | None,
) -> InProgressPhysicalPlan[PartitionT]:
    """Write the results of `child_plan` into pyiceberg data files described by `write_info`."""

    yield from (
        step.add_instruction(
            execution_step.WriteDeltaLake(
                base_path=base_path,
                large_dtypes=large_dtypes,
                version=version,
                io_config=io_config,
            ),
        )
        if isinstance(step, PartitionTaskBuilder)
        else step
        for step in child_plan
    )


def lance_write(
    child_plan: InProgressPhysicalPlan[PartitionT],
    base_path: str,
    mode: str,
    io_config: IOConfig | None,
    kwargs: dict | None,
) -> InProgressPhysicalPlan[PartitionT]:
    """Write the results of `child_plan` into lance data files described by `write_info`."""

    yield from (
        step.add_instruction(
            execution_step.WriteLance(
                base_path=base_path,
                mode=mode,
                io_config=io_config,
                kwargs=kwargs,
            ),
        )
        if isinstance(step, PartitionTaskBuilder)
        else step
        for step in child_plan
    )


def pipeline_instruction(
    child_plan: InProgressPhysicalPlan[PartitionT],
    pipeable_instruction: Instruction,
    resource_request: execution_step.ResourceRequest,
) -> InProgressPhysicalPlan[PartitionT]:
    """Apply an instruction to the results of `child_plan`."""

    yield from (
        step.add_instruction(pipeable_instruction, resource_request) if isinstance(step, PartitionTaskBuilder) else step
        for step in child_plan
    )


def monotonically_increasing_id(
    child_plan: InProgressPhysicalPlan[PartitionT], column_name: str
) -> InProgressPhysicalPlan[PartitionT]:
    """Apply a monotonically_increasing_id instruction to the results of `child_plan`."""

    partition_counter = (
        0  # This counter gives each partition a monotonically increasing int to use as the leftmost 28 bits of the id
    )
    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            yield step.add_instruction(
                execution_step.MonotonicallyIncreasingId(partition_counter, column_name), ResourceRequest()
            )
            partition_counter += 1
        else:
            yield step


def hash_join(
    left_plan: InProgressPhysicalPlan[PartitionT],
    right_plan: InProgressPhysicalPlan[PartitionT],
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
) -> InProgressPhysicalPlan[PartitionT]:
    """Hash-based pairwise join the partitions from `left_child_plan` and `right_child_plan` together."""

    # Materialize the steps from the left and right sources to get partitions.
    # As the materializations complete, emit new steps to join each left and right partition.
    left_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    right_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    yield_left = True

    while True:
        # Emit new join steps if we have left and right partitions ready.
        while (
            len(left_requests) > 0 and len(right_requests) > 0 and left_requests[0].done() and right_requests[0].done()
        ):
            next_left = left_requests.popleft()
            next_right = right_requests.popleft()

            # Calculate memory request for task.
            left_size_bytes = next_left.partition_metadata().size_bytes
            right_size_bytes = next_right.partition_metadata().size_bytes
            if left_size_bytes is None and right_size_bytes is None:
                size_bytes = None
            elif left_size_bytes is None and right_size_bytes is not None:
                # Use 2x the right side as the memory request, assuming that left and right side are ~ the same size.
                size_bytes = 2 * right_size_bytes
            elif right_size_bytes is None and left_size_bytes is not None:
                # Use 2x the left side as the memory request, assuming that left and right side are ~ the same size.
                size_bytes = 2 * left_size_bytes
            elif left_size_bytes is not None and right_size_bytes is not None:
                size_bytes = left_size_bytes + right_size_bytes

            join_step = PartitionTaskBuilder[PartitionT](
                inputs=[next_left.partition(), next_right.partition()],
                partial_metadatas=[next_left.partition_metadata(), next_right.partition_metadata()],
                resource_request=ResourceRequest(memory_bytes=size_bytes),
            ).add_instruction(
                instruction=execution_step.HashJoin(
                    left_on=left_on,
                    right_on=right_on,
                    how=how,
                    is_swapped=False,
                )
            )
            yield join_step

        # Exhausted all ready inputs; execute a single child step to get more join inputs.
        # Choose whether to execute from left child or right child (whichever one is more behind)
        if len(left_requests) < len(right_requests):
            next_plan, next_requests = left_plan, left_requests
        elif len(left_requests) > len(right_requests):
            next_plan, next_requests = right_plan, right_requests
        elif len(left_requests) == len(right_requests):
            # Both plans have progressed equally; alternate between the two plans to avoid starving either one
            next_plan, next_requests = (left_plan, left_requests) if yield_left else (right_plan, right_requests)
            yield_left = not yield_left

        try:
            step = next(next_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output(stage_id=stage_id)
                next_requests.append(step)
            yield step

        except StopIteration:
            # Left and right child plans have completed.
            # Are we still waiting for materializations to complete? (We will emit more joins from them).
            if len(left_requests) + len(right_requests) > 0:
                logger.debug(
                    "join blocked on completion of sources.\n Left sources: %s\nRight sources: %s",
                    left_requests,
                    right_requests,
                )
                yield None

            # Otherwise, we are entirely done.
            else:
                return


def _create_broadcast_join_step(
    broadcaster_parts: deque[SingleOutputPartitionTask[PartitionT]],
    receiver_part: SingleOutputPartitionTask[PartitionT],
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
    is_swapped: bool,
) -> PartitionTaskBuilder[PartitionT]:
    # Calculate memory request for task.
    broadcaster_size_bytes_ = 0
    broadcaster_partitions = []
    broadcaster_partition_metadatas = []
    null_count = 0
    for next_broadcaster in broadcaster_parts:
        next_broadcaster_partition_metadata = next_broadcaster.partition_metadata()
        if next_broadcaster_partition_metadata is None or next_broadcaster_partition_metadata.size_bytes is None:
            null_count += 1
        else:
            broadcaster_size_bytes_ += next_broadcaster_partition_metadata.size_bytes
        broadcaster_partitions.append(next_broadcaster.partition())
        broadcaster_partition_metadatas.append(next_broadcaster_partition_metadata)
    if null_count == len(broadcaster_parts):
        broadcaster_size_bytes = None
    elif null_count > 0:
        # Impute null size estimates with mean of non-null estimates.
        broadcaster_size_bytes = broadcaster_size_bytes_ + math.ceil(
            null_count * broadcaster_size_bytes_ / (len(broadcaster_parts) - null_count)
        )
    else:
        broadcaster_size_bytes = broadcaster_size_bytes_
    receiver_size_bytes = receiver_part.partition_metadata().size_bytes
    if broadcaster_size_bytes is None and receiver_size_bytes is None:
        size_bytes = None
    elif broadcaster_size_bytes is None and receiver_size_bytes is not None:
        # Use 1.25x the receiver side as the memory request, assuming that receiver side is ~4x larger than the broadcaster side.
        size_bytes = int(1.25 * receiver_size_bytes)
    elif receiver_size_bytes is None and broadcaster_size_bytes is not None:
        # Use 4x the broadcaster side as the memory request, assuming that receiver side is ~4x larger than the broadcaster side.
        size_bytes = 4 * broadcaster_size_bytes
    elif broadcaster_size_bytes is not None and receiver_size_bytes is not None:
        size_bytes = broadcaster_size_bytes + receiver_size_bytes

    return PartitionTaskBuilder[PartitionT](
        inputs=broadcaster_partitions + [receiver_part.partition()],
        partial_metadatas=list(broadcaster_partition_metadatas + [receiver_part.partition_metadata()]),
        resource_request=ResourceRequest(memory_bytes=size_bytes),
    ).add_instruction(
        instruction=execution_step.BroadcastJoin(
            left_on=left_on,
            right_on=right_on,
            how=how,
            is_swapped=is_swapped,
        )
    )


def broadcast_join(
    broadcaster_plan: InProgressPhysicalPlan[PartitionT],
    receiver_plan: InProgressPhysicalPlan[PartitionT],
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
    is_swapped: bool,
) -> InProgressPhysicalPlan[PartitionT]:
    """Broadcast join all partitions from the broadcaster child plan to each partition in the receiver child plan."""

    # Materialize the steps from the broadcaster and receiver sources to get partitions.
    # As the receiver-side materializations complete, emit new steps to join each broadcaster and receiver partition.
    stage_id = next(stage_id_counter)
    broadcaster_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    broadcaster_parts: deque[SingleOutputPartitionTask[PartitionT]] = deque()

    # First, fully materialize the broadcasting side (broadcaster side) of the join.
    while True:
        # Moved completed partition tasks in the broadcaster side of the join to the materialized partition set.
        while broadcaster_requests and broadcaster_requests[0].done():
            broadcaster_parts.append(broadcaster_requests.popleft())

        # Execute single child step to pull in more broadcaster-side partitions.
        try:
            step = next(broadcaster_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output(stage_id=stage_id)
                broadcaster_requests.append(step)
            yield step
        except StopIteration:
            if broadcaster_requests:
                logger.debug(
                    "broadcast join blocked on completion of broadcasting side of join.\n broadcaster sources: %s",
                    broadcaster_requests,
                )
                yield None
            else:
                break

    # Second, broadcast materialized partitions to receiver side of join, as it materializes.
    receiver_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()

    while True:
        # Emit join steps for newly materialized partitions.
        # Broadcast all broadcaster partitions to each new receiver partition that was materialized on this dispatch loop.
        while receiver_requests and receiver_requests[0].done():
            receiver_part = receiver_requests.popleft()
            yield _create_broadcast_join_step(broadcaster_parts, receiver_part, left_on, right_on, how, is_swapped)

        # Execute single child step to pull in more input partitions.
        try:
            step = next(receiver_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output(stage_id=stage_id)
                receiver_requests.append(step)
            yield step
        except StopIteration:
            if receiver_requests:
                logger.debug(
                    "broadcast join blocked on completion of receiver side of join.\n receiver sources: %s",
                    receiver_requests,
                )
                yield None
            else:
                return


class MergeJoinTaskTracker(Generic[PartitionT]):
    """
    Tracks merge-join tasks for each larger-side partition.

    Merge-join tasks are added to the tracker, and the tracker handles empty tasks, finalizing PartitionTaskBuilders,
    determining whether tasks are ready to be executed, checking whether tasks are done, and deciding whether a coalesce
    is needed.
    """

    def __init__(self, stage_id: int):
        # Merge-join tasks that have not yet been finalized or yielded to the runner. We don't finalize a merge-join
        # task until we have at least 2 non-empty merge-join tasks, at which point this task will be popped from
        # _task_staging, finalized, and put into _finalized_tasks.
        self._task_staging: dict[str, PartitionTaskBuilder[PartitionT]] = {}
        # Merge-join tasks that have been finalized, but not yet yielded to the runner.
        self._finalized_tasks: collections.defaultdict[str, deque[SingleOutputPartitionTask[PartitionT]]] = (
            collections.defaultdict(deque)
        )
        # Merge-join tasks that have been yielded to the runner, and still need to be coalesced.
        self._uncoalesced_tasks: collections.defaultdict[str, deque[SingleOutputPartitionTask[PartitionT]]] = (
            collections.defaultdict(deque)
        )
        # Larger-side partitions that have been finalized, i.e. we're guaranteed that no more smaller-side partitions
        # will be added to the tracker for this partition.
        self._finalized: dict[str, bool] = {}
        self._stage_id = stage_id

    def add_task(self, part_id: str, task: PartitionTaskBuilder[PartitionT]) -> None:
        """
        Add a merge-join task to the tracker for the provided larger-side partition.

        This task needs to be unfinalized, i.e. a PartitionTaskBuilder.
        """
        # If no merge-join tasks have been added to the tracker yet for this partition, or we have an empty task in
        # staging, add the unfinalized merge-join task to staging.
        if not self._is_contained(part_id) or (
            part_id in self._task_staging and self._task_staging[part_id].is_empty()
        ):
            self._task_staging[part_id] = task
        # Otherwise, we have at least 2 (probably) non-empty merge-join tasks, so we finalize the new task and add it
        # to _finalized_tasks. If the new task is empty, then we drop it (we already have at least one task for this
        # partition, so no use in keeping an additional empty task around).
        elif not task.is_empty():
            # If we have a task in staging, we know from the first if statement that it's non-empty, so we finalize it
            # and add it to _finalized_tasks.
            if part_id in self._task_staging:
                self._finalized_tasks[part_id].append(
                    self._task_staging.pop(part_id).finalize_partition_task_single_output(self._stage_id)
                )
            self._finalized_tasks[part_id].append(task.finalize_partition_task_single_output(self._stage_id))

    def finalize(self, part_id: str) -> None:
        """
        Indicates to the tracker that we are done adding merge-join tasks for this partition.
        """
        # All finalized tasks should have been yielded before the tracker.finalize() call.
        finalized_tasks = self._finalized_tasks.pop(part_id, deque())
        assert len(finalized_tasks) == 0

        self._finalized[part_id] = True

    def yield_ready(
        self, part_id: str
    ) -> Iterator[SingleOutputPartitionTask[PartitionT] | PartitionTaskBuilder[PartitionT]]:
        """
        Returns an iterator of all tasks for this partition that are ready for execution. Each merge-join task will be
        yielded once, even across multiple calls.
        """
        assert self._is_contained(part_id)
        if part_id in self._finalized_tasks:
            # Yield the finalized tasks and add them to the uncoalesced queue.
            while self._finalized_tasks[part_id]:
                task = self._finalized_tasks[part_id].popleft()
                yield task
                self._uncoalesced_tasks[part_id].append(task)
        elif self._finalized.get(part_id, False) and part_id in self._task_staging:
            # If the tracker has been finalized for this partition, we can yield unfinalized tasks directly from
            # staging since no future tasks will be added.
            yield self._task_staging.pop(part_id)

    def pop_uncoalesced(self, part_id: str) -> deque[SingleOutputPartitionTask[PartitionT]] | None:
        """
        Returns all tasks for this partition that need to be coalesced. If this partition only involved a single
        merge-join task (i.e. we don't need to coalesce), this this function will return None.

        NOTE: tracker.finalize(part_id) must be called before this function.
        """
        assert self._finalized[part_id]
        return self._uncoalesced_tasks.pop(part_id, None)

    def all_tasks_done_for_partition(self, part_id: str) -> bool:
        """
        Return whether all merge-join tasks for this partition are done.
        """
        assert self._is_contained(part_id)
        if part_id in self._task_staging:
            # Unfinalized tasks are trivially "done".
            return True
        return all(
            task.done()
            for task in itertools.chain(
                self._finalized_tasks.get(part_id, deque()), self._uncoalesced_tasks.get(part_id, deque())
            )
        )

    def all_tasks_done(self) -> bool:
        """
        Return whether all merge-join tasks for all partitions are done.
        """
        return all(
            self.all_tasks_done_for_partition(part_id)
            for part_id in itertools.chain(
                self._uncoalesced_tasks.keys(), self._finalized_tasks.keys(), self._task_staging.keys()
            )
        )

    def _is_contained(self, part_id: str) -> bool:
        """
        Return whether the provided partition is being tracked by this tracker.
        """
        return part_id in self._task_staging or part_id in self._finalized_tasks or part_id in self._uncoalesced_tasks


def _emit_merge_joins_on_window(
    next_part: SingleOutputPartitionTask[PartitionT],
    other_window: deque[SingleOutputPartitionTask[PartitionT]],
    merge_join_task_tracker: MergeJoinTaskTracker[PartitionT],
    flipped: bool,
    next_is_larger: bool,
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
) -> Iterator[PartitionTaskBuilder[PartitionT] | PartitionTask[PartitionT]]:
    """
    Emits merge-join steps of next_part with each partition in other_window.
    """
    # Emit a merge-join step for all partitions in the other window that intersect with this new partition.
    for other_next_part in other_window:
        memory_bytes = _memory_bytes_for_merge(next_part, other_next_part)
        inputs = [next_part.partition(), other_next_part.partition()]
        partial_metadatas = [
            next_part.partition_metadata().downcast_to_partial(),
            other_next_part.partition_metadata().downcast_to_partial(),
        ]
        # If next, other are flipped (right, left partitions), flip them back.
        if flipped:
            inputs = list(reversed(inputs))
            partial_metadatas = list(reversed(partial_metadatas))
        join_task = PartitionTaskBuilder[PartitionT](
            inputs=inputs,
            partial_metadatas=partial_metadatas,
            resource_request=ResourceRequest(memory_bytes=memory_bytes),
        ).add_instruction(
            instruction=execution_step.MergeJoin(
                left_on=left_on,
                right_on=right_on,
                how=how,
                preserve_left_bounds=not flipped,
            )
        )
        part_id = next_part.id() if next_is_larger else other_next_part.id()
        # Add to new merge-join step to tracked steps for this larger-side partition, and possibly start finalizing +
        # emitting non-empty join steps if there are now more than one.
        merge_join_task_tracker.add_task(part_id, join_task)
        yield from merge_join_task_tracker.yield_ready(part_id)


def _memory_bytes_for_merge(
    next_left: SingleOutputPartitionTask[PartitionT], next_right: SingleOutputPartitionTask[PartitionT]
) -> int | None:
    # Calculate memory request for merge task.
    left_size_bytes = next_left.partition_metadata().size_bytes
    right_size_bytes = next_right.partition_metadata().size_bytes
    if left_size_bytes is None and right_size_bytes is None:
        size_bytes = None
    elif left_size_bytes is None and right_size_bytes is not None:
        # Use 2x the right side as the memory request, assuming that left and right side are ~ the same size.
        size_bytes = 2 * right_size_bytes
    elif right_size_bytes is None and left_size_bytes is not None:
        # Use 2x the left side as the memory request, assuming that left and right side are ~ the same size.
        size_bytes = 2 * left_size_bytes
    elif left_size_bytes is not None and right_size_bytes is not None:
        size_bytes = left_size_bytes + right_size_bytes
    return size_bytes


def merge_join_sorted(
    left_plan: InProgressPhysicalPlan[PartitionT],
    right_plan: InProgressPhysicalPlan[PartitionT],
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
    left_is_larger: bool,
) -> InProgressPhysicalPlan[PartitionT]:
    """
    Merge the sorted partitions from `left_plan` and `right_plan` together.

    This assumes that `left_plan` and `right_plan` are both sorted on the join key(s), although with potentially
    different range partitionings (partition boundaries).
    """

    # Large vs. smaller side of join.
    larger_plan = left_plan if left_is_larger else right_plan
    smaller_plan = right_plan if left_is_larger else left_plan

    stage_id = next(stage_id_counter)

    # In-progress tasks for larger side of join.
    larger_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    # In-progress tasks for smaller side of join.
    smaller_requests: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    # Materialized partitions for larger side of join; a larger-side partition isn't dropped until we've emitted all
    # join steps with smaller-side partitions that may overlap with it..
    larger_window: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    # Materialized partitions for smaller side of join; a smaller-side partition isn't dropped until the most recent
    # larger-side materialized partition has a higher upper bound, which suggests that this smaller-side partition won't
    # be able to intersect with any future larger-side partitions.
    smaller_window: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    # Tracks merge-join partition tasks emitted for each partition on the larger side of the join.
    # Once all merge-join tasks are done, the corresponding output partitions will be coalesced together.
    # If only a single merge-join task is emitted for a larger-side partition, it will be an unfinalized
    # PartitionTaskBuilder, the coalescing step will be skipped, and this merge-join task will be yielded without
    # finalizing in order to allow fusion with downstream tasks; otherwise, the tracker will contain finalized
    # PartitionTasks.
    merge_join_task_tracker: MergeJoinTaskTracker[PartitionT] = MergeJoinTaskTracker(stage_id)

    yield_smaller = True
    smaller_done = False
    larger_done = False

    # As partitions materialize from either side of the join, emit new merge-join steps to join overlapping partitions
    # together.
    while True:
        # Emit merge-join steps on newly completed partitions from the smaller side of the join with a window of
        # (possibly) intersecting partitions from the larger side.
        while smaller_requests and smaller_requests[0].done():
            next_part = smaller_requests.popleft()
            yield from _emit_merge_joins_on_window(
                next_part,
                larger_window,
                merge_join_task_tracker,
                left_is_larger,
                False,
                left_on,
                right_on,
                how,
            )
            smaller_window.append(next_part)
        # Emit merge-join steps on newly completed partitions from the larger side of the join with a window of
        # (possibly) intersecting partitions from the smaller side.
        while larger_requests and larger_requests[0].done():
            next_part = larger_requests.popleft()
            yield from _emit_merge_joins_on_window(
                next_part,
                smaller_window,
                merge_join_task_tracker,
                not left_is_larger,
                True,
                left_on,
                right_on,
                how,
            )
            larger_window.append(next_part)
        # Remove prefix of smaller window that's under the high water mark set by this new larger-side partition,
        # since this prefix won't be able to match any future partitions on the smaller side of the join.
        while (
            # We always leave at least one partition in the smaller-side window in case we need to yield an empty
            # merge-join step for a future larger-side partition.
            len(smaller_window) > (1 if larger_requests else 0)
            and larger_window
            and _is_strictly_bounded_above_by(smaller_window[0], larger_window[-1])
        ):
            smaller_window.popleft()
        # For each partition we remove from the larger window, we launch a coalesce task over all output partitions
        # that correspond to that larger partition.
        # This loop also removes the prefix of larger window that's under the high water mark set by the smaller window,
        # since this prefix won't be able to match any future partitions on the smaller side.
        while (
            # Must be a larger-side partition whose outputs need finalizing.
            larger_window
            and (
                # Larger-side partition is bounded above by the most recent smaller-side partition, which means that no
                # future smaller-side partition can intersect with this larger-side partition, allowing us to finalize
                # the merge-join steps for the larger-side partition.
                (smaller_window and _is_strictly_bounded_above_by(larger_window[0], smaller_window[-1]))
                # No more smaller partitions left, so we should launch coalesce tasks for all remaining
                # larger-side partitions.
                or smaller_done
            )
            and (
                # Only finalize merge-join tasks for larger-side partition if all outputs are done OR there's only a
                # single finalized output (in which case we yield and unfinalized merge-join task to allow downstream
                # fusion with it).
                merge_join_task_tracker.all_tasks_done_for_partition(larger_window[0].id())
            )
        ):
            done_larger_part = larger_window.popleft()
            part_id = done_larger_part.id()
            # Indicate to merge-join task tracker that no more merge-join tasks will be added for this partition.
            merge_join_task_tracker.finalize(part_id)
            # Yield any merge-join tasks that are now ready after finalizing the tracking for this partition (i.e. if
            # there was only a single merge-join task added to the tracker for this partition, it will now be yielded
            # here).
            yield from merge_join_task_tracker.yield_ready(part_id)
            # Get merge-join tasks that need to be coalesced.
            tasks = merge_join_task_tracker.pop_uncoalesced(part_id)
            if tasks is None:
                # Only one output partition, so no coalesce needed.
                continue
            # At least two (probably non-empty) merge-join tasks for this group, so need to coalesce.
            # NOTE: We guarantee in _emit_merge_joins_on_window that any group containing 2 or more partition tasks
            # will only contain non-guaranteed-empty partitions; i.e., we'll need to execute a task to determine if
            # they actually are empty, so we just issue the coalesce task.
            # TODO(Clark): Elide coalescing by emitting a single merge-join task per larger-side partition, including as
            # input all intersecting partitions from the smaller side of the join.
            size_bytes = _memory_bytes_for_coalesce(tasks)
            coalesce_task = PartitionTaskBuilder[PartitionT](
                inputs=[task.partition() for task in tasks],
                partial_metadatas=[task.partition_metadata() for task in tasks],
                resource_request=ResourceRequest(memory_bytes=size_bytes),
            ).add_instruction(
                instruction=execution_step.ReduceMerge(),
            )
            yield coalesce_task

        # Exhausted all ready inputs; execute a single child step to get more join inputs.
        # Choose whether to execute from smaller child or larger child (whichever one is furthest behind).
        num_smaller_in_flight = len(smaller_requests) + len(smaller_window)
        num_larger_in_flight = len(larger_requests) + len(larger_window)
        if smaller_done or larger_done or num_smaller_in_flight == num_larger_in_flight:
            # Both plans have progressed equally (or the last yielded side is done); alternate between the two plans
            # to avoid starving either one.
            yield_smaller = not yield_smaller
            next_plan, next_requests = (
                (smaller_plan, smaller_requests) if yield_smaller else (larger_plan, larger_requests)
            )
        elif num_smaller_in_flight < num_larger_in_flight:
            # Larger side of join is further along than the smaller side, so pull from the smaller side next.
            next_plan, next_requests = smaller_plan, smaller_requests
            yield_smaller = True
        else:
            # Smaller side of join is further along than the larger side, so pull from the larger side next.
            next_plan, next_requests = larger_plan, larger_requests
            yield_smaller = False

        # Pull from the chosen side of the join.
        try:
            step = next(next_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output(stage_id=stage_id)
                next_requests.append(step)
            yield step

        except StopIteration:
            # We've exhausted one of the sides of the join.
            # If we have active tasks for either side of the join that completed while dispatching intermediate work,
            # we continue with another loop so we can process those newly ready inputs.
            if (smaller_requests and smaller_requests[0].done()) or (larger_requests and larger_requests[0].done()):
                continue
            # If we have active tasks for either side of the join that aren't done, tell runner that we're blocked on inputs.
            elif smaller_requests or larger_requests:
                logger.debug(
                    "merge join blocked on completion of sources.\n Left sources: %s\nRight sources: %s",
                    larger_requests if left_is_larger else smaller_requests,
                    smaller_requests if left_is_larger else larger_requests,
                )
                yield None
            # If we just exhausted small side of join, set smaller done flag.
            elif yield_smaller and not smaller_done:
                smaller_done = True
            # If we just exhausted larger side of join, set larger done flag.
            elif not yield_smaller and not larger_done:
                larger_done = True
            # We might still be waiting for some merge-join tasks to complete whose output we still need
            # to coalesce.
            elif not merge_join_task_tracker.all_tasks_done():
                logger.debug(
                    "merge join blocked on completion of merge join tasks (pre-coalesce).\nMerge-join tasks: %s",
                    list(merge_join_task_tracker._finalized_tasks.values()),
                )
                yield None
            # Otherwise, all join inputs are done and all merge-join tasks are done, so we are entirely done emitting
            # merge join work.
            else:
                return


def _is_strictly_bounded_above_by(
    lower_part: SingleOutputPartitionTask[PartitionT], upper_part: SingleOutputPartitionTask[PartitionT]
) -> bool:
    """
    Returns whether lower_part is strictly bounded above by upper part; i.e., whether lower_part's upper bound is
    strictly less than upper_part's upper bound.
    """
    lower_boundaries = lower_part.partition_metadata().boundaries
    upper_boundaries = upper_part.partition_metadata().boundaries
    assert lower_boundaries is not None and upper_boundaries is not None
    return lower_boundaries.is_strictly_bounded_above_by(upper_boundaries)


def _memory_bytes_for_coalesce(input_parts: Iterable[SingleOutputPartitionTask[PartitionT]]) -> int | None:
    # Calculate memory request for task.
    size_bytes_per_task = [task.partition_metadata().size_bytes for task in input_parts]
    non_null_size_bytes_per_task = [size for size in size_bytes_per_task if size is not None]
    non_null_size_bytes = sum(non_null_size_bytes_per_task)
    if len(size_bytes_per_task) == len(non_null_size_bytes_per_task):
        # If all task size bytes are non-null, directly use the non-null size bytes sum.
        size_bytes = non_null_size_bytes
    elif non_null_size_bytes_per_task:
        # If some are null, calculate the non-null mean and assume that null task size bytes
        # have that size.
        mean_size = math.ceil(non_null_size_bytes / len(non_null_size_bytes_per_task))
        size_bytes = non_null_size_bytes + mean_size * (len(size_bytes_per_task) - len(non_null_size_bytes_per_task))
    else:
        # If all null, set to null.
        size_bytes = None
    return size_bytes


def sort_merge_join_aligned_boundaries(
    left_plan: InProgressPhysicalPlan[PartitionT],
    right_plan: InProgressPhysicalPlan[PartitionT],
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
    num_partitions: int,
    left_is_larger: bool,
) -> InProgressPhysicalPlan[PartitionT]:
    """
    Sort-merge join the partitions from `left_plan` and `right_plan` together.

    This assumes that both `left_plan` and `right_plan` need to be sorted, and will be sorted using the same
    partitioning boundaries.
    """
    # This algorithm proceeds in the following phases:
    #  1. Sort both sides of the join.
    #    a. Fully materialize left and right child plans.
    #    b. Sample all partitions from both sides of the join.
    #    c. Create partitioning boundaries from global samples.
    #    d. Sort each side of join using global partitioning boundaries.
    #  2. Merge-join the now-sorted sides of the join.
    descending = [False] * len(left_on)
    # First, materialize the left and right child plans.
    left_source_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    right_source_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id_children = next(stage_id_counter)
    for child, source_materializations in [
        (left_plan, left_source_materializations),
        (right_plan, right_source_materializations),
    ]:
        for step in child:
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output(stage_id=stage_id_children)
                source_materializations.append(step)
            yield step

    # Sample all partitions (to be used for calculating sort partitioning boundaries).
    left_sample_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    right_sample_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id_sampling = next(stage_id_counter)
    sample_size = get_context().daft_execution_config.sample_size_for_sort

    sample_size = get_context().daft_execution_config.sample_size_for_sort
    for source_materializations, on, sample_materializations in [
        (left_source_materializations, left_on, left_sample_materializations),
        (right_source_materializations, right_on, right_sample_materializations),
    ]:
        for source in source_materializations:
            while not source.done():
                logger.debug("sort blocked on completion of source: %s", source)
                yield None

            sample = (
                PartitionTaskBuilder[PartitionT](
                    inputs=[source.partition()],
                    partial_metadatas=None,
                )
                .add_instruction(
                    instruction=execution_step.Sample(sort_by=on, size=sample_size),
                )
                # Rename sample columns so they align with sort_by_left naming, so we can reduce to combined quantiles below.
                # NOTE: This instruction will be a no-op for the left side of the sort.
                .add_instruction(
                    instruction=execution_step.Project(
                        projection=ExpressionsProjection(
                            [
                                e.alias(left_name)
                                for e, left_name in zip(on.to_column_expressions(), [e.name() for e in left_on])
                            ]
                        ),
                    )
                )
                .finalize_partition_task_single_output(stage_id=stage_id_sampling)
            )

            sample_materializations.append(sample)
            yield sample

    # Wait for samples from both child plans to materialize.
    for sample_materializations in (left_sample_materializations, right_sample_materializations):
        while any(not _.done() for _ in sample_materializations):
            logger.debug("sort blocked on completion of all samples: %s", sample_materializations)
            yield None

    stage_id_reduce = next(stage_id_counter)

    # Reduce the samples from both child plans to get combined sort partitioning boundaries.
    left_boundaries = (
        PartitionTaskBuilder[PartitionT](
            inputs=[
                sample.partition()
                for sample in itertools.chain(
                    consume_deque(left_sample_materializations), consume_deque(right_sample_materializations)
                )
            ],
            partial_metadatas=None,
        )
        .add_instruction(
            execution_step.ReduceToQuantiles(
                num_quantiles=num_partitions,
                sort_by=left_on,
                descending=descending,
            ),
        )
        .finalize_partition_task_single_output(stage_id=stage_id_reduce)
    )
    yield left_boundaries

    # Wait for boundaries to materialize.
    while not left_boundaries.done():
        logger.debug("sort blocked on completion of boundary partition: %s", left_boundaries)
        yield None

    # Project boundaries back to the right-side column names.
    # TODO(Clark): Refactor execution model to be able to fuse this with downstream sorting.
    right_boundaries = (
        PartitionTaskBuilder[PartitionT](
            inputs=[left_boundaries.partition()],
            partial_metadatas=None,
        )
        # Rename quantile columns so their original naming is restored, so we can sort each child with their native expression.
        .add_instruction(
            instruction=execution_step.Project(
                projection=ExpressionsProjection(
                    [
                        e.alias(right_name)
                        for e, right_name in zip(left_on.to_column_expressions(), [e.name() for e in right_on])
                    ]
                ),
            )
        )
        .finalize_partition_task_single_output(stage_id=stage_id_reduce)
    )
    yield right_boundaries

    # Wait for right-side boundaries to materialize.
    while not right_boundaries.done():
        logger.debug("sort blocked on completion of boundary partition: %s", right_boundaries)
        yield None

    # Sort both children using the combined boundaries.
    sorted_plans: list[InProgressPhysicalPlan[PartitionT]] = []
    for on, source_materializations, boundaries in [
        (left_on, left_source_materializations, left_boundaries),
        (right_on, right_source_materializations, right_boundaries),
    ]:
        # NOTE: We need to give reduce() an iter(list), since giving it a generator would result in lazy
        # binding in this loop.
        range_fanout_plan = [
            PartitionTaskBuilder[PartitionT](
                inputs=[boundaries.partition(), source.partition()],
                partial_metadatas=[boundaries.partition_metadata(), source.partition_metadata()],
                resource_request=ResourceRequest(
                    memory_bytes=source.partition_metadata().size_bytes,
                ),
            ).add_instruction(
                instruction=execution_step.FanoutRange[PartitionT](
                    _num_outputs=num_partitions,
                    sort_by=on,
                    descending=descending,
                ),
            )
            for source in consume_deque(source_materializations)
        ]

        # Execute a sorting reduce on it.
        per_partition_bounds = _to_per_partition_bounds(boundaries.vpartition(), num_partitions)
        sorted_plans.append(
            reduce(
                fanout_plan=iter(range_fanout_plan),
                reduce_instructions=[
                    execution_step.ReduceMergeAndSort(
                        sort_by=on,
                        descending=descending,
                        bounds=per_part_boundaries,
                    )
                    for per_part_boundaries in per_partition_bounds
                ],
            )
        )

    left_sorted_plan, right_sorted_plan = sorted_plans

    # Merge-join the two sorted sides of the join.
    yield from merge_join_sorted(left_sorted_plan, right_sorted_plan, left_on, right_on, how, left_is_larger)


def _to_per_partition_bounds(boundaries: MicroPartition, num_partitions: int) -> list[MicroPartition]:
    boundaries_dict = boundaries.to_pydict()
    return [
        MicroPartition.from_pydict(
            {
                col_name: [
                    pivots[i - 1] if i > 0 and i - 1 < len(pivots) else None,
                    pivots[i] if i < len(pivots) else None,
                ]
                for col_name, pivots in boundaries_dict.items()
            }
        )
        for i in range(num_partitions)
    ]


def concat(
    top_plan: InProgressPhysicalPlan[PartitionT], bottom_plan: InProgressPhysicalPlan[PartitionT]
) -> InProgressPhysicalPlan[PartitionT]:
    """Vertical concat of the partitions in `top_plan` and `bottom_plan`"""
    # Yield steps in order from the top_plan to bottom_plan
    yield from top_plan
    yield from bottom_plan


def local_limit(
    child_plan: InProgressPhysicalPlan[PartitionT],
    limit: int,
) -> Generator[None | PartitionTask[PartitionT] | PartitionTaskBuilder[PartitionT], int, None]:
    """Apply a limit instruction to each partition in the child_plan.

    limit:
        The value of the limit to apply to each partition.

    Yields: PartitionTask with the limit applied.
    Send back: A new value to the limit (optional). This allows you to update the limit after each partition if desired.
    """
    for step in child_plan:
        if not isinstance(step, PartitionTaskBuilder):
            yield step
        else:
            maybe_new_limit = yield step.add_instruction(
                execution_step.LocalLimit(limit),
            )
            if maybe_new_limit is not None:
                limit = maybe_new_limit


def global_limit(
    child_plan: InProgressPhysicalPlan[PartitionT],
    limit_rows: int,
    eager: bool,
    num_partitions: int,
) -> InProgressPhysicalPlan[PartitionT]:
    """Return the first n rows from the `child_plan`."""

    remaining_rows = limit_rows
    assert remaining_rows >= 0, f"Invalid value for limit: {remaining_rows}"
    remaining_partitions = num_partitions

    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    # To dynamically schedule the global limit, we need to apply an appropriate limit to each child partition.
    # We don't know their exact sizes since they are pending execution, so we will have to iteratively execute them,
    # count their rows, and then apply and update the remaining limit.

    # As an optimization, push down a limit into each partition to reduce what gets materialized,
    # since we will never take more than the remaining limit anyway.
    child_plan = local_limit(child_plan=child_plan, limit=remaining_rows)
    started = False
    while True:
        # Check if any inputs finished executing.
        # Apply and deduct the rolling global limit.
        while len(materializations) > 0 and materializations[0].done():
            done_task = materializations.popleft()
            done_task_metadata = done_task.partition_metadata()
            limit = remaining_rows and min(remaining_rows, done_task_metadata.num_rows)

            global_limit_step = PartitionTaskBuilder[PartitionT](
                inputs=[done_task.partition()],
                partial_metadatas=[done_task_metadata],
                resource_request=ResourceRequest(memory_bytes=done_task_metadata.size_bytes),
            ).add_instruction(
                instruction=execution_step.GlobalLimit(limit),
            )

            yield global_limit_step
            remaining_partitions -= 1
            remaining_rows -= limit

            if remaining_rows == 0:
                # We only need to return empty partitions now.
                # Instead of computing new ones and applying limit(0),
                # we can just reuse an existing computed partition.

                # Cancel all remaining results; we won't need them.
                while len(materializations) > 0:
                    materializations.pop().cancel()

                yield from (
                    PartitionTaskBuilder[PartitionT](
                        inputs=[done_task.partition()],
                        partial_metadatas=[done_task.partition_metadata()],
                        resource_request=ResourceRequest(memory_bytes=done_task.partition_metadata().size_bytes),
                    ).add_instruction(
                        instruction=execution_step.GlobalLimit(0),
                    )
                    for _ in range(remaining_partitions)
                )
                return

        # (Optimization. If we are doing limit(0) and already have a partition executing to use for it, just wait.)
        if remaining_rows == 0 and len(materializations) > 0:
            logger.debug("global_limit blocked on completion of: %s", materializations[0])
            yield None
            continue

        # If running in eager mode, only allow one task in flight
        if eager and len(materializations) > 0:
            logger.debug("global_limit blocking on eager execution of: %s", materializations[0])
            yield None
            continue

        # Execute a single child partition.
        try:
            child_step = child_plan.send(remaining_rows) if started else next(child_plan)
            started = True
            if isinstance(child_step, PartitionTaskBuilder):
                # If this is the very next partition to apply a nonvacuous global limit on,
                # see if it has any row metadata already.
                # If so, we can deterministically apply and deduct the rolling limit without materializing.
                [partial_meta] = child_step.partial_metadatas
                if len(materializations) == 0 and remaining_rows > 0 and partial_meta.num_rows is not None:
                    limit = min(remaining_rows, partial_meta.num_rows)
                    child_step = child_step.add_instruction(instruction=execution_step.LocalLimit(limit))

                    remaining_partitions -= 1
                    remaining_rows -= limit
                else:
                    child_step = child_step.finalize_partition_task_single_output(stage_id=stage_id)
                    materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                logger.debug("global_limit blocked on completion of first source in: %s", materializations)
                yield None
            else:
                return


def flatten_plan(child_plan: InProgressPhysicalPlan[PartitionT]) -> InProgressPhysicalPlan[PartitionT]:
    """Wrap a plan that emits multi-output tasks to a plan that emits single-output tasks."""

    materializations: deque[MultiOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    while True:
        while len(materializations) > 0 and materializations[0].done():
            done_task = materializations.popleft()
            for partition, metadata in zip(done_task.partitions(), done_task.partition_metadatas()):
                yield PartitionTaskBuilder[PartitionT](
                    inputs=[partition],
                    partial_metadatas=[metadata],
                    resource_request=ResourceRequest(memory_bytes=metadata.size_bytes),
                )

        try:
            step = next(child_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_multi_output(stage_id=stage_id)
                materializations.append(step)
            yield step

        except StopIteration:
            if len(materializations) > 0:
                logger.debug("flatten_plan blocked on completion of first source in: %s", materializations)
                yield None
            else:
                return


def split(
    child_plan: InProgressPhysicalPlan[PartitionT],
    num_input_partitions: int,
    num_output_partitions: int,
) -> InProgressPhysicalPlan[PartitionT]:
    """Repartition the child_plan into more partitions by splitting partitions only. Preserves order."""

    assert (
        num_output_partitions >= num_input_partitions
    ), f"Cannot split from {num_input_partitions} to {num_output_partitions}."

    # Materialize the input partitions so we can see the number of rows and try to split evenly.
    # Splitting evenly is fairly important if this operation is to be used for parallelism.
    # (optimization TODO: don't materialize if num_rows is already available in physical plan metadata.)
    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_single_output(stage_id=stage_id)
            materializations.append(step)
        yield step

    while any(not _.done() for _ in materializations):
        logger.debug("split_to blocked on completion of all sources: %s", materializations)
        yield None

    splits_per_partition = deque([1 for _ in materializations])
    num_splits_to_apply = num_output_partitions - num_input_partitions

    # Split by rows for now.
    # In the future, maybe parameterize to allow alternatively splitting by size.
    rows_by_partitions = [task.partition_metadata().num_rows for task in materializations]

    # Calculate how to spread the required splits across all the partitions.
    # Iteratively apply a split and update how many rows would be in the resulting partitions.
    # After this loop, splits_per_partition has the final number of splits to apply to each partition.
    rows_after_splitting = [float(_) for _ in rows_by_partitions]
    for _ in range(num_splits_to_apply):
        _, split_at = max((rows, index) for (index, rows) in enumerate(rows_after_splitting))
        splits_per_partition[split_at] += 1
        rows_after_splitting[split_at] = float(rows_by_partitions[split_at] / splits_per_partition[split_at])

    # Emit the split partitions.
    for task, num_out, num_rows in zip(consume_deque(materializations), splits_per_partition, rows_by_partitions):
        if num_out == 1:
            yield PartitionTaskBuilder[PartitionT](
                inputs=[task.partition()],
                partial_metadatas=[task.partition_metadata()],
                resource_request=ResourceRequest(memory_bytes=task.partition_metadata().size_bytes),
            )
        else:
            boundaries = [math.ceil(num_rows * i / num_out) for i in range(num_out + 1)]
            starts, ends = boundaries[:-1], boundaries[1:]
            yield PartitionTaskBuilder[PartitionT](
                inputs=[task.partition()],
                partial_metadatas=[task.partition_metadata()],
                resource_request=ResourceRequest(memory_bytes=task.partition_metadata().size_bytes),
            ).add_instruction(
                instruction=execution_step.FanoutSlices(_num_outputs=num_out, slices=list(zip(starts, ends)))
            )


def coalesce(
    child_plan: InProgressPhysicalPlan[PartitionT],
    from_num_partitions: int,
    to_num_partitions: int,
) -> InProgressPhysicalPlan[PartitionT]:
    """Coalesce the results of the child_plan into fewer partitions.

    The current implementation only does partition merging, no rebalancing.
    """

    assert (
        to_num_partitions <= from_num_partitions
    ), f"Cannot coalesce upwards from {from_num_partitions} to {to_num_partitions} partitions."

    boundaries = [math.ceil((from_num_partitions / to_num_partitions) * i) for i in range(to_num_partitions + 1)]
    starts, stops = boundaries[:-1], boundaries[1:]
    # For each output partition, the number of input partitions to merge in.
    merges_per_result = deque([stop - start for start, stop in zip(starts, stops)])

    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    while True:
        # See if we can emit a coalesced partition.
        num_partitions_to_merge = merges_per_result[0]
        ready_to_coalesce = [task for task in list(materializations)[:num_partitions_to_merge] if task.done()]
        if len(ready_to_coalesce) == num_partitions_to_merge:
            # Coalesce the partition and emit it.

            # Calculate memory request for task.
            size_bytes_per_task = [task.partition_metadata().size_bytes for task in ready_to_coalesce]
            non_null_size_bytes_per_task = [size for size in size_bytes_per_task if size is not None]
            non_null_size_bytes = sum(non_null_size_bytes_per_task)
            if len(size_bytes_per_task) == len(non_null_size_bytes_per_task):
                # If all task size bytes are non-null, directly use the non-null size bytes sum.
                size_bytes = non_null_size_bytes
            elif non_null_size_bytes_per_task:
                # If some are null, calculate the non-null mean and assume that null task size bytes
                # have that size.
                mean_size = math.ceil(non_null_size_bytes / len(non_null_size_bytes_per_task))
                size_bytes = non_null_size_bytes + mean_size * (
                    len(size_bytes_per_task) - len(non_null_size_bytes_per_task)
                )
            else:
                # If all null, set to null.
                size_bytes = None

            merge_step = PartitionTaskBuilder[PartitionT](
                inputs=[_.partition() for _ in ready_to_coalesce],
                partial_metadatas=[_.partition_metadata() for _ in ready_to_coalesce],
                resource_request=ResourceRequest(memory_bytes=size_bytes),
            ).add_instruction(
                instruction=execution_step.ReduceMerge(),
            )
            [materializations.popleft() for _ in range(num_partitions_to_merge)]
            merges_per_result.popleft()
            yield merge_step

        # Cannot emit a coalesced partition.
        # Materialize a single dependency.
        try:
            child_step = next(child_plan)
            if isinstance(child_step, PartitionTaskBuilder):
                child_step = child_step.finalize_partition_task_single_output(stage_id)
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                logger.debug("coalesce blocked on completion of a task in: %s", materializations)
                yield None
            else:
                return


def reduce(
    fanout_plan: InProgressPhysicalPlan[PartitionT],
    reduce_instructions: ReduceInstruction | list[ReduceInstruction],
) -> InProgressPhysicalPlan[PartitionT]:
    """Reduce the result of fanout_plan.

    The child plan fanout_plan must produce a 2d list of partitions,
    by producing a single list in each step.

    Then, the reduce instruction is applied to each `i`th slice across the child lists.
    """

    materializations = list()
    stage_id = next(stage_id_counter)

    # Dispatch all fanouts.
    for step in fanout_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_multi_output(stage_id=stage_id)
            materializations.append(step)
        yield step

    # All fanouts dispatched. Wait for all of them to materialize
    # (since we need all of them to emit even a single reduce).
    while any(not _.done() for _ in materializations):
        logger.debug("reduce blocked on completion of all sources in: %s", materializations)
        yield None

    inputs_to_reduce = [deque(_.partitions()) for _ in materializations]
    metadatas = [deque(_.partition_metadatas()) for _ in materializations]
    del materializations
    if not isinstance(reduce_instructions, list):
        reduce_instructions = [reduce_instructions] * len(inputs_to_reduce[0])
    reduce_instructions_ = deque(reduce_instructions)
    del reduce_instructions

    # Yield all the reduces in order.
    while len(inputs_to_reduce[0]) > 0:
        partition_batch = [_.popleft() for _ in inputs_to_reduce]
        metadata_batch = [_.popleft() for _ in metadatas]
        yield PartitionTaskBuilder[PartitionT](
            inputs=partition_batch,
            partial_metadatas=metadata_batch,
            resource_request=ResourceRequest(
                memory_bytes=sum(metadata.size_bytes for metadata in metadata_batch),
            ),
        ).add_instruction(reduce_instructions_.popleft())


def sort(
    child_plan: InProgressPhysicalPlan[PartitionT],
    sort_by: ExpressionsProjection,
    descending: list[bool],
    num_partitions: int,
) -> InProgressPhysicalPlan[PartitionT]:
    """Sort the result of `child_plan` according to `sort_info`."""

    # First, materialize the child plan.
    source_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id_children = next(stage_id_counter)
    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_single_output(stage_id=stage_id_children)
            source_materializations.append(step)
        yield step

    # Sample all partitions (to be used for calculating sort boundaries).
    sample_materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id_sampling = next(stage_id_counter)

    sample_size = get_context().daft_execution_config.sample_size_for_sort
    for source in source_materializations:
        while not source.done():
            logger.debug("sort blocked on completion of source: %s", source)
            yield None

        sample = (
            PartitionTaskBuilder[PartitionT](
                inputs=[source.partition()],
                partial_metadatas=None,
            )
            .add_instruction(
                instruction=execution_step.Sample(size=sample_size, sort_by=sort_by),
            )
            .finalize_partition_task_single_output(stage_id=stage_id_sampling)
        )

        sample_materializations.append(sample)
        yield sample

    # Wait for samples to materialize.
    while any(not _.done() for _ in sample_materializations):
        logger.debug("sort blocked on completion of all samples: %s", sample_materializations)
        yield None

    stage_id_reduce = next(stage_id_counter)

    # Reduce the samples to get sort boundaries.
    boundaries = (
        PartitionTaskBuilder[PartitionT](
            inputs=[sample.partition() for sample in consume_deque(sample_materializations)],
            partial_metadatas=None,
        )
        .add_instruction(
            execution_step.ReduceToQuantiles(
                num_quantiles=num_partitions,
                sort_by=sort_by,
                descending=descending,
            ),
        )
        .finalize_partition_task_single_output(stage_id=stage_id_reduce)
    )
    yield boundaries

    # Wait for boundaries to materialize.
    while not boundaries.done():
        logger.debug("sort blocked on completion of boundary partition: %s", boundaries)
        yield None

    # Create a range fanout plan.
    range_fanout_plan = (
        PartitionTaskBuilder[PartitionT](
            inputs=[boundaries.partition(), source.partition()],
            partial_metadatas=[boundaries.partition_metadata(), source.partition_metadata()],
            resource_request=ResourceRequest(
                memory_bytes=source.partition_metadata().size_bytes,
            ),
        ).add_instruction(
            instruction=execution_step.FanoutRange[PartitionT](
                _num_outputs=num_partitions,
                sort_by=sort_by,
                descending=descending,
            ),
        )
        for source in consume_deque(source_materializations)
    )
    per_partition_bounds = _to_per_partition_bounds(boundaries.vpartition(), num_partitions)

    # Execute a sorting reduce on it.
    yield from reduce(
        fanout_plan=range_fanout_plan,
        reduce_instructions=[
            execution_step.ReduceMergeAndSort(
                sort_by=sort_by,
                descending=descending,
                bounds=per_part_boundaries,
            )
            for per_part_boundaries in per_partition_bounds
        ],
    )


def fanout_random(child_plan: InProgressPhysicalPlan[PartitionT], num_partitions: int):
    """Splits the results of `child_plan` randomly into a list of `node.num_partitions()` number of partitions"""
    seed = 0
    for step in child_plan:
        if isinstance(step, PartitionTaskBuilder):
            instruction = execution_step.FanoutRandom(num_partitions, seed)
            step = step.add_instruction(instruction)
        yield step
        seed += 1


def _best_effort_next_step(
    stage_id: int, child_plan: InProgressPhysicalPlan[PartitionT]
) -> tuple[PartitionTask[PartitionT] | None, bool]:
    """Performs a best-effort attempt at retrieving the next step from a child plan

    Returns None in cases where there is nothing to run, or the plan has been exhausted.

    Returns:
        step: the step (potentially None) to run
        is_final_task: a boolean indicating whether or not this step was a final step
    """
    try:
        step = next(child_plan)
    except StopIteration:
        return (None, False)
    else:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_single_output(stage_id=stage_id)
            return (step, True)
        elif isinstance(step, PartitionTask):
            return (step, False)
        else:
            return (None, False)


class Materialize:
    """Materialize the child plan.

    Repeatedly yields either a PartitionTask (to produce an intermediate partition)
    or a PartitionT (which is part of the final result).
    """

    def __init__(
        self,
        child_plan: InProgressPhysicalPlan[PartitionT],
        results_buffer_size: int | None,
    ):
        self.child_plan = child_plan
        self.materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
        self.results_buffer_size = results_buffer_size

    def __iter__(self) -> MaterializedPhysicalPlan:
        num_materialized_yielded = 0
        num_intermediate_yielded = 0
        num_final_yielded = 0
        stage_id = next(stage_id_counter)

        logger.debug(
            "[plan-%s] Starting to emit tasks from `materialize` with results_buffer_size=%s",
            stage_id,
            self.results_buffer_size,
        )

        while True:
            # If any inputs have finished executing, we want to drain the `materializations` buffer
            while len(self.materializations) > 0 and self.materializations[0].done():
                # Make space on buffer by popping the task that was done
                done_task = self.materializations.popleft()

                # Best-effort attempt to yield new work and fill up the buffer to the desired `results_buffer_size`
                if self.results_buffer_size is not None:
                    for _ in range(self.results_buffer_size - len(self.materializations)):
                        best_effort_step, is_final_task = _best_effort_next_step(stage_id, self.child_plan)
                        if best_effort_step is None:
                            break
                        elif is_final_task:
                            assert isinstance(best_effort_step, SingleOutputPartitionTask)
                            self.materializations.append(best_effort_step)
                            num_final_yielded += 1
                            logger.debug(
                                "[plan-%s] YIELDING final task to replace done materialized task (%s so far)",
                                stage_id,
                                num_final_yielded,
                            )
                        else:
                            num_intermediate_yielded += 1
                            logger.debug(
                                "[plan-%s] YIELDING an intermediate task to replace done materialized task (%s so far)",
                                stage_id,
                                num_intermediate_yielded,
                            )
                        yield best_effort_step

                # Yield the task that was done
                num_materialized_yielded += 1
                logger.debug("[plan-%s] YIELDING a materialized task (%s so far)", stage_id, num_materialized_yielded)
                yield done_task.result()

            # If the buffer has too many results already, we yield None until some are completed
            if self.results_buffer_size is not None and len(self.materializations) >= self.results_buffer_size:
                logger.debug(
                    "[plan-%s] YIELDING none, waiting on tasks in buffer to complete: %s in buffer, but maximum is %s",
                    stage_id,
                    len(self.materializations),
                    self.results_buffer_size,
                )
                yield None

                # Important: start again at the top and drain materialized results
                # Otherwise it may lead to a weird corner-case where the plan has ended (raising StopIteration)
                # but some of the completed materializations haven't been drained from the buffer.
                continue

            # Materialize a single dependency.
            try:
                step = next(self.child_plan)
                if isinstance(step, PartitionTaskBuilder):
                    step = step.finalize_partition_task_single_output(stage_id=stage_id)
                    self.materializations.append(step)
                    num_final_yielded += 1
                    logger.debug("[plan-%s] YIELDING final task (%s so far)", stage_id, num_final_yielded)
                elif isinstance(step, PartitionTask):
                    num_intermediate_yielded += 1
                    logger.debug(
                        "[plan-%s] YIELDING an intermediate task (%s so far)", stage_id, num_intermediate_yielded
                    )

                assert isinstance(step, (PartitionTask, type(None)))
                yield step

            except StopIteration:
                if len(self.materializations) > 0:
                    logger.debug(
                        "[plan-%s] YIELDING none, iterator completed but materialize is blocked on completion of all sources: %s",
                        stage_id,
                        self.materializations,
                    )
                    yield None
                else:
                    return


def enumerate_open_executions(
    schedule: InProgressPhysicalPlan[PartitionT],
) -> Iterator[tuple[int, None | PartitionTask[PartitionT] | PartitionTaskBuilder[PartitionT]]]:
    """Helper. Like enumerate() on an iterator, but only counts up if the result is an PartitionTaskBuilder.

    Intended for counting the number of PartitionTaskBuilders returned by the iterator.
    """
    index = 0
    for item in schedule:
        if isinstance(item, PartitionTaskBuilder):
            yield index, item
            index += 1
        else:
            yield index, item


def consume_deque(dq: deque[T]) -> Iterator[T]:
    while len(dq) > 0:
        yield dq.popleft()
