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

import logging
import math
import pathlib
from collections import deque
from typing import Generator, Iterator, TypeVar, Union

from daft.daft import (
    FileFormat,
    FileFormatConfig,
    IOConfig,
    JoinType,
    ResourceRequest,
    StorageConfig,
)
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
    PartialPartitionMetadata,
    PartitionT,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
    partitions: Iterator[PartitionT], metadatas: Iterator[PartialPartitionMetadata] | None = None
) -> InProgressPhysicalPlan[PartitionT]:
    """Instantiate a (no-op) physical plan from existing partitions."""
    if metadatas is None:
        # Iterator of empty metadatas.
        metadatas = (PartialPartitionMetadata(num_rows=None, size_bytes=None) for _ in iter(int, 1))

    yield from (
        PartitionTaskBuilder[PartitionT](inputs=[partition], partial_metadatas=[metadata])
        for partition, metadata in zip(partitions, metadatas)
    )


def file_read(
    child_plan: InProgressPhysicalPlan[PartitionT],
    # Max number of rows to read.
    limit_rows: int | None,
    schema: Schema,
    storage_config: StorageConfig,
    columns_to_read: list[str] | None,
    file_format_config: FileFormatConfig,
) -> InProgressPhysicalPlan[PartitionT]:
    """child_plan represents partitions with filenames.

    Yield a plan to read those filenames.
    """
    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    output_partition_index = 0

    while True:
        # Check if any inputs finished executing.
        while len(materializations) > 0 and materializations[0].done():
            done_task = materializations.popleft()

            vpartition = done_task.vpartition()
            file_infos = vpartition.to_pydict()
            file_sizes_bytes = file_infos["size"]
            file_rows = file_infos["num_rows"]

            # Emit one partition for each file (NOTE: hardcoded for now).
            for i in range(len(vpartition)):
                file_read_step = PartitionTaskBuilder[PartitionT](
                    inputs=[done_task.partition()],
                    partial_metadatas=None,  # Child's metadata doesn't really matter for a file read
                ).add_instruction(
                    instruction=execution_step.ReadFile(
                        index=i,
                        file_rows=file_rows[i],
                        limit_rows=limit_rows,
                        schema=schema,
                        storage_config=storage_config,
                        columns_to_read=columns_to_read,
                        file_format_config=file_format_config,
                    ),
                    # Set the filesize as the memory request.
                    # (Note: this is very conservative; file readers empirically use much more peak memory than 1x file size.)
                    resource_request=ResourceRequest(memory_bytes=file_sizes_bytes[i]),
                )
                yield file_read_step
                output_partition_index += 1

        # Materialize a single dependency.
        try:
            child_step = next(child_plan)
            if isinstance(child_step, PartitionTaskBuilder):
                child_step = child_step.finalize_partition_task_single_output(stage_id=stage_id)
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                logger.debug("file_read blocked on completion of first source in: %s", materializations)
                yield None
            else:
                return


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


def hash_join(
    left_plan: InProgressPhysicalPlan[PartitionT],
    right_plan: InProgressPhysicalPlan[PartitionT],
    left_on: ExpressionsProjection,
    right_on: ExpressionsProjection,
    how: JoinType,
) -> InProgressPhysicalPlan[PartitionT]:
    """Pairwise join the partitions from `left_child_plan` and `right_child_plan` together."""

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
                instruction=execution_step.Join(
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


def _create_join_step(
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
        instruction=execution_step.Join(
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
        receiver_parts: deque[SingleOutputPartitionTask[PartitionT]] = deque()
        # Moved completed partition tasks in the receiver side of the join to the materialized partition set.
        while receiver_requests and receiver_requests[0].done():
            receiver_parts.append(receiver_requests.popleft())

        # Emit join steps for newly materialized partitions.
        # Broadcast all broadcaster partitions to each new receiver partition that was materialized on this dispatch loop.
        for receiver_part in receiver_parts:
            yield _create_join_step(broadcaster_parts, receiver_part, left_on, right_on, how, is_swapped)

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

    # Emit the splitted partitions.
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
    reduce_instruction: ReduceInstruction,
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
        ).add_instruction(reduce_instruction)


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
                instruction=execution_step.Sample(sort_by=sort_by),
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

    # Execute a sorting reduce on it.
    yield from reduce(
        fanout_plan=range_fanout_plan,
        reduce_instruction=execution_step.ReduceMergeAndSort(
            sort_by=sort_by,
            descending=descending,
        ),
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


def materialize(
    child_plan: InProgressPhysicalPlan[PartitionT],
) -> MaterializedPhysicalPlan:
    """Materialize the child plan.

    Repeatedly yields either a PartitionTask (to produce an intermediate partition)
    or a PartitionT (which is part of the final result).
    """

    materializations: deque[SingleOutputPartitionTask[PartitionT]] = deque()
    stage_id = next(stage_id_counter)
    while True:
        # Check if any inputs finished executing.
        while len(materializations) > 0 and materializations[0].done():
            done_task = materializations.popleft()
            yield done_task.result()

        # Materialize a single dependency.
        try:
            step = next(child_plan)
            if isinstance(step, PartitionTaskBuilder):
                step = step.finalize_partition_task_single_output(stage_id=stage_id)
                materializations.append(step)
            assert isinstance(step, (PartitionTask, type(None)))

            yield step

        except StopIteration:
            if len(materializations) > 0:
                logger.debug("materialize blocked on completion of all sources: %s", materializations)
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
