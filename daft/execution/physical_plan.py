"""
This file contains physical plan building blocks.
To get a physical plan for a logical plan, see physical_plan_factory.py.

Conceptually, a physical plan decides what steps, and the order of steps, to run to build some target.
Physical plans are closely related to logical plans. A logical plan describes "what you want", and a physical plan figures out "what to do" to get it.
They are not exact analogues, especially due to the ability of a physical plan to dynamically decide what to do next.

Physical plans are implemented here as an iterator of ExecutionStep | None.
When a physical plan returns None, it means it cannot tell you what the next step is,
because it is waiting for the result of a previous ExecutionStep to can decide what to do next.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Generator, Iterator, List, TypeVar, Union

from daft.execution import execution_step
from daft.execution.execution_step import (
    ExecutionStep,
    ExecutionStepBuilder,
    Instruction,
    ReduceInstruction,
    SingleOutputExecutionStep,
)
from daft.logical import logical_plan
from daft.resource_request import ResourceRequest

PartitionT = TypeVar("PartitionT")
T = TypeVar("T")


# A PhysicalPlan that is still being built - may yield both ExecutionStepBuilders and ExecutionSteps.
InProgressPhysicalPlan = Iterator[Union[None, ExecutionStep[PartitionT], ExecutionStepBuilder[PartitionT]]]

# A PhysicalPlan that is complete and will only yield ExecutionSteps.
MaterializedPhysicalPlan = Generator[
    Union[None, ExecutionStep[PartitionT]],
    None,
    List[PartitionT],
]


def partition_read(partitions: Iterator[PartitionT]) -> InProgressPhysicalPlan[PartitionT]:
    """Instantiate a (no-op) physical plan from existing partitions."""
    yield from (ExecutionStepBuilder[PartitionT](inputs=[partition]) for partition in partitions)


def file_read(
    child_plan: InProgressPhysicalPlan[PartitionT],
    scan_info: logical_plan.TabularFilesScan,
) -> InProgressPhysicalPlan[PartitionT]:
    """child_plan represents partitions with filenames.

    Yield a plan to read those filenames.
    """

    materializations: deque[SingleOutputExecutionStep[PartitionT]] = deque()
    output_partition_index = 0

    while True:
        # Check if any inputs finished executing.
        while len(materializations) > 0 and materializations[0].result is not None:
            result = materializations.popleft().result
            assert result is not None  # for mypy only

            vpartition = result.vpartition()
            file_sizes_bytes = vpartition.to_pydict()["size"]

            # Emit one partition for each file (NOTE: hardcoded for now).
            for i in range(vpartition.metadata().num_rows):

                file_read_step = ExecutionStepBuilder[PartitionT](inputs=[result.partition()]).add_instruction(
                    instruction=execution_step.ReadFile(
                        partition_id=output_partition_index,
                        logplan=scan_info,
                        index=i,
                    ),
                    resource_request=ResourceRequest(memory_bytes=file_sizes_bytes[i]),
                )
                yield file_read_step
                output_partition_index += 1

        # Materialize a single dependency.
        try:
            child_step = next(child_plan)
            if isinstance(child_step, ExecutionStepBuilder):
                child_step = child_step.build_materialization_request_single()
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def file_write(
    child_plan: InProgressPhysicalPlan[PartitionT],
    write_info: logical_plan.FileWrite,
) -> InProgressPhysicalPlan[PartitionT]:
    """Write the results of `child_plan` into files described by `write_info`."""

    yield from (
        step.add_instruction(
            execution_step.WriteFile(partition_id=index, logplan=write_info),
        )
        if isinstance(step, ExecutionStepBuilder)
        else step
        for index, step in enumerate_open_executions(child_plan)
    )


def pipeline_instruction(
    child_plan: InProgressPhysicalPlan[PartitionT],
    pipeable_instruction: Instruction,
    resource_request: execution_step.ResourceRequest,
) -> InProgressPhysicalPlan[PartitionT]:
    """Apply an instruction to the results of `child_plan`."""

    yield from (
        step.add_instruction(pipeable_instruction, resource_request) if isinstance(step, ExecutionStepBuilder) else step
        for step in child_plan
    )


def join(
    left_plan: InProgressPhysicalPlan[PartitionT],
    right_plan: InProgressPhysicalPlan[PartitionT],
    join: logical_plan.Join,
) -> InProgressPhysicalPlan[PartitionT]:
    """Pairwise join the partitions from `left_child_plan` and `right_child_plan` together."""

    # Materialize the steps from the left and right sources to get partitions.
    # As the materializations complete, emit new steps to join each left and right partition.
    left_requests: deque[SingleOutputExecutionStep[PartitionT]] = deque()
    right_requests: deque[SingleOutputExecutionStep[PartitionT]] = deque()

    while True:
        # Emit new join steps if we have left and right partitions ready.
        while (
            len(left_requests) > 0
            and len(right_requests) > 0
            and left_requests[0].result is not None
            and right_requests[0].result is not None
        ):
            next_left = left_requests.popleft()
            next_right = right_requests.popleft()
            assert next_left.result is not None  # for mypy only; guaranteed by while condition
            assert next_right.result is not None  # for mypy only; guaranteed by while condition

            join_step = ExecutionStepBuilder[PartitionT](
                inputs=[next_left.result.partition(), next_right.result.partition()]
            ).add_instruction(instruction=execution_step.Join(join))
            yield join_step

        # Exhausted all ready inputs; execute a single child step to get more join inputs.
        # Choose whether to execute from left child or right child (whichever one is more behind),
        if len(left_requests) <= len(right_requests):
            next_plan, next_requests = left_plan, left_requests
        else:
            next_plan, next_requests = right_plan, right_requests

        try:
            step = next(next_plan)
            if isinstance(step, ExecutionStepBuilder):
                step = step.build_materialization_request_single()
                next_requests.append(step)
            yield step

        except StopIteration:
            # Left and right child plans have completed.
            # Are we still waiting for materializations to complete? (We will emit more joins from them).
            if len(left_requests) + len(right_requests) > 0:
                yield None

            # Otherwise, we are entirely done.
            else:
                return


def local_limit(
    child_plan: InProgressPhysicalPlan[PartitionT],
    limit: int,
) -> Generator[None | ExecutionStep[PartitionT] | ExecutionStepBuilder[PartitionT], int, None]:
    """Apply a limit instruction to each partition in the child_plan.

    limit:
        The value of the limit to apply to each partition.

    Yields: ExecutionStep with the limit applied.
    Send back: A new value to the limit (optional). This allows you to update the limit after each partition if desired.
    """
    for step in child_plan:
        if not isinstance(step, ExecutionStepBuilder):
            yield step
        else:
            maybe_new_limit = yield step.add_instruction(
                execution_step.LocalLimit(limit),
            )
            if maybe_new_limit is not None:
                limit = maybe_new_limit


def global_limit(
    child_plan: InProgressPhysicalPlan[PartitionT],
    global_limit: logical_plan.GlobalLimit,
) -> InProgressPhysicalPlan[PartitionT]:
    """Return the first n rows from the `child_plan`."""

    remaining_rows = global_limit._num
    assert remaining_rows >= 0, f"Invalid value for limit: {remaining_rows}"
    remaining_partitions = global_limit.num_partitions()

    materializations: deque[SingleOutputExecutionStep[PartitionT]] = deque()

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
        while len(materializations) > 0 and materializations[0].result is not None:
            result = materializations.popleft().result
            assert result is not None  # for mypy only

            limit = remaining_rows and min(remaining_rows, result.metadata().num_rows)

            global_limit_step = ExecutionStepBuilder[PartitionT](inputs=[result.partition()]).add_instruction(
                instruction=execution_step.LocalLimit(limit),
            )
            yield global_limit_step
            remaining_partitions -= 1
            remaining_rows -= limit

            if remaining_rows == 0:
                # We only need to return empty partitions now.
                # Instead of computing new ones and applying limit(0),
                # we can just reuse an existing computed partition.

                # Cancel all remaining results; we won't need them.
                for _ in range(len(materializations)):
                    result_to_cancel = materializations.pop().result
                    if result_to_cancel is not None:
                        result_to_cancel.cancel()

                yield from (
                    ExecutionStepBuilder[PartitionT](inputs=[result.partition()]).add_instruction(
                        instruction=execution_step.LocalLimit(0),
                    )
                    for _ in range(remaining_partitions)
                )
                return

        # (Optimization. If we are doing limit(0) and already have a partition executing to use for it, just wait.)
        if remaining_rows == 0 and len(materializations) > 0:
            yield None
            continue

        # Execute a single child partition.
        try:
            child_step = child_plan.send(remaining_rows) if started else next(child_plan)
            started = True
            if isinstance(child_step, ExecutionStepBuilder):
                child_step = child_step.build_materialization_request_single()
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def coalesce(
    child_plan: InProgressPhysicalPlan[PartitionT],
    coalesce: logical_plan.Coalesce,
) -> InProgressPhysicalPlan[PartitionT]:
    """Coalesce the results of the child_plan into fewer partitions.

    The current implementation only does partition merging, no rebalancing.
    """

    coalesce_from = coalesce._children()[0].num_partitions()
    coalesce_to = coalesce.num_partitions()
    assert coalesce_to <= coalesce_from, f"Cannot coalesce upwards from {coalesce_from} to {coalesce_to} partitions."

    starts = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(coalesce_to)]
    stops = [math.ceil((coalesce_from / coalesce_to) * i) for i in range(1, coalesce_to + 1)]
    # For each output partition, the number of input partitions to merge in.
    merges_per_result = deque([stop - start for start, stop in zip(starts, stops)])

    materializations: deque[SingleOutputExecutionStep[PartitionT]] = deque()

    while True:

        # See if we can emit a coalesced partition.
        num_partitions_to_merge = merges_per_result[0]
        if len(materializations) >= num_partitions_to_merge:
            ready_to_coalesce = [
                materializations[i].result
                for i in range(num_partitions_to_merge)
                if materializations[i].result is not None
            ]
            if len(ready_to_coalesce) == num_partitions_to_merge:
                # Coalesce the partition and emit it.
                merge_step = ExecutionStepBuilder[PartitionT](
                    inputs=[_.partition() for _ in ready_to_coalesce]
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
            if isinstance(child_step, ExecutionStepBuilder):
                child_step = child_step.build_materialization_request_single()
                materializations.append(child_step)
            yield child_step

        except StopIteration:
            if len(materializations) > 0:
                yield None
            else:
                return


def reduce(
    fanout_plan: InProgressPhysicalPlan[PartitionT],
    num_partitions: int,
    reduce_instruction: ReduceInstruction,
) -> InProgressPhysicalPlan[PartitionT]:
    """Reduce the result of fanout_plan.

    The child plan fanout_plan must produce a 2d list of partitions,
    by producing a single list in each step.

    Then, the reduce instruction is applied to each `i`th slice across the child lists.
    """

    materializations = list()

    # Dispatch all fanouts.
    for step in fanout_plan:
        if isinstance(step, ExecutionStepBuilder):
            step = step.build_materialization_request_multi(num_partitions)
            materializations.append(step)
        yield step

    # All fanouts dispatched. Wait for all of them to materialize
    # (since we need all of them to emit even a single reduce).
    while any(_.results is None for _ in materializations):
        yield None

    inputs_to_reduce = [deque(_.results) for _ in materializations if _.results is not None]
    del materializations

    # Yield all the reduces in order.
    while len(inputs_to_reduce[0]) > 0:
        yield ExecutionStepBuilder[PartitionT](
            inputs=[result.partition() for result in (_.popleft() for _ in inputs_to_reduce)],
            instructions=[reduce_instruction],
        )


def sort(
    child_plan: InProgressPhysicalPlan[PartitionT],
    sort_info: logical_plan.Sort,
) -> InProgressPhysicalPlan[PartitionT]:
    """Sort the result of `child_plan` according to `sort_info`."""

    # First, materialize the child plan.
    source_materializations: deque[SingleOutputExecutionStep[PartitionT]] = deque()
    for step in child_plan:
        if isinstance(step, ExecutionStepBuilder):
            step = step.build_materialization_request_single()
            source_materializations.append(step)
        yield step

    # Sample all partitions (to be used for calculating sort boundaries).
    sample_materializations: deque[SingleOutputExecutionStep[PartitionT]] = deque()
    for source in source_materializations:
        while source.result is None:
            yield None
        sample = (
            ExecutionStepBuilder[PartitionT](inputs=[source.result.partition()])
            .add_instruction(
                instruction=execution_step.Sample(sort_by=sort_info._sort_by),
            )
            .build_materialization_request_single()
        )
        sample_materializations.append(sample)
        yield sample

    # Wait for samples to materialize.
    while any(_.result is None for _ in sample_materializations):
        yield None

    # Reduce the samples to get sort boundaries.
    boundaries = (
        ExecutionStepBuilder[PartitionT](
            inputs=[
                sample.result.partition()
                for sample in consume_deque(sample_materializations)
                if sample.result is not None  # for mypy only; guaranteed to be not None by while loop
            ]
        )
        .add_instruction(
            execution_step.ReduceToQuantiles(
                num_quantiles=sort_info.num_partitions(),
                sort_by=sort_info._sort_by,
                descending=sort_info._descending,
            ),
        )
        .build_materialization_request_single()
    )
    yield boundaries

    # Wait for boundaries to materialize.
    while boundaries.result is None:
        yield None

    boundaries_partition = boundaries.result.partition()

    # Create a range fanout plan.
    range_fanout_plan = (
        ExecutionStepBuilder[PartitionT](inputs=[boundaries_partition, source_partition]).add_instruction(
            instruction=execution_step.FanoutRange[PartitionT](
                num_outputs=sort_info.num_partitions(),
                sort_by=sort_info._sort_by,
                descending=sort_info._descending,
            ),
        )
        for source_partition in (
            source.result.partition() for source in consume_deque(source_materializations) if source.result is not None
        )
    )

    # Execute a sorting reduce on it.
    yield from reduce(
        fanout_plan=range_fanout_plan,
        num_partitions=sort_info.num_partitions(),
        reduce_instruction=execution_step.ReduceMergeAndSort(
            sort_by=sort_info._sort_by,
            descending=sort_info._descending,
        ),
    )


def materialize(
    child_plan: InProgressPhysicalPlan[PartitionT],
) -> MaterializedPhysicalPlan:
    """Materialize the child plan.

    Returns (via generator return): the completed plan's result partitions.
    """

    materializations = list()

    for step in child_plan:
        if isinstance(step, ExecutionStepBuilder):
            step = step.build_materialization_request_single()
            materializations.append(step)
        assert isinstance(step, (ExecutionStep, type(None)))

        yield step

    while any(_.result is None for _ in materializations):
        yield None

    return [_.result.partition() for _ in materializations]


def enumerate_open_executions(
    schedule: InProgressPhysicalPlan[PartitionT],
) -> Iterator[tuple[int, None | ExecutionStep[PartitionT] | ExecutionStepBuilder[PartitionT]]]:
    """Helper. Like enumerate() on an iterator, but only counts up if the result is an ExecutionStepBuilder.

    Intended for counting the number of ExecutionStepBuilders returned by the iterator.
    """
    index = 0
    for item in schedule:
        if isinstance(item, ExecutionStepBuilder):
            yield index, item
            index += 1
        else:
            yield index, item


def consume_deque(dq: deque[T]) -> Iterator[T]:
    while len(dq) > 0:
        yield dq.popleft()
