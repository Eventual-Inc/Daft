import logging
from typing import Dict

from daft.daft import ResourceRequest
from daft.execution import execution_step
from daft.execution.execution_step import (
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.runners.partitioning import (
    PartitionT,
)

logger = logging.getLogger(__name__)


def pre_shuffle_merge(
    map_plan: InProgressPhysicalPlan[PartitionT],
    pre_shuffle_merge_threshold: int,
) -> InProgressPhysicalPlan[PartitionT]:
    """
    Merges intermediate partitions from the map_plan based on memory constraints.

    The function processes incoming map tasks and merges their outputs when:
    1. There are multiple materialized maps available
    2. All remaining maps are materialized and no more input is expected

    Args:
        map_plan: Iterator of partition tasks to be merged

    Yields:
        Merged partition tasks or processed child steps
    """

    NUM_MAPS_THRESHOLD = 4

    stage_id = next(stage_id_counter)
    in_flight_maps: Dict[str, SingleOutputPartitionTask[PartitionT]] = {}
    no_more_input = False

    while True:
        # Get and sort materialized maps by size.
        materialized_maps = sorted(
            [
                (
                    p,
                    p.partition_metadata().size_bytes or pre_shuffle_merge_threshold + 1,
                )  # Assume large size if unknown
                for p in in_flight_maps.values()
                if p.done()
            ],
            key=lambda x: x[1],
        )

        # Try to merge materialized maps if we have enough (default to 4) or if we're done with input
        enough_maps = len(materialized_maps) > NUM_MAPS_THRESHOLD
        done_with_input = no_more_input and len(materialized_maps) == len(in_flight_maps)

        if enough_maps or done_with_input:
            # Initialize the first merge group
            merge_groups = []
            current_group = [materialized_maps[0][0]]
            current_size = materialized_maps[0][1]

            # Group remaining maps based on memory threshold
            for partition, size in materialized_maps[1:]:
                if current_size + size > pre_shuffle_merge_threshold:
                    merge_groups.append(current_group)
                    current_group = [partition]
                    current_size = size
                else:
                    current_group.append(partition)
                    current_size += size

            # Add the last group if it exists and is either:
            # 1. Contains more than 1 partition
            # 2. Is the last group and we're done with input
            # 3. The partition exceeds the memory threshold
            if current_group:
                if len(current_group) > 1 or done_with_input or current_size > pre_shuffle_merge_threshold:
                    merge_groups.append(current_group)

            # Create merge steps and remove processed maps
            for group in merge_groups:
                # Remove processed maps from in_flight_maps
                for partition in group:
                    del in_flight_maps[partition.id()]

                total_size = sum(m.partition_metadata().size_bytes or 0 for m in group)
                merge_step = PartitionTaskBuilder[PartitionT](
                    inputs=[p.partition() for p in group],
                    partial_metadatas=[m.partition_metadata() for m in group],
                    resource_request=ResourceRequest(memory_bytes=total_size),
                ).add_instruction(instruction=execution_step.ReduceMerge())
                yield merge_step

        # Process next map task if available
        try:
            child_step = next(map_plan)
            if isinstance(child_step, PartitionTaskBuilder):
                child_step = child_step.finalize_partition_task_single_output(stage_id=stage_id)
                in_flight_maps[child_step.id()] = child_step
            yield child_step

        except StopIteration:
            no_more_input = True
            if in_flight_maps:
                logger.debug("PreShuffleMerge blocked on completion of a map task")
                yield None
            else:
                break
