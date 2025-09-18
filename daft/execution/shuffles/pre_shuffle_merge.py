from __future__ import annotations

import logging
from collections import defaultdict

import ray.experimental  # noqa: TID253

from daft.daft import ResourceRequest
from daft.execution import execution_step
from daft.execution.execution_step import (
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter

logger = logging.getLogger(__name__)


def pre_shuffle_merge(
    map_plan: InProgressPhysicalPlan[ray.ObjectRef],
    pre_shuffle_merge_threshold: int,
) -> InProgressPhysicalPlan[ray.ObjectRef]:
    """Merges intermediate partitions from the map_plan based on memory constraints.

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
    in_flight_maps: dict[str, SingleOutputPartitionTask[ray.ObjectRef]] = {}
    no_more_input = False

    while True:
        # Get materialized maps by size
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
            # Get location information for all materialized partitions
            partitions = [m[0].result().partition() for m in materialized_maps]
            location_map = ray.experimental.get_object_locations(partitions)

            # Group partitions by node
            node_groups: dict[str, list[tuple[SingleOutputPartitionTask[ray.ObjectRef], int]]] = defaultdict(list)
            unknown_location_group: list[
                tuple[SingleOutputPartitionTask[ray.ObjectRef], int]
            ] = []  # Special group for partitions without known location

            for partition, size in materialized_maps:
                partition_ref = partition.partition()
                location_info = location_map.get(partition_ref, {})

                if not location_info or "node_ids" not in location_info or not location_info["node_ids"]:
                    unknown_location_group.append((partition, size))
                else:
                    # TODO: Handle multiple locations, with a strategy to select more optimal nodes, e.g. based on memory
                    node_id = location_info["node_ids"][0]  # Use first node if multiple locations exist
                    node_groups[node_id].append((partition, size))

            # Function to create merge groups for a list of partitions
            def create_merge_groups(
                partitions_list: list[tuple[SingleOutputPartitionTask[ray.ObjectRef], int]],
            ) -> list[list[SingleOutputPartitionTask[ray.ObjectRef]]]:
                if not partitions_list:
                    return []

                groups = []
                current_group = [partitions_list[0][0]]
                current_size = partitions_list[0][1]

                for partition, size in partitions_list[1:]:
                    if current_size + size > pre_shuffle_merge_threshold:
                        groups.append(current_group)
                        current_group = [partition]
                        current_size = size
                    else:
                        current_group.append(partition)
                        current_size += size

                # Add the last group if it exists and is either:
                # 1. Contains more than 1 partition
                # 2. Is the last group and we're done with input
                # 3. The partition exceeds the memory threshold
                should_add_last_group = (
                    len(current_group) > 1 or done_with_input or current_size > pre_shuffle_merge_threshold
                )
                if current_group and should_add_last_group:
                    groups.append(current_group)

                return groups

            # Process each node's partitions and unknown location partitions
            merge_groups: dict[str | None, list[list[SingleOutputPartitionTask[ray.ObjectRef]]]] = {}

            # Process node-specific groups
            for node_id, node_partitions in node_groups.items():
                merge_groups[node_id] = create_merge_groups(node_partitions)

            # Process unknown location group
            merge_groups[None] = create_merge_groups(unknown_location_group)

            # Create merge steps and remove processed maps
            for node_id, groups in merge_groups.items():
                # Remove processed maps from in_flight_maps
                for group in groups:
                    for partition in group:
                        del in_flight_maps[partition.id()]
                    total_size = sum(m.partition_metadata().size_bytes or 0 for m in group)
                    merge_step = PartitionTaskBuilder[ray.ObjectRef](
                        inputs=[p.partition() for p in group],
                        partial_metadatas=[m.partition_metadata() for m in group],
                        resource_request=ResourceRequest(memory_bytes=total_size),
                        node_id=node_id,
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
