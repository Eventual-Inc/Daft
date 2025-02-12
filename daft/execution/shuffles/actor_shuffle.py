from __future__ import annotations
import logging

from daft.daft import ResourceRequest
from daft.execution import execution_step
from daft.execution.physical_plan import InProgressPhysicalPlan
from daft.runners.partitioning import PartitionT
from daft.runners.ray_runner import DaftActor, _ray_num_cpus_provider
from daft.execution.physical_plan import (
    InProgressPhysicalPlan,
    PartitionTaskBuilder,
    PartitionT,
    stage_id_counter,
)
from collections import deque

logger = logging.getLogger(__name__)

DAFT_ACTORS = []


def actor_shuffle(
    fanout_plan: InProgressPhysicalPlan[PartitionT],
) -> InProgressPhysicalPlan[PartitionT]:

    """Reduce the result of fanout_plan.

    The child plan fanout_plan must produce a 2d list of partitions,
    by producing a single list in each step.

    Then, the reduce instruction is applied to each `i`th slice across the child lists.
    """
    global DAFT_ACTORS

    reduce_instructions = execution_step.ReduceMerge()
    materializations = list()
    stage_id = next(stage_id_counter)

    # Dispatch all fanouts.
    for step in fanout_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_multi_output(stage_id=stage_id)
            materializations.append(step)
        yield step

    if len(DAFT_ACTORS) == 0:
        num_cpus_provider = _ray_num_cpus_provider()
        DAFT_ACTORS = [
            DaftActor.options(scheduling_strategy="SPREAD").remote()
            for _ in range(next(num_cpus_provider))
        ]

    # All fanouts dispatched. Wait for all of them to materialize
    # (since we need all of them to emit even a single reduce).
    while any(not _._results for _ in materializations):
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
    next_actor_idx = 0
    while len(inputs_to_reduce[0]) > 0:
        partition_batch = [_.popleft() for _ in inputs_to_reduce]
        metadata_batch = [_.popleft() for _ in metadatas]
        yield PartitionTaskBuilder[PartitionT](
            inputs=partition_batch,
            partial_metadatas=metadata_batch,
            resource_request=ResourceRequest(
                memory_bytes=sum(metadata.size_bytes for metadata in metadata_batch),
            ),
            actor=DAFT_ACTORS[next_actor_idx],
        ).add_instruction(reduce_instructions_.popleft())
        next_actor_idx = (next_actor_idx + 1) % len(DAFT_ACTORS)
