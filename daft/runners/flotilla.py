from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.daft import (
    DistributedPhysicalPlan,
    DistributedPhysicalPlanRunner,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    RayPartitionRef,
    RaySwordfishTask,
    RaySwordfishWorker,
    set_compute_runtime_num_worker_threads,
)
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.constants import (
    MAX_SWORDFISH_ACTOR_RESTARTS,
    MAX_SWORDFISH_ACTOR_TASK_RETRIES,
)
from daft.runners.partitioning import (
    PartitionMetadata,
    PartitionSet,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from daft.runners.ray_runner import RayMaterializedResult

try:
    import ray
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote(
    max_restarts=MAX_SWORDFISH_ACTOR_RESTARTS,
    max_task_retries=MAX_SWORDFISH_ACTOR_TASK_RETRIES,
)
class RaySwordfishActor:
    """RaySwordfishActor is a ray actor that runs local physical plans on swordfish.

    It is a stateless, async actor, and can run multiple plans concurrently and is able to retry itself and it's tasks.
    """

    def __init__(self, num_worker_threads: int) -> None:
        # Configure the number of worker threads for swordfish, according to the number of CPUs visible to ray.
        set_compute_runtime_num_worker_threads(num_worker_threads)
        self.native_executor = NativeExecutor()

    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        config: PyDaftExecutionConfig,
        psets: dict[str, list[ray.ObjectRef]],
        context: dict[str, str] | None,
    ) -> AsyncGenerator[MicroPartition | list[PartitionMetadata], None]:
        """Run a plan on swordfish and yield partitions."""
        psets = {k: await asyncio.gather(*v) for k, v in psets.items()}
        psets_mp = {k: [v._micropartition for v in v] for k, v in psets.items()}

        metas = []
        async for partition in self.native_executor.run_async(plan, psets_mp, config, None, context):
            if partition is None:
                break
            mp = MicroPartition._from_pymicropartition(partition)
            metas.append(PartitionMetadata.from_table(mp))
            yield mp
        yield metas


@dataclass
class RaySwordfishTaskHandle:
    """RaySwordfishTaskHandle is a handle to a task that is running on a swordfish actor.

    It is used to asynchronously get the result of the task, cancel the task, and perform any post-task cleanup.
    """

    result_handle: ray.ObjectRef
    actor_handle: ray.actor.ActorHandle
    task: asyncio.Task[list[RayPartitionRef]] | None = None

    async def _get_result(self) -> list[RayPartitionRef]:
        await self.result_handle.completed()
        results = [result for result in self.result_handle]
        metadatas_ref = results.pop()

        metadatas = await metadatas_ref
        assert len(results) == len(metadatas)

        res = [
            RayPartitionRef(result, metadata.num_rows, metadata.size_bytes)
            for result, metadata in zip(results, metadatas)
        ]
        return res

    async def get_result(self) -> list[RayPartitionRef]:
        self.task = asyncio.create_task(self._get_result())
        return await self.task

    def cancel(self) -> None:
        if self.task:
            self.task.cancel()
        ray.cancel(self.result_handle)


class RaySwordfishActorHandle:
    """RaySwordfishWorkerHandle is a wrapper around a ray swordfish actor.

    It is used to submit tasks to the worker and keep track of the worker's node id, handle, number of cpus, total and available memory.
    """

    def __init__(
        self,
        actor_handle: ray.actor.ActorHandle,
    ):
        self.actor_handle = actor_handle

    def submit_task(self, task: RaySwordfishTask) -> RaySwordfishTaskHandle:
        psets = {k: [v.object_ref for v in v] for k, v in task.psets().items()}
        result_handle = self.actor_handle.run_plan.options(name=task.name()).remote(
            task.plan(), task.config(), psets, task.context()
        )
        return RaySwordfishTaskHandle(
            result_handle,
            self.actor_handle,
        )

    def shutdown(self) -> None:
        ray.kill(self.actor_handle)


def start_ray_workers() -> list[RaySwordfishWorker]:
    handles = []
    for node in ray.nodes():
        if (
            "Resources" in node
            and "CPU" in node["Resources"]
            and "memory" in node["Resources"]
            and node["Resources"]["CPU"] > 0
            and node["Resources"]["memory"] > 0
        ):
            actor = RaySwordfishActor.options(  # type: ignore
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"],
                    soft=False,
                ),
            ).remote(num_worker_threads=int(node["Resources"]["CPU"]))
            actor_handle = RaySwordfishActorHandle(actor)
            handles.append(
                RaySwordfishWorker(
                    node["NodeID"],
                    actor_handle,
                    int(node["Resources"]["CPU"]),
                    int(node["Resources"].get("GPU", 0)),
                    int(node["Resources"]["memory"]),
                )
            )

    return handles


@ray.remote(
    num_cpus=0,
)
class FlotillaPlanRunner:
    def __init__(self) -> None:
        self.curr_plans: dict[str, DistributedPhysicalPlan] = {}
        self.curr_result_gens: dict[str, AsyncIterator[tuple[ray.ObjectRef, int, int]]] = {}
        self.plan_runner = DistributedPhysicalPlanRunner()

    def run_plan(
        self,
        plan: DistributedPhysicalPlan,
        partition_sets: dict[str, PartitionSet[ray.ObjectRef]],
    ) -> None:
        psets = {
            k: [RayPartitionRef(v.partition(), v.metadata().num_rows, v.metadata().size_bytes or 0) for v in v.values()]
            for k, v in partition_sets.items()
        }
        self.curr_plans[plan.id()] = plan
        self.curr_result_gens[plan.id()] = self.plan_runner.run_plan(plan, psets)

    async def get_next_partition(self, plan_id: str) -> RayMaterializedResult | None:
        from daft.runners.ray_runner import (
            PartitionMetadataAccessor,
            RayMaterializedResult,
        )

        if plan_id not in self.curr_result_gens:
            raise ValueError(f"Plan {plan_id} not found in FlotillaPlanRunner")

        next_result = await self.curr_result_gens[plan_id].__anext__()
        if next_result is None:
            self.curr_plans.pop(plan_id)
            self.curr_result_gens.pop(plan_id)
            return None

        obj, num_rows, size_bytes = next_result
        metadata_accessor = PartitionMetadataAccessor.from_metadata_list([PartitionMetadata(num_rows, size_bytes)])
        materialized_result = RayMaterializedResult(
            partition=obj,
            metadatas=metadata_accessor,
            metadata_idx=0,
        )
        return materialized_result
