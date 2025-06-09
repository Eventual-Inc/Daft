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
    PyExpr,
    PyMicroPartition,
    RayPartitionRef,
    RaySwordfishTask,
    RaySwordfishWorker,
    set_compute_runtime_num_worker_threads,
)
from daft.expressions.expressions import Expression, ExpressionsProjection
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
    from collections.abc import AsyncIterator

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
    ) -> tuple[MicroPartition, PartitionMetadata]:
        """Run a plan on swordfish and yield partitions."""
        psets = {k: await asyncio.gather(*v) for k, v in psets.items()}
        psets_mp = {k: [v._micropartition for v in v] for k, v in psets.items()}

        res = []
        async for partition in self.native_executor.run_async(plan, psets_mp, config, None):
            if partition is None:
                break
            mp = MicroPartition._from_pymicropartition(partition)
            res.append(mp)
        concated = MicroPartition.concat(res)
        return concated, PartitionMetadata.from_table(concated)


@dataclass
class RaySwordfishTaskHandle:
    """RaySwordfishTaskHandle is a handle to a task that is running on a swordfish actor.

    It is used to asynchronously get the result of the task, cancel the task, and perform any post-task cleanup.
    """

    result_handle: ray.ObjectRef
    metadata_handle: ray.ObjectRef
    actor_handle: ray.actor.ActorHandle
    task: asyncio.Task[list[RayPartitionRef]] | None = None

    async def _get_result(self) -> list[RayPartitionRef]:
        # await the metadata, but not the data, so that the data is not transferred to head node.
        metadata = await self.metadata_handle
        res = [RayPartitionRef(self.result_handle, metadata.num_rows, metadata.size_bytes)]
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
        result_handle, metadata_handle = self.actor_handle.run_plan.options(name=task.name(), num_returns=2).remote(
            task.plan(), task.config(), psets
        )
        return RaySwordfishTaskHandle(
            result_handle,
            metadata_handle,
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


@ray.remote
class UDFActor:
    def __init__(self, uninitialized_projection: ExpressionsProjection) -> None:
        self.projection = ExpressionsProjection([e._initialize_udfs() for e in uninitialized_projection])

    def get_node_id(self) -> str:
        return ray.get_runtime_context().get_node_id()

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        mp = MicroPartition._from_pymicropartition(input)
        res = mp.eval_expression_list(self.projection)
        return res._micropartition


class UDFActorHandle:
    def __init__(self, node_id: str, actor_ref: ray.ObjectRef) -> None:
        self.node_id = node_id
        self.actor = actor_ref

    def eval_input(self, input: PyMicroPartition) -> PyMicroPartition:
        return ray.get(self.actor.eval_input.remote(input))

    def teardown(self) -> None:
        ray.kill(self.actor)


def start_udf_actors(
    projection: list[PyExpr],
    num_actors: int,
    num_gpus_per_actor: float,
    memory_per_actor: float,
    num_cpus_per_actor: float,
) -> list[tuple[str, list[UDFActorHandle]]]:
    expr_projection = ExpressionsProjection([Expression._from_pyexpr(expr) for expr in projection])
    handles: dict[str, list[UDFActorHandle]] = {}
    actors = [
        UDFActor.options(  # type: ignore
            scheduling_strategy="SPREAD",
            num_gpus=num_gpus_per_actor,
            num_cpus=num_cpus_per_actor,
            memory=memory_per_actor,
        ).remote(expr_projection)
        for _ in range(num_actors)
    ]
    node_ids = ray.get([actor.get_node_id.remote() for actor in actors])
    for actor, node_id in zip(actors, node_ids):
        handles.setdefault(node_id, []).append(UDFActorHandle(node_id, actor))

    res = [(node_id, handles) for node_id, handles in handles.items()]
    return res
