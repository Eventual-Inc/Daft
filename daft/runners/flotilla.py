from __future__ import annotations

import asyncio
import logging
import os
import time
import subprocess
from dataclasses import dataclass
from typing import TYPE_CHECKING

from daft.daft import (
    DistributedPhysicalPlan,
    DistributedPhysicalPlanRunner,
    InProgressSwordfishPlan,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    PyExpr,
    PyMicroPartition,
    RayPartitionRef,
    RaySwordfishTask,
    RaySwordfishWorker,
    ScanTask,
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
    def __init__(self) -> None:
        # os.makedirs("/tmp/ray/session_latest/logs/daft", exist_ok=True)
        # current_pid = os.getpid()
        # time.time()
        # subprocess.Popen(
        #     [
        #         "bash",
        #         "-c",
        #         f"""
        #         echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid
        #         samply record --pid {current_pid} -s -o /tmp/ray/session_latest/logs/daft/samply_{current_pid}_{time.time()}.json.gz
        #         """,
        #     ],
        # )
        self.native_executor = NativeExecutor()
        self.in_progress_plans: dict[str, InProgressSwordfishPlan] = {}

    async def start_plan(
        self,
        plan_id: str,
        plan: LocalPhysicalPlan,
        config: PyDaftExecutionConfig,
    ) -> None:
        in_progress_plan = self.native_executor.run_distributed(plan, config)
        self.in_progress_plans[plan_id] = in_progress_plan

    async def put_input(
        self,
        plan_id: str,
        input_id: int,
        inputs: ScanTask,
    ) -> None:
        in_progress_plan = self.in_progress_plans[plan_id]
        await in_progress_plan.put_input(input_id, inputs)

    async def get_result(
        self,
        plan_id: str,
        input_id: int,
    ) -> MicroPartition:
        in_progress_plan = self.in_progress_plans[plan_id]
        py_mp = await in_progress_plan.get_result(input_id)
        return MicroPartition._from_pymicropartition(py_mp)

    async def execute(
        self,
        plan_id: str,
        plan: LocalPhysicalPlan,
        input_id: int,
        input: ScanTask,
        config: PyDaftExecutionConfig,
    ) -> tuple[MicroPartition, PartitionMetadata]:
        if plan_id not in self.in_progress_plans:
            await self.start_plan(plan_id, plan, config)

        await self.put_input(plan_id, input_id, input)
        del input

        result = await self.get_result(plan_id, input_id)
        return result, PartitionMetadata.from_table(result)

    async def mark_plan_complete(self, plan_id: str) -> None:
        self.in_progress_plans.pop(plan_id)


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
        metadata = await self.metadata_handle
        return [
            RayPartitionRef(self.result_handle, metadata.num_rows, metadata.size_bytes)
        ]

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
        result_handle, metadata_handle = self.actor_handle.execute.options(
            name=task.name(),
            num_returns=2,
        ).remote(
            plan_id=task.plan_id(),
            plan=task.plan(),
            input=task.input(),
            input_id=task.input_id(),
            config=task.config(),
        )
        return RaySwordfishTaskHandle(
            result_handle,
            metadata_handle,
            self.actor_handle,
        )

    def mark_plan_complete(self, plan_id: str) -> None:
        self.actor_handle.mark_plan_complete.remote(plan_id)

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
            ).remote()
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
        self.curr_result_gens: dict[
            str, AsyncIterator[tuple[ray.ObjectRef, int, int]]
        ] = {}
        self.plan_runner = DistributedPhysicalPlanRunner()

    def run_plan(
        self,
        plan: DistributedPhysicalPlan,
        partition_sets: dict[str, PartitionSet[ray.ObjectRef]],
    ) -> None:
        psets = {
            k: [
                RayPartitionRef(
                    v.partition(), v.metadata().num_rows, v.metadata().size_bytes or 0
                )
                for v in v.values()
            ]
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
        metadata_accessor = PartitionMetadataAccessor.from_metadata_list(
            [PartitionMetadata(num_rows, size_bytes)]
        )
        materialized_result = RayMaterializedResult(
            partition=obj,
            metadatas=metadata_accessor,
            metadata_idx=0,
        )
        return materialized_result


@ray.remote
class UDFActor:
    def __init__(self, uninitialized_projection: ExpressionsProjection) -> None:
        self.projection = ExpressionsProjection(
            [e._initialize_udfs() for e in uninitialized_projection]
        )

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
    expr_projection = ExpressionsProjection(
        [Expression._from_pyexpr(expr) for expr in projection]
    )
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
