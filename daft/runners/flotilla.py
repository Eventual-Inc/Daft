from __future__ import annotations

import asyncio
import logging
import os
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
    RayTaskResult,
    set_compute_runtime_num_worker_threads,
)
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.partitioning import (
    PartitionMetadata,
    PartitionSet,
)
from daft.runners.profiler import profile

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Iterator

    from daft.runners.ray_runner import RayMaterializedResult

try:
    import ray
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote
class RaySwordfishActor:
    """RaySwordfishActor is a ray actor that runs local physical plans on swordfish.

    It is a stateless, async actor, and can run multiple plans concurrently and is able to retry itself and it's tasks.
    """

    def __init__(self, num_cpus: int, num_gpus: int) -> None:
        if num_gpus > 0:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in range(num_gpus))
        # Configure the number of worker threads for swordfish, according to the number of CPUs visible to ray.
        set_compute_runtime_num_worker_threads(num_cpus)

    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        config: PyDaftExecutionConfig,
        psets: dict[str, list[ray.ObjectRef]],
        context: dict[str, str] | None,
    ) -> AsyncGenerator[MicroPartition | list[PartitionMetadata], None]:
        """Run a plan on swordfish and yield partitions."""
        with profile():
            psets = {k: await asyncio.gather(*v) for k, v in psets.items()}
            psets_mp = {k: [v._micropartition for v in v] for k, v in psets.items()}

            metas = []
            native_executor = NativeExecutor()
            async for partition in native_executor.run_async(plan, psets_mp, config, None, context):
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
    task: asyncio.Task[RayTaskResult] | None = None

    async def _get_result(self) -> RayTaskResult:
        try:
            await self.result_handle.completed()
            results = [result for result in self.result_handle]
            metadatas_ref = results.pop()

            metadatas = await metadatas_ref
            assert len(results) == len(metadatas)

            return RayTaskResult.success(
                [
                    RayPartitionRef(result, metadata.num_rows, metadata.size_bytes)
                    for result, metadata in zip(results, metadatas)
                ]
            )
        except (ray.exceptions.ActorDiedError, ray.exceptions.ActorUnschedulableError):
            return RayTaskResult.worker_died()
        except ray.exceptions.ActorUnavailableError:
            return RayTaskResult.worker_unavailable()
        except Exception as e:
            raise e

    async def get_result(self) -> RayTaskResult:
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


def start_ray_workers(existing_worker_ids: list[str]) -> list[RaySwordfishWorker]:
    handles = []
    for node in ray.nodes():
        if (
            "Resources" in node
            and "CPU" in node["Resources"]
            and "memory" in node["Resources"]
            and node["Resources"]["CPU"] > 0
            and node["Resources"]["memory"] > 0
            and node["NodeID"] not in existing_worker_ids
        ):
            actor = RaySwordfishActor.options(  # type: ignore
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"],
                    soft=False,
                ),
            ).remote(
                num_cpus=int(node["Resources"]["CPU"]),
                num_gpus=int(node["Resources"].get("GPU", 0)),
            )
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


def try_autoscale(bundles: list[dict[str, int]]) -> None:
    from ray.autoscaler.sdk import request_resources

    request_resources(
        bundles=bundles,
    )


@ray.remote(
    num_cpus=0,
)
class RemoteFlotillaRunner:
    def __init__(self) -> None:
        self.curr_plans: dict[str, DistributedPhysicalPlan] = {}
        self.curr_result_gens: dict[str, AsyncIterator[RayPartitionRef]] = {}
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

        next_partition_ref = await self.curr_result_gens[plan_id].__anext__()
        if next_partition_ref is None:
            self.curr_plans.pop(plan_id)
            self.curr_result_gens.pop(plan_id)
            return None

        metadata_accessor = PartitionMetadataAccessor.from_metadata_list(
            [PartitionMetadata(next_partition_ref.num_rows, next_partition_ref.size_bytes)]
        )
        materialized_result = RayMaterializedResult(
            partition=next_partition_ref.object_ref,
            metadatas=metadata_accessor,
            metadata_idx=0,
        )
        return materialized_result


FLOTILLA_RUNNER_NAMESPACE = "daft"
FLOTILLA_RUNNER_NAME = "flotilla-plan-runner"


def get_head_node_id() -> str | None:
    for node in ray.nodes():
        if (
            "Resources" in node
            and "node:__internal_head__" in node["Resources"]
            and node["Resources"]["node:__internal_head__"] == 1
        ):
            return node["NodeID"]
    return None


class FlotillaRunner:
    """FlotillaRunner is a wrapper around FlotillaRunnerCore that provides a Ray actor interface."""

    def __init__(self) -> None:
        head_node_id = get_head_node_id()
        self.runner = RemoteFlotillaRunner.options(  # type: ignore
            name=FLOTILLA_RUNNER_NAME,
            namespace=FLOTILLA_RUNNER_NAMESPACE,
            get_if_exists=True,
            scheduling_strategy=(
                ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=head_node_id,
                    soft=False,
                )
                if head_node_id is not None
                else "DEFAULT"
            ),
        ).remote()

    def stream_plan(
        self,
        plan: DistributedPhysicalPlan,
        partition_sets: dict[str, PartitionSet[ray.ObjectRef]],
    ) -> Iterator[RayMaterializedResult]:
        plan_id = plan.id()
        ray.get(self.runner.run_plan.remote(plan, partition_sets))
        while True:
            materialized_result = ray.get(self.runner.get_next_partition.remote(plan_id))
            if materialized_result is None:
                break
            yield materialized_result
