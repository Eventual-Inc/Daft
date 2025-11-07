from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple

from daft.daft import (
    DistributedPhysicalPlan,
    DistributedPhysicalPlanRunner,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    PyMicroPartition,
    RayPartitionRef,
    RaySwordfishTask,
    RaySwordfishWorker,
    RayTaskResult,
    set_compute_runtime_num_worker_threads,
)
from daft.event_loop import set_event_loop
from daft.expressions import Expression, ExpressionsProjection
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


class SwordfishTaskMetadata(NamedTuple):
    partition_metadatas: list[PartitionMetadata]
    stats: bytes


@ray.remote
class RaySwordfishActor:
    """RaySwordfishActor is a ray actor that runs local physical plans on swordfish.

    It is a stateless, async actor, and can run multiple plans concurrently and is able to retry itself and it's tasks.
    """

    def __init__(self, num_cpus: int, num_gpus: int) -> None:
        os.environ["DAFT_FLOTILLA_WORKER"] = "1"
        if num_gpus > 0:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in range(num_gpus))
        # Configure the number of worker threads for swordfish, according to the number of CPUs visible to ray.
        set_compute_runtime_num_worker_threads(num_cpus)
        set_event_loop(asyncio.get_running_loop())

    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        exec_cfg: PyDaftExecutionConfig,
        psets: dict[str, list[ray.ObjectRef]],
        context: dict[str, str] | None,
    ) -> AsyncGenerator[MicroPartition | SwordfishTaskMetadata, None]:
        """Run a plan on swordfish and yield partitions."""
        # We import PyDaftContext inside the function because PyDaftContext is not serializable.
        from daft.daft import PyDaftContext

        with profile():
            psets = {k: await asyncio.gather(*v) for k, v in psets.items()}
            psets_mp = {k: [v._micropartition for v in v] for k, v in psets.items()}

            metas = []
            native_executor = NativeExecutor()
            ctx = PyDaftContext()
            ctx._daft_execution_config = exec_cfg
            result_handle = native_executor.run(plan, psets_mp, ctx, None, context)
            async for partition in result_handle:
                if partition is None:
                    break
                mp = MicroPartition._from_pymicropartition(partition)
                metas.append(PartitionMetadata.from_table(mp))
                yield mp

            stats = await result_handle.finish()
            yield SwordfishTaskMetadata(partition_metadatas=metas, stats=stats)


@ray.remote  # type: ignore[misc]
def get_boundaries_remote(
    sort_by: list[Expression],
    descending: list[bool],
    nulls_first: list[bool] | None,
    num_quantiles: int,
    *samples: MicroPartition,
) -> PyMicroPartition:
    sort_by_exprs = ExpressionsProjection(sort_by)

    mp = MicroPartition.concat(list(samples))
    nulls_first = nulls_first if nulls_first is not None else descending
    merged_sorted = mp.sort(sort_by_exprs.to_column_expressions(), descending=descending, nulls_first=nulls_first)

    result = merged_sorted.quantiles(num_quantiles)
    return result._micropartition


async def get_boundaries(
    samples: list[ray.ObjectRef],
    sort_by: list[Expression],
    descending: list[bool],
    nulls_first: list[bool] | None,
    num_quantiles: int,
) -> PyMicroPartition:
    return await get_boundaries_remote.remote(sort_by, descending, nulls_first, num_quantiles, *samples)


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
            metadata_ref = results.pop()

            task_metadata: SwordfishTaskMetadata = await metadata_ref
            assert len(results) == len(task_metadata.partition_metadatas)

            return RayTaskResult.success(
                [
                    RayPartitionRef(result, metadata.num_rows, metadata.size_bytes or 0)
                    for result, metadata in zip(results, task_metadata.partition_metadatas)
                ],
                task_metadata.stats,
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
        ray._private.worker.blocking_get_inside_async_warned = True
        set_event_loop(asyncio.get_running_loop())

    def run_plan(
        self,
        plan: DistributedPhysicalPlan,
        partition_sets: dict[str, PartitionSet[ray.ObjectRef]],
    ) -> None:
        psets = {
            k: [RayPartitionRef(v.partition(), v.metadata().num_rows, v.metadata().size_bytes or 0) for v in v.values()]
            for k, v in partition_sets.items()
        }
        self.curr_plans[plan.idx()] = plan
        self.curr_result_gens[plan.idx()] = self.plan_runner.run_plan(plan, psets)

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
        plan_id = plan.idx()
        ray.get(self.runner.run_plan.remote(plan, partition_sets))
        while True:
            materialized_result = ray.get(self.runner.get_next_partition.remote(plan_id))
            if materialized_result is None:
                break
            yield materialized_result
