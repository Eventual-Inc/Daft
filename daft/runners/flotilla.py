from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple
from datetime import datetime  # <-- added

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


def _nowstr():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


class SwordfishTaskMetadata(NamedTuple):
    partition_metadatas: list[PartitionMetadata]
    stats: bytes | None


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
        set_compute_runtime_num_worker_threads(2)
        set_event_loop(asyncio.get_running_loop())
        # Track active plans: fingerprint -> (result_handle, active_input_ids_set, plan)
        # Use fingerprint as key to identify functionally equivalent plans
        # even when PyObject identities differ
        self.active_plans: dict[int, dict[str, Any]] = {}
        self.native_executor = NativeExecutor()

    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        exec_cfg: PyDaftExecutionConfig,
        psets: dict[str, list[ray.ObjectRef]],
        context: dict[str, str] | None,
        scan_tasks: dict[str, list] | None = None,
        glob_paths: dict[str, list[str]] | None = None,
    ) -> AsyncGenerator[MicroPartition | SwordfishTaskMetadata, None]:
        """Run a plan on swordfish and yield partitions."""
        # We import PyDaftContext inside the function because PyDaftContext is not serializable.
        from daft.daft import PyDaftContext
        
        with profile():
            # Get task_id from the context to use as input_id
            task_id = int(context.get("task_id", "0")) if context else 0

            # Get fingerprint for this plan
            fingerprint = plan.fingerprint()

            # Check if plan already exists
            if fingerprint in self.active_plans:
                # Reuse existing result_handle
                result_handle = self.active_plans[fingerprint]["result_handle"]
                self.active_plans[fingerprint]["active_input_ids_set"].add(task_id)
            else:
                # Create new result_handle
                ctx = PyDaftContext()
                ctx._daft_execution_config = exec_cfg
                # Pass empty psets to run() since we'll enqueue them instead
                result_handle = self.native_executor.run(plan, {}, ctx, context)
                self.active_plans[fingerprint] = {
                    "result_handle": result_handle,
                    "active_input_ids_set": {task_id},
                }
            
            # Await the partition refs from Ray
            psets = {k: await asyncio.gather(*v) for k, v in psets.items()}
            
            # Extract the _micropartition objects for enqueueing
            psets_mp = {k: [v._micropartition for v in v] for k, v in psets.items()}

            # Enqueue all inputs across all source_ids in a single call
            # This consolidates enqueue_in_memory, enqueue_scan_tasks, and enqueue_glob_paths
            metas = []
            result_receiver = await result_handle.enqueue_inputs(
                task_id,
                scan_tasks if scan_tasks and len(scan_tasks) > 0 else None,
                psets_mp if psets_mp and len(psets_mp) > 0 else None,
                glob_paths if glob_paths and len(glob_paths) > 0 else None,
            )
            
            if result_receiver is None:
                if fingerprint in self.active_plans:
                    self.active_plans[fingerprint]["active_input_ids_set"].remove(task_id)
                    if len(self.active_plans[fingerprint]["active_input_ids_set"]) == 0:
                        del self.active_plans[fingerprint]
                # Plan was cancelled or channel closed before we could get the receiver
                yield SwordfishTaskMetadata(partition_metadatas=[], stats=None)
                return
            
            async for partition in result_receiver:
                if partition is None:
                    break
                mp = MicroPartition._from_pymicropartition(partition)
                metas.append(PartitionMetadata.from_table(mp))
                yield mp

            # Cleanup: remove task_id from active set
            # Check if plan is still in active_plans (it might have been cancelled)
            if fingerprint in self.active_plans:
                # Update the dictionary entry
                self.active_plans[fingerprint]["active_input_ids_set"].remove(task_id)
                
                # If no more active inputs, finish and clean up
                if len(self.active_plans[fingerprint]["active_input_ids_set"]) == 0:
                    del self.active_plans[fingerprint]
                    stats = await result_handle.finish()
                    yield SwordfishTaskMetadata(partition_metadatas=metas, stats=stats)
                else:
                    # Other inputs are still active, just yield metadata for this input
                    # Note: We don't call finish() yet - it will be called when the last input completes
                    # For now, yield empty stats bytes since we haven't finished yet
                    yield SwordfishTaskMetadata(partition_metadatas=metas, stats=None)

    async def cancel(self, plan: LocalPhysicalPlan) -> None:
        """Cancel a plan by finishing it and removing it from active_plans."""
        fingerprint = plan.fingerprint()
        if fingerprint in self.active_plans:
            result_handle = self.active_plans.pop(fingerprint)["result_handle"]
            await result_handle.finish()


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
    plan: LocalPhysicalPlan
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
        # Cancel the plan on the actor
        self.actor_handle.cancel.remote(self.plan)


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
        # Get scan_tasks if available (now returns HashMap<String, Vec<PyScanTask>>)
        scan_tasks = task.scan_tasks()
        # Pass the HashMap directly (or empty dict if None)
        scan_tasks_map = scan_tasks if scan_tasks is not None else {}
        # Get glob_paths if available
        glob_paths = task.glob_paths()
        glob_paths_map = glob_paths if glob_paths is not None else {}
        plan = task.plan()
        result_handle = self.actor_handle.run_plan.options(name=task.name()).remote(
            plan, task.config(), psets, task.context(), scan_tasks_map, glob_paths_map
        )
        return RaySwordfishTaskHandle(
            result_handle,
            self.actor_handle,
            plan,
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
