from __future__ import annotations

import asyncio
import atexit
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NamedTuple, TypeAlias

from daft.context import get_context
from daft.daft import (
    DistributedPhysicalPlan,
    DistributedPhysicalPlanRunner,
    FlightShufflePartitionRef,
    Input,
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    PyExecutionStats,
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
    from collections.abc import AsyncGenerator, AsyncIterator, Generator

    from daft.daft import ShuffleWriteInfo
    from daft.runners.ray_runner import RayMaterializedResult

try:
    import ray
except ImportError:
    raise

logger = logging.getLogger(__name__)

ShufflePlanMetadata = list[tuple[object | None, int, int]]
ShufflePlanResult = tuple[str, ShufflePlanMetadata, bytes]
ShuffleWriteInfoLike: TypeAlias = "ShuffleWriteInfo | tuple[str, int, int]"


class SwordfishTaskMetadata(NamedTuple):
    partition_metadatas: list[PartitionMetadata]
    stats: bytes


def _shuffle_write_backend(info: ShuffleWriteInfoLike) -> str:
    if isinstance(info, tuple):
        return info[0]
    return info.backend


def _shuffle_write_id(info: ShuffleWriteInfoLike) -> int:
    if isinstance(info, tuple):
        return info[1]
    return info.shuffle_id


@ray.remote  # type: ignore[untyped-decorator]
def _clear_flight_shuffle_dirs(shuffle_dirs: list[str]) -> None:
    """Clear flight shuffle directories on a worker node.

    Args:
        shuffle_dirs: List of shuffle directories to clear (full paths)
    """
    for shuffle_dir in shuffle_dirs:
        if os.path.exists(shuffle_dir):
            try:
                shutil.rmtree(shuffle_dir)
                logger.info("Cleared flight shuffle directory: %s", shuffle_dir)
            except Exception as e:
                logger.warning("Failed to clear flight shuffle directory %s: %s", shuffle_dir, e)


async def clear_flight_shuffle_dirs_on_all_nodes(shuffle_dirs: list[str]) -> None:
    """Clear flight shuffle directories on all Ray nodes with CPU resources.

    Args:
        shuffle_dirs: List of shuffle directories to clear (full paths)
    """
    if not shuffle_dirs:
        return

    await asyncio.gather(
        *[
            _clear_flight_shuffle_dirs.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"],
                    soft=False,
                )
            ).remote(shuffle_dirs)
            for node in ray.nodes()
            if "Resources" in node and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0
        ]
    )


@ray.remote
class RaySwordfishActor:
    """RaySwordfishActor is a ray actor that runs local physical plans on swordfish.

    It is a stateless, async actor, and can run multiple plans concurrently and is able to retry itself and it's tasks.
    """

    def __init__(self, num_cpus: int, num_gpus: int) -> None:
        os.environ["DAFT_FLOTILLA_WORKER"] = "1"  # TODO: Remove once fixed DashboardSubscriber
        if num_gpus > 0:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in range(num_gpus))
        # Configure the number of worker threads for swordfish, according to the number of CPUs visible to ray.
        set_compute_runtime_num_worker_threads(num_cpus)
        set_event_loop(asyncio.get_running_loop())

        self.ip = ray.util.get_node_ip_address()
        self.native_executor = NativeExecutor(is_flotilla_worker=True, ip=self.ip)
        self.port = self.native_executor.shuffle_port()

    def get_address(self) -> str:
        return f"grpc://{self.ip}:{self.port}"

    async def _resolve_inputs(
        self,
        context: dict[str, str] | None,
        inputs: dict[str, Input | list[ray.ObjectRef]],
    ) -> tuple[dict[int, Input | list[PyMicroPartition]], int]:
        resolved_inputs: dict[int, Input | list[PyMicroPartition]] = {}
        list_items = [(source_id, part) for source_id, part in inputs.items() if isinstance(part, list)]
        non_list_items = [(source_id, part) for source_id, part in inputs.items() if not isinstance(part, list)]
        task_id = int(context.get("task_id", "0")) if context else 0
        if list_items:
            list_results = await asyncio.gather(*[asyncio.gather(*part) for _, part in list_items])
            for (source_id, _), mps in zip(list_items, list_results):
                resolved_inputs[int(source_id)] = [getattr(mp, "_micropartition", mp) for mp in mps]

        for source_id, part in non_list_items:
            resolved_inputs[int(source_id)] = part

        return resolved_inputs, task_id

    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        exec_cfg: PyDaftExecutionConfig,
        context: dict[str, str] | None,
        **inputs: (
            Input | list[ray.ObjectRef]
        ),  # PyMicroPartitions are separated from Inputs because they are Ray ObjectRefs, which will be resolved by Ray.
    ) -> AsyncGenerator[MicroPartition | SwordfishTaskMetadata, None]:
        """Run a plan on swordfish and yield partitions."""
        # We import PyDaftContext inside the function because PyDaftContext is not serializable.
        from daft.daft import PyDaftContext

        with profile():
            resolved_inputs, task_id = await self._resolve_inputs(context, inputs)

            ctx = PyDaftContext()
            ctx._daft_execution_config = exec_cfg

            result_handle = await self.native_executor.run(
                plan,
                ctx,
                task_id,
                resolved_inputs,
                context,
                False,
            )
            metas = []
            async for partition in result_handle:
                if partition is None:
                    break
                mp = MicroPartition._from_pymicropartition(partition)
                metas.append(PartitionMetadata.from_table(mp))
                yield mp

            stats = await result_handle.try_finish()
            yield SwordfishTaskMetadata(partition_metadatas=metas, stats=stats.encode())

    async def run_shuffle_plan(
        self,
        plan: LocalPhysicalPlan,
        exec_cfg: PyDaftExecutionConfig,
        context: dict[str, str] | None,
        **inputs: Input | list[ray.ObjectRef],
    ) -> ShufflePlanResult:
        from daft.daft import PyDaftContext

        with profile():
            resolved_inputs, task_id = await self._resolve_inputs(context, inputs)

            ctx = PyDaftContext()
            ctx._daft_execution_config = exec_cfg

            backend_info = plan.shuffle_write_info()
            if backend_info is None:
                raise ValueError("run_shuffle_plan() requires a repartition write plan")
            backend = _shuffle_write_backend(backend_info)

            result_handle = await self.native_executor.run(
                plan,
                ctx,
                task_id,
                resolved_inputs,
                context,
                False,
            )
            stats, shuffle_metadata = await result_handle.try_finish_with_shuffle_metadata()
            if shuffle_metadata is None:
                raise ValueError("Shuffle plan did not return shuffle metadata")

            return (
                backend,
                [(object_ref, int(num_rows), int(size_bytes)) for object_ref, num_rows, size_bytes in shuffle_metadata],
                stats.encode(),
            )


@ray.remote  # type: ignore[untyped-decorator]
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
    merged_sorted = mp.sort(
        sort_by_exprs.to_column_expressions(),
        descending=descending,
        nulls_first=nulls_first,
    )

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
    shuffle_write_info: ShuffleWriteInfoLike | None = None
    cache_id: int | None = None
    task: asyncio.Task[RayTaskResult] | None = None

    async def _get_result(self) -> RayTaskResult:
        try:
            if self.shuffle_write_info is not None:
                backend, refs, stats = await self.result_handle
                if backend == "ray":
                    ray_refs: list[RayPartitionRef] = []
                    for object_ref, num_rows, size_bytes in refs:
                        if object_ref is None:
                            raise ValueError("Expected Ray shuffle metadata to include object refs")
                        ray_refs.append(RayPartitionRef(object_ref, num_rows, size_bytes))
                    return RayTaskResult.ray_shuffle_success(
                        ray_refs,
                        stats,
                    )
                if backend == "flight":
                    shuffle_id = _shuffle_write_id(self.shuffle_write_info)
                    actor_address = await self.actor_handle.get_address.remote()
                    cache_id = self.cache_id if self.cache_id is not None else 0
                    flight_refs: list[FlightShufflePartitionRef] = []
                    for partition_idx, (object_ref, num_rows, size_bytes) in enumerate(refs):
                        if object_ref is not None:
                            raise ValueError("Expected Flight shuffle metadata to not include object refs")
                        flight_refs.append(
                            FlightShufflePartitionRef(
                                shuffle_id,
                                partition_idx,
                                actor_address,
                                cache_id,
                                num_rows,
                                size_bytes,
                            )
                        )
                    return RayTaskResult.flight_shuffle_success(
                        flight_refs,
                        stats,
                    )
                raise NotImplementedError(f"Unsupported shuffle write backend: {backend}")

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
        inputs: dict[str, Input | list[ray.ObjectRef]] = {}
        plan = task.plan()
        for source_id, py_input in task.inputs().items():
            inputs[str(source_id)] = py_input
        for source_id, refs in task.psets().items():
            inputs[str(source_id)] = [ref.object_ref for ref in refs]
        shuffle_write_info = plan.shuffle_write_info()
        if shuffle_write_info is None:
            result_handle = self.actor_handle.run_plan.options(name=task.name()).remote(
                plan,
                task.config(),
                task.context(),
                **inputs,
            )
        else:
            result_handle = self.actor_handle.run_shuffle_plan.options(name=task.name()).remote(
                plan,
                task.config(),
                task.context(),
                **inputs,
            )
        return RaySwordfishTaskHandle(
            result_handle,
            self.actor_handle,
            shuffle_write_info,
            task.id(),
        )

    def shutdown(self) -> None:
        ray.kill(self.actor_handle)


def _get_worker_startup_timeout() -> int:
    return get_context().daft_execution_config.worker_startup_timeout


@dataclass
class _PendingActor:
    """Tracks a spawned actor that hasn't yet reported its address."""

    node: dict[str, Any]
    actor: ray.actor.ActorHandle
    address_ref: ray.ObjectRef
    spawn_time: float


_pending_actors: dict[str, _PendingActor] = {}


def _is_eligible_node(node: dict[str, Any]) -> bool:
    return (
        node.get("Alive", True)
        and "Resources" in node
        and "CPU" in node["Resources"]
        and "memory" in node["Resources"]
        and node["Resources"]["CPU"] > 0
        and node["Resources"]["memory"] > 0
    )


def _kill_actor(actor: ray.actor.ActorHandle) -> None:
    try:
        ray.kill(actor)
    except Exception:
        pass


def clear_pending_ray_workers() -> None:
    for node_id, pending in list(_pending_actors.items()):
        logger.info("Cleaning up pending actor on node %s", node_id)
        _pending_actors.pop(node_id, None)
        _kill_actor(pending.actor)


atexit.register(clear_pending_ray_workers)


def start_ray_workers(existing_worker_ids: list[str]) -> list[RaySwordfishWorker]:
    worker_startup_timeout = _get_worker_startup_timeout()
    now = time.monotonic()
    all_nodes = ray.nodes()

    # Prune pending actors whose nodes no longer exist
    current_node_ids = {node["NodeID"] for node in all_nodes if _is_eligible_node(node)}
    for nid in [nid for nid in _pending_actors if nid not in current_node_ids]:
        pending = _pending_actors.pop(nid)
        logger.warning("Node %s disappeared while actor was pending startup; cleaning up", nid)
        _kill_actor(pending.actor)

    # Spawn actors for new nodes (not already tracked or existing)
    skip_ids = set(existing_worker_ids) | set(_pending_actors.keys())
    for node in all_nodes:
        node_id = node["NodeID"]
        if node_id not in skip_ids and _is_eligible_node(node):
            actor = RaySwordfishActor.options(  # type: ignore
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False,
                ),
            ).remote(
                num_cpus=int(node["Resources"]["CPU"]),
                num_gpus=int(node["Resources"].get("GPU", 0)),
            )
            _pending_actors[node_id] = _PendingActor(
                node=node,
                actor=actor,
                address_ref=actor.get_address.remote(),
                spawn_time=now,
            )

    if not _pending_actors:
        return []

    # Non-blocking check for ready actors
    pending_items = list(_pending_actors.items())
    ready_refs, _ = ray.wait(
        [p.address_ref for _, p in pending_items],
        num_returns=len(pending_items),
        timeout=0,
    )
    ready_ref_set = set(ready_refs)

    # Collect ready workers
    ready_workers: list[RaySwordfishWorker] = []
    resolved_node_ids: list[str] = []
    failed_node_ids: list[str] = []

    for node_id, pending in pending_items:
        if pending.address_ref not in ready_ref_set:
            continue
        resolved_node_ids.append(node_id)
        try:
            ip_address = ray.get(pending.address_ref)
            ready_workers.append(
                RaySwordfishWorker(
                    node_id,
                    RaySwordfishActorHandle(pending.actor),
                    int(pending.node["Resources"]["CPU"]),
                    int(pending.node["Resources"].get("GPU", 0)),
                    int(pending.node["Resources"]["memory"]),
                    ip_address,
                )
            )
        except (ray.exceptions.ActorDiedError, ray.exceptions.ActorUnschedulableError) as e:
            logger.warning("Pending actor on node %s died during startup: %s", node_id, e)
            failed_node_ids.append(node_id)
            _kill_actor(pending.actor)
        except Exception as e:
            logger.warning("Unexpected error getting address for node %s: %s", node_id, e)
            failed_node_ids.append(node_id)
            _kill_actor(pending.actor)

    for nid in resolved_node_ids:
        _pending_actors.pop(nid, None)

    # Expire actors that exceeded the startup timeout
    timed_out_node_ids: list[str] = []
    for nid in [nid for nid, p in _pending_actors.items() if now - p.spawn_time > worker_startup_timeout]:
        pending = _pending_actors.pop(nid)
        timed_out_node_ids.append(nid)
        logger.warning("Actor on node %s failed to start within %ds; killing actor", nid, worker_startup_timeout)
        _kill_actor(pending.actor)

    if (
        not existing_worker_ids
        and not ready_workers
        and not _pending_actors
        and (failed_node_ids or timed_out_node_ids)
    ):
        failed_nodes = ", ".join(sorted({*failed_node_ids, *timed_out_node_ids}))
        raise RuntimeError(
            "Failed to start any Ray workers: "
            f"startup failed or timed out within {worker_startup_timeout} seconds "
            f"on nodes: {failed_nodes}"
        )

    return ready_workers


def try_autoscale(bundles: list[dict[str, int]]) -> None:
    from ray.autoscaler.sdk import request_resources

    request_resources(
        bundles=bundles,
    )


@ray.remote(num_cpus=0)
class RemoteFlotillaRunner:
    def __init__(self, dashboard_url: str | None = None) -> None:
        if dashboard_url:
            os.environ["DAFT_DASHBOARD_URL"] = dashboard_url
            try:
                from daft.daft import refresh_dashboard_subscriber

                refresh_dashboard_subscriber()
            except ImportError:
                pass
            except Exception:
                pass

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

    async def get_next_partition(self, plan_id: str) -> RayMaterializedResult | PyExecutionStats | None:
        from daft.runners.ray_runner import (
            PartitionMetadataAccessor,
            RayMaterializedResult,
        )

        try:
            next_partition_ref = await self.curr_result_gens[plan_id].__anext__()
        except StopAsyncIteration:
            next_partition_ref = None

        if next_partition_ref is None:
            stats: PyExecutionStats = self.curr_result_gens[plan_id].finish()  # type: ignore[attr-defined]
            self.curr_plans.pop(plan_id, None)
            self.curr_result_gens.pop(plan_id, None)
            return stats

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
_FLOTILLA_RUNNER_NAME_SUFFIX: str | None = None


def _get_ray_job_id_for_actor_naming() -> str | None:
    """Best-effort detection of the current Ray job id for flotilla actor naming."""
    try:
        runtime_ctx = ray.get_runtime_context()
    except Exception:
        runtime_ctx = None

    if runtime_ctx is not None:
        get_job_id = getattr(runtime_ctx, "get_job_id", None)
        if callable(get_job_id):
            try:
                job_id = get_job_id()
            except Exception:
                job_id = None
        else:
            job_id = getattr(runtime_ctx, "job_id", None)
        if job_id is not None:
            try:
                return job_id.hex()
            except Exception:
                return str(job_id)

    return None


def get_flotilla_runner_actor_name() -> str:
    """Return the per-Ray-job actor name for RemoteFlotillaRunner."""
    global _FLOTILLA_RUNNER_NAME_SUFFIX

    if _FLOTILLA_RUNNER_NAME_SUFFIX is None:
        job_id: str | None
        try:
            job_id = _get_ray_job_id_for_actor_naming()
        except Exception:
            job_id = None

        if job_id is None:
            _FLOTILLA_RUNNER_NAME_SUFFIX = uuid.uuid4().hex
        else:
            _FLOTILLA_RUNNER_NAME_SUFFIX = job_id

    return f"{FLOTILLA_RUNNER_NAME}-{_FLOTILLA_RUNNER_NAME_SUFFIX}"


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
        dashboard_url = os.environ.get("DAFT_DASHBOARD_URL")
        self.runner = RemoteFlotillaRunner.options(  # type: ignore
            name=get_flotilla_runner_actor_name(),
            namespace=FLOTILLA_RUNNER_NAMESPACE,
            get_if_exists=True,
            runtime_env=({"env_vars": {"DAFT_DASHBOARD_URL": dashboard_url}} if dashboard_url else None),
            scheduling_strategy=(
                ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=head_node_id,
                    soft=False,
                )
                if head_node_id
                else None
            ),
        ).remote(dashboard_url=dashboard_url)

    def stream_plan(
        self,
        plan: DistributedPhysicalPlan,
        partition_sets: dict[str, PartitionSet[RayMaterializedResult]],
    ) -> Generator[RayMaterializedResult, None, PyExecutionStats]:
        plan_id = plan.idx()
        ray.get(self.runner.run_plan.remote(plan, partition_sets))

        while True:
            result = ray.get(self.runner.get_next_partition.remote(plan_id))
            if isinstance(result, PyExecutionStats):
                return result
            yield result
