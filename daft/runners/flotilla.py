from __future__ import annotations

import asyncio
import atexit
import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple

from daft.context import get_context
from daft.daft import (
    DistributedPhysicalPlan,
    DistributedPhysicalPlanRunner,
    FlightPartitionRef,
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
from daft.runners.partitioning import PartitionMetadata, PartitionSet
from daft.runners.profiler import profile
from daft.subscribers.event_log import RemoteEventLogSubscriber
from daft.subscribers.event_log_sink import (
    create_or_get_sink,
    get_sink,
    teardown_sink,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Generator

    from daft.runners.ray_runner import RayMaterializedResult

try:
    import ray
except ImportError:
    raise

logger = logging.getLogger(__name__)


class SwordfishTaskMetadata(NamedTuple):
    partition_metadatas: list[PartitionMetadata]
    stats: bytes
    is_flight_shuffle: bool


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


def _load_extensions_from_env() -> None:
    """Load every extension listed in the `DAFT_EXTENSION_PATHS` env var.

    The env var holds a JSON-encoded list of absolute paths. Set by the driver
    when it creates Ray actors, so workers dlopen the same `.so`s the driver
    has and can re-attach FFI handles when plans deserialize.
    """
    paths_json = os.environ.get("DAFT_EXTENSION_PATHS")
    if not paths_json:
        return
    import json

    import daft

    try:
        paths = json.loads(paths_json)
    except json.JSONDecodeError:
        logger.warning("DAFT_EXTENSION_PATHS is set but not valid JSON: %r", paths_json)
        return
    for path in paths:
        try:
            daft.load_extension(path)
        except Exception as e:
            logger.warning("Failed to load extension %s on worker: %s", path, e)


def _extension_runtime_env() -> dict[str, dict[str, str]]:
    """Build a Ray `runtime_env` fragment that propagates the driver's loaded extensions.

    Propagates via the `DAFT_EXTENSION_PATHS` env var.
    Returns an empty dict if no extensions are loaded. Callers should merge
    the returned dict into any existing `runtime_env` they pass to Ray.
    """
    import json

    from daft.daft import get_loaded_extension_paths

    paths = get_loaded_extension_paths()
    if not paths:
        return {}
    return {"env_vars": {"DAFT_EXTENSION_PATHS": json.dumps(paths)}}


@ray.remote
class RaySwordfishActor:
    """RaySwordfishActor is a ray actor that runs local physical plans on swordfish.

    It is a stateless, async actor, and can run multiple plans concurrently and is able to retry itself and it's tasks.
    """

    def __init__(
        self,
        num_cpus: int,
        num_gpus: int,
        is_head: bool = False,
        event_log_dir: str | None = None,
    ) -> None:
        os.environ["DAFT_FLOTILLA_WORKER"] = "1"  # TODO: Remove once fixed DashboardSubscriber

        if event_log_dir:
            _attach_remote_event_log_subscriber(
                component="swordfish_worker",
                node_role="head" if is_head else "worker",
            )

        if num_gpus > 0:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(i) for i in range(num_gpus))
        # Configure the number of worker threads for swordfish, according to the number of CPUs visible to ray.
        set_compute_runtime_num_worker_threads(num_cpus)
        set_event_loop(asyncio.get_running_loop())
        _load_extensions_from_env()

        self.ip = ray.util.get_node_ip_address()
        self.native_executor = NativeExecutor(is_flotilla_worker=True, ip=self.ip)

    def get_address(self) -> str:
        address = self.native_executor.shuffle_address()
        if address is None:
            raise RuntimeError("Flotilla worker should have started a Flight shuffle server")
        return address

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
    ) -> AsyncGenerator[MicroPartition | FlightPartitionRef | SwordfishTaskMetadata, None]:
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
            is_flight_shuffle = False
            async for partition in result_handle:
                if isinstance(partition, FlightPartitionRef):
                    is_flight_shuffle = True
                    metas.append(PartitionMetadata.from_flight_partition_ref(partition))
                    yield partition
                elif isinstance(partition, PyMicroPartition):
                    mp = MicroPartition._from_pymicropartition(partition)
                    metas.append(PartitionMetadata.from_table(mp))
                    yield mp
                else:
                    break

            stats = await result_handle.try_finish()
            yield SwordfishTaskMetadata(
                partition_metadatas=metas,
                stats=stats.encode(),
                is_flight_shuffle=is_flight_shuffle,
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
    task: asyncio.Task[RayTaskResult] | None = None

    async def _get_result(self) -> RayTaskResult:
        try:
            await self.result_handle.completed()
            results = [result for result in self.result_handle]
            metadata_ref = results.pop()
            task_metadata: SwordfishTaskMetadata = await metadata_ref
            assert len(results) == len(task_metadata.partition_metadatas)

            if task_metadata.is_flight_shuffle:
                flight_part_refs = await asyncio.gather(*results)
                return RayTaskResult.success_flight(flight_part_refs, task_metadata.stats)

            return RayTaskResult.success_ray(
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
        for source_id, py_input in task.inputs().items():
            inputs[str(source_id)] = py_input
        for source_id, refs in task.psets().items():
            inputs[str(source_id)] = [ref.object_ref for ref in refs]
        result_handle = self.actor_handle.run_plan.options(name=task.name()).remote(
            task.plan(),
            task.config(),
            task.context(),
            **inputs,
        )
        return RaySwordfishTaskHandle(result_handle)

    def shutdown(self) -> None:
        ray.kill(self.actor_handle)


def _get_worker_startup_timeout() -> int:
    return get_context().daft_execution_config.worker_startup_timeout


def _attach_remote_event_log_subscriber(component: str, node_role: str) -> None:
    """Attach a RemoteEventLogSubscriber to this process's context.

    Looks up the per-job event log sink actor and wires a subscriber that
    ships events to it. No-op if the sink does not exist. Failures are logged
    (not raised) so event-log setup cannot break actor initialization.
    """
    try:
        sink = get_sink(_get_ray_job_id_for_actor_naming())
        if sink is None:
            return
        get_context().attach_subscriber(
            "_daft_event_log_remote",
            RemoteEventLogSubscriber(sink, component=component, node_role=node_role),
        )
    except Exception as e:
        logger.warning("Failed to attach remote event log subscriber: %s", e)


def start_ray_workers(existing_worker_ids: list[str]) -> list[RaySwordfishWorker]:
    event_log_dir = os.environ.get("DAFT_EVENT_LOG_DIR")
    actors = []
    for node in ray.nodes():
        if (
            "Resources" in node
            and "CPU" in node["Resources"]
            and "memory" in node["Resources"]
            and node["Resources"]["CPU"] > 0
            and node["Resources"]["memory"] > 0
            and node["NodeID"] not in existing_worker_ids
        ):
            # `node:__internal_head__` is Ray's internal resource key tagging the head node;
            # not in public Ray docs but stable and relied on by Ray's own autoscaler/GCS code.
            is_head = node["Resources"].get("node:__internal_head__", 0) == 1
            ext_env = _extension_runtime_env()
            swordfish_options: dict[str, object] = {
                "scheduling_strategy": ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"],
                    soft=False,
                ),
            }
            if ext_env:
                swordfish_options["runtime_env"] = ext_env
            actor = RaySwordfishActor.options(**swordfish_options).remote(  # type: ignore
                num_cpus=int(node["Resources"]["CPU"]),
                num_gpus=int(node["Resources"].get("GPU", 0)),
                is_head=is_head,
                event_log_dir=event_log_dir,
            )
            actors.append((node, actor))

    # Batch all IP address retrievals into a single ray.get call
    actor_startup_timeout = _get_worker_startup_timeout()
    try:
        ip_addresses = ray.get(
            [actor.get_address.remote() for _, actor in actors],
            timeout=actor_startup_timeout,
        )
    except ray.exceptions.GetTimeoutError:
        raise RuntimeError(f"Failed to get IP addresses for actors within {actor_startup_timeout} seconds")

    handles = []
    for (node, actor), ip_address in zip(actors, ip_addresses):
        actor_handle = RaySwordfishActorHandle(actor)
        handles.append(
            RaySwordfishWorker(
                node["NodeID"],
                actor_handle,
                int(node["Resources"]["CPU"]),
                int(node["Resources"].get("GPU", 0)),
                int(node["Resources"]["memory"]),
                ip_address,
            )
        )

    return handles


def try_autoscale(bundles: list[dict[str, int]]) -> None:
    from ray.autoscaler.sdk import request_resources

    request_resources(
        bundles=bundles,
    )


@ray.remote(num_cpus=0)
class RemoteFlotillaRunner:
    def __init__(
        self,
        dashboard_url: str | None = None,
        event_log_dir: str | None = None,
    ) -> None:
        _load_extensions_from_env()
        if dashboard_url:
            os.environ["DAFT_DASHBOARD_URL"] = dashboard_url
            try:
                from daft.daft import refresh_dashboard_subscriber

                refresh_dashboard_subscriber()
            except ImportError:
                pass
            except Exception:
                pass

        if event_log_dir:
            _attach_remote_event_log_subscriber(
                component="flotilla_runner",
                node_role="head",
            )

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
            k: [
                RayPartitionRef(res.partition(), res.metadata().num_rows, res.metadata().size_bytes or 0)
                for res in v.values()
            ]
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

        event_log_dir = os.environ.get("DAFT_EVENT_LOG_DIR")
        if event_log_dir:
            sink_job_id = _get_ray_job_id_for_actor_naming()
            create_or_get_sink(event_log_dir, sink_job_id, head_node_id)
            atexit.register(teardown_sink, sink_job_id)

        runner_env_vars: dict[str, str] = {}
        if dashboard_url:
            runner_env_vars["DAFT_DASHBOARD_URL"] = dashboard_url

        ext_env = _extension_runtime_env()
        if "env_vars" in ext_env:
            runner_env_vars.update(ext_env["env_vars"])

        flotilla_options: dict[str, object] = {
            "name": get_flotilla_runner_actor_name(),
            "namespace": FLOTILLA_RUNNER_NAMESPACE,
            "get_if_exists": True,
            "scheduling_strategy": (
                ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=head_node_id,
                    soft=False,
                )
                if head_node_id
                else None
            ),
        }
        if runner_env_vars:
            flotilla_options["runtime_env"] = {"env_vars": runner_env_vars}

        self.runner = RemoteFlotillaRunner.options(**flotilla_options).remote(  # type: ignore
            dashboard_url=dashboard_url, event_log_dir=event_log_dir
        )

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
