from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from daft.daft import (
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    RayPartitionRef,
    RaySwordfishTask,
)
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.constants import MAX_SWORDFISH_ACTOR_RESTARTS, MAX_SWORDFISH_ACTOR_TASK_RETRIES
from daft.runners.partitioning import PartitionMetadata

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

try:
    import ray
    import ray.util.scheduling_strategies
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote(max_restarts=MAX_SWORDFISH_ACTOR_RESTARTS, max_task_retries=MAX_SWORDFISH_ACTOR_TASK_RETRIES)
class RaySwordfishWorker:
    """RaySwordfishWorker is a ray actor that runs local physical plans on swordfish.

    It is a stateless, async actor, and can run multiple plans concurrently and is able to retry itself and it's tasks.
    """

    def __init__(self, daft_execution_config: PyDaftExecutionConfig) -> None:
        self.native_executor = NativeExecutor()
        self.daft_execution_config = daft_execution_config

    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        psets: dict[str, list[ray.ObjectRef]],
    ) -> AsyncGenerator[MicroPartition, None]:
        """Run a plan on swordfish and yield partitions."""
        psets_gathered = {k: await asyncio.gather(*v) for k, v in psets.items()}
        psets_mp = {k: [v._micropartition for v in v] for k, v in psets_gathered.items()}
        async for partition in self.native_executor.run_async(plan, psets_mp, self.daft_execution_config, None):
            if partition is None:
                break
            mp = MicroPartition._from_pymicropartition(partition)
            yield mp

    def concat_and_get_metadata(self, *partitions: MicroPartition) -> tuple[PartitionMetadata, MicroPartition]:
        """Concatenate a list of partitions and return the metadata and the concatenated partition."""
        concated = MicroPartition.concat(list(partitions))
        return PartitionMetadata.from_table(concated), concated


@dataclass
class RaySwordfishTaskHandle:
    """RaySwordfishTaskHandle is a handle to a task that is running on a swordfish actor.

    It is used to asynchronously get the result of the task, cancel the task, and perform any post-task cleanup.
    """

    result_handle: ray.ObjectRef
    actor_handle: ray.actor.ActorHandle
    done_callback: Callable[[asyncio.Task[RayPartitionRef]], None]
    task_memory_cost: int

    async def _get_result(self) -> RayPartitionRef:
        results = []
        async for result in self.result_handle:
            results.append(result)

        metadata, result = self.actor_handle.concat_and_get_metadata.options(num_returns=2).remote(*results)
        metadata = await metadata
        return RayPartitionRef(result, metadata.num_rows, metadata.size_bytes)

    async def get_result(self) -> RayPartitionRef:
        task = asyncio.create_task(self._get_result())
        task.add_done_callback(self.done_callback)
        return await task

    def cancel(self) -> None:
        ray.cancel(self.result_handle)


class RaySwordfishWorkerHandle:
    """RaySwordfishWorkerHandle is a wrapper around a ray swordfish actor.

    It is used to submit tasks to the worker and keep track of the worker's node id, handle, number of cpus, total and available memory.
    """

    def __init__(
        self,
        actor_node_id: str,
        actor_handle: ray.actor.ActorHandle,
        num_cpus: int,
        total_memory_bytes: int,
    ) -> None:
        self.actor_node_id = actor_node_id
        self.actor_handle = actor_handle
        self.num_cpus = num_cpus
        self.total_memory_bytes = total_memory_bytes
        self.available_memory_bytes = total_memory_bytes

    def get_actor_node_id(self) -> str:
        return self.actor_node_id

    def get_num_cpus(self) -> int:
        return self.num_cpus

    def get_total_memory_bytes(self) -> int:
        return self.total_memory_bytes

    def get_available_memory_bytes(self) -> int:
        return self.available_memory_bytes

    def add_back_memory(self, memory_bytes: int) -> None:
        self.available_memory_bytes += memory_bytes

    def submit_task(self, task: RaySwordfishTask) -> RaySwordfishTaskHandle:
        self.available_memory_bytes -= task.estimated_memory_cost()
        psets = {k: [v.object_ref for v in v] for k, v in task.psets().items()}
        memory_to_return = task.estimated_memory_cost()

        def done_callback(_result: asyncio.Task[RayPartitionRef]) -> None:
            self.add_back_memory(memory_to_return)

        return RaySwordfishTaskHandle(
            self.actor_handle.run_plan.remote(task.plan(), psets),
            self.actor_handle,
            done_callback,
            task.estimated_memory_cost(),
        )

    def shutdown(self) -> None:
        ray.kill(self.actor_handle)


class RaySwordfishWorkerManager:
    """RaySwordfishWorkerManager is keeps track of all swordfish workers, creates new workers, and shuts down workers.

    It should have a global view of all workers and their resources.
    It is also responsible for autoscaling the number of workers.
    """

    def __init__(self, daft_execution_config: PyDaftExecutionConfig) -> None:
        self.workers: dict[str, RaySwordfishWorkerHandle] = {}
        self.daft_execution_config = daft_execution_config
        self._initialize_workers()

    def _initialize_workers(self) -> None:
        for node in ray.nodes():
            if (
                "Resources" in node
                and "CPU" in node["Resources"]
                and "memory" in node["Resources"]
                and node["Resources"]["CPU"] > 0
                and node["Resources"]["memory"] > 0
            ):
                actor = RaySwordfishWorker.options(  # type: ignore[attr-defined]
                    num_cpus=node["Resources"]["CPU"],
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"],
                        soft=True,
                    ),
                ).remote(self.daft_execution_config)
                self.workers[node["NodeID"]] = RaySwordfishWorkerHandle(
                    node["NodeID"],
                    actor,
                    int(node["Resources"]["CPU"]),
                    int(node["Resources"]["memory"]),
                )

    def submit_task_to_worker(self, task: RaySwordfishTask, worker_id: str) -> RaySwordfishTaskHandle:
        return self.workers[worker_id].submit_task(task)

    def get_worker_resources(self) -> list[tuple[str, int, int]]:
        return [
            (
                worker.get_actor_node_id(),
                worker.get_num_cpus(),
                worker.get_available_memory_bytes(),
            )
            for worker in self.workers.values()
        ]

    def try_autoscale(self, num_workers: int) -> None:
        """Try to autoscale the number of workers. this is a hint to the autoscaler to add more workers if needed."""
        ray.autoscaler.sdk.request_resources(
            num_cpus=num_workers,
        )

    def shutdown(self) -> None:
        for worker in self.workers.values():
            worker.shutdown()
