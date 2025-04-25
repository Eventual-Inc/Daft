import asyncio
import logging
from typing import AsyncGenerator, Callable, Dict, List, Tuple

from daft.daft import (
    LocalPhysicalPlan,
    NativeExecutor,
    PyDaftExecutionConfig,
    RayPartitionRef,
    RaySwordfishTask,
)
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.partitioning import PartitionMetadata

try:
    import ray
    import ray.util.scheduling_strategies
except ImportError:
    raise

logger = logging.getLogger(__name__)


@ray.remote(max_restarts=4, max_task_retries=4)
class SwordfishActor:
    def __init__(self):
        self.native_executor = NativeExecutor()

    # Run a plan on swordfish and yield partitions
    async def run_plan(
        self,
        plan: LocalPhysicalPlan,
        psets: dict[str, list[ray.ObjectRef]],
        daft_execution_config: PyDaftExecutionConfig,
    ) -> AsyncGenerator[MicroPartition, None]:
        psets = {k: await asyncio.gather(*v) for k, v in psets.items()}
        psets_mp = {k: [v._micropartition for v in v] for k, v in psets.items()}
        async for partition in self.native_executor.run_async(plan, psets_mp, daft_execution_config):
            if partition is None:
                break
            mp = MicroPartition._from_pymicropartition(partition)
            yield mp

    async def concat_and_get_metadata(self, *partitions: MicroPartition) -> Tuple[PartitionMetadata, MicroPartition]:
        concated = MicroPartition.concat(list(partitions))
        return PartitionMetadata.from_table(concated), concated


class RaySwordfishTaskHandle:
    def __init__(
        self,
        result_handle: ray.ObjectRef,
        actor_handle: ray.actor.ActorHandle,
        add_memory_callback: Callable[[int], None],
        task_memory_cost: int,
    ):
        self.result_handle = result_handle
        self.actor_handle = actor_handle
        self.add_memory_callback = add_memory_callback
        self.task_memory_cost = task_memory_cost

    async def _get_result(self) -> RayPartitionRef:
        results = []
        async for result in self.result_handle:
            results.append(result)

        metadata, result = self.actor_handle.concat_and_get_metadata.options(num_returns=2).remote(*results)
        metadata = await metadata
        self.add_memory_callback(self.task_memory_cost)
        return RayPartitionRef(result, metadata.num_rows, metadata.size_bytes)

    async def get_result(self) -> RayPartitionRef:
        task = asyncio.create_task(self._get_result())
        return await task

    def cancel(self):
        ray.cancel(self.result_handle)


class RaySwordfishWorker:
    def __init__(
        self,
        actor_node_id: str,
        actor_handle: ray.actor.ActorHandle,
        num_cpus: int,
        total_memory_bytes: int,
    ):
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

    def add_back_memory(self, memory_bytes: int):
        self.available_memory_bytes += memory_bytes

    def submit_task(self, task: RaySwordfishTask) -> RaySwordfishTaskHandle:
        self.available_memory_bytes -= task.estimated_memory_cost()
        psets = {k: [v.object_ref for v in v] for k, v in task.psets().items()}
        return RaySwordfishTaskHandle(
            self.actor_handle.run_plan.remote(task.plan(), psets, task.execution_config()),
            self.actor_handle,
            self.add_back_memory,
            task.estimated_memory_cost(),
        )

    def shutdown(self):
        ray.kill(self.actor_handle)


class RaySwordfishWorkerManager:
    def __init__(self):
        self.workers: Dict[str, RaySwordfishWorker] = {}
        self._initialize_workers()

    def _initialize_workers(self):
        for node in ray.nodes():
            if (
                "Resources" in node
                and "CPU" in node["Resources"]
                and "memory" in node["Resources"]
                and node["Resources"]["CPU"] > 0
                and node["Resources"]["memory"] > 0
            ):
                actor = SwordfishActor.options(
                    num_cpus=node["Resources"]["CPU"],
                    scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                        node_id=node["NodeID"],
                        soft=True,
                    ),
                ).remote()
                self.workers[node["NodeID"]] = RaySwordfishWorker(
                    node["NodeID"],
                    actor,
                    int(node["Resources"]["CPU"]),
                    int(node["Resources"]["memory"]),
                )

    def submit_task_to_worker(self, task: RaySwordfishTask, worker_id: str) -> RaySwordfishTaskHandle:
        if worker_id not in self.workers:
            raise ValueError(f"Worker {worker_id} not found")
        return self.workers[worker_id].submit_task(task)

    def get_worker_resources(self) -> List[tuple[str, int, int]]:
        return [
            (
                worker.get_actor_node_id(),
                worker.get_num_cpus(),
                worker.get_available_memory_bytes(),
            )
            for worker in self.workers.values()
        ]

    def try_autoscale(self, num_workers: int):
        ray.autoscaler.sdk.request_resources(
            num_cpus=num_workers,
        )

    def shutdown(self):
        for worker in self.workers.values():
            worker.shutdown()
