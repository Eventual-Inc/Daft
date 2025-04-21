import logging
from typing import AsyncGenerator, Callable, Dict, List, Union

from daft.daft import (
    NativeExecutor,
    SwordfishWorkerTask,
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
        swordfish_worker_task: SwordfishWorkerTask,
    ) -> AsyncGenerator[Union[MicroPartition, List[PartitionMetadata]], None]:
        local_physical_plan = swordfish_worker_task.plan()
        daft_execution_config = swordfish_worker_task.execution_config()
        metadatas = []

        async for partition in self.native_executor.run_distributed(local_physical_plan, daft_execution_config):
            if partition is None:
                break
            mp = MicroPartition._from_pymicropartition(partition)
            yield mp
            metadata = PartitionMetadata.from_table(mp)
            metadatas.append(metadata)

        yield metadatas


class RayPartitionRef:
    def __init__(self, object_ref: ray.ObjectRef, num_rows: int, size_bytes: int):
        self.object_ref = object_ref
        self._num_rows = num_rows
        self._size_bytes = size_bytes

    def size_bytes(self) -> int:
        return self._size_bytes

    def num_rows(self) -> int:
        return self._num_rows


class RaySwordfishTaskHandle:
    def __init__(self, handle: ray.ObjectRef, add_memory_callback: Callable[[], None]):
        print("initializing handle")
        self.handle = handle
        self.add_memory_callback = add_memory_callback

    async def get_result(self) -> List[RayPartitionRef]:
        results = []
        async for result in self.handle:
            results.append(result)

        metadata = await results.pop()
        self.add_memory_callback()
        return [RayPartitionRef(ref, meta.num_rows, meta.size_bytes) for meta, ref in zip(metadata, results)]


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

    def add_back_memory(self):
        print(f"adding back memory to worker {self.actor_node_id}")
        self.available_memory_bytes += 100 * 1024 * 1024 * 1024

    def submit_task(self, task: SwordfishWorkerTask) -> RaySwordfishTaskHandle:
        print(f"submitting task to worker {self.actor_node_id}, estimated memory cost: {task.estimated_memory_cost()}")
        self.available_memory_bytes -= task.estimated_memory_cost()
        return RaySwordfishTaskHandle(self.actor_handle.run_plan.remote(task), self.add_back_memory)


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

    def submit_task_to_worker(self, task: SwordfishWorkerTask, worker_id: str) -> RaySwordfishTaskHandle:
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
