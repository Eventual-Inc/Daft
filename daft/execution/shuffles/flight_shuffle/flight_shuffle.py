from __future__ import annotations

import logging
import random
from collections import defaultdict, deque
from typing import TYPE_CHECKING

from daft.daft import (
    FlightClientManager,
    FlightServerConnectionHandle,
    InProgressShuffleCache,
    PyExpr,
    ShuffleCache,
    start_flight_server,
)
from daft.execution.execution_step import (
    PartitionTask,
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.partitioning import PartitionMetadata
from daft.runners.ray_runner import _ray_num_cpus_provider

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger(__name__)

try:
    import ray
    import ray.experimental
    import ray.util.scheduling_strategies
    from ray._private.state import total_resources_per_node
except ImportError:
    logger.error(
        "Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://docs.getdaft.io/en/latest/install)"
    )
    raise


@ray.remote(num_cpus=0)
class ShuffleActorManager:
    """ShuffleActorManager manages the shuffle actors.

    It is responsible for:
    - Creating shuffle actors
    - Routing partitions to the correct actor based on the object location
    - Waiting for all the partitions to be pushed to the actors
    - Sending clear partition requests to the actors
    - Shutting down the actors
    """

    def __init__(
        self,
        shuffle_stage_id: int,
        storage_dirs: list[str],
        num_output_partitions: int,
        partition_by: list[PyExpr] | None = None,
    ):
        self.shuffle_stage_id = shuffle_stage_id
        self.storage_dirs = storage_dirs
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        self.all_actors: dict[str, ray.actor.ActorHandle] = {}
        self.active_actors: dict[str, ray.actor.ActorHandle] = {}

        self.fetch_partition_futures: list[ray.ObjectRef] = []
        self.clear_partition_futures: list[ray.ObjectRef] = []

        # Eagerly create actors for all nodes that are currently available (with cpu)
        for node in ray.nodes():
            if "Resources" in node and "CPU" in node["Resources"] and node["Resources"]["CPU"] > 0:
                self.get_or_create_actor(node["NodeID"])

    # Get or create an actor for a given node id
    def get_or_create_actor(self, node_id: str) -> ray.actor.ActorHandle:
        if node_id not in self.all_actors:
            num_cpus = total_resources_per_node()[node_id]["CPU"]
            actor = ShuffleActor.options(  # type: ignore[attr-defined]
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False,  # TODO: check if this is dangerous, if so, check if we can do true
                ),
                max_concurrency=int(num_cpus * 2),
            ).remote(
                ray.get_runtime_context().current_actor,
                self.shuffle_stage_id,
                self.storage_dirs,
                self.num_output_partitions,
                self.partition_by,
            )
            self.all_actors[node_id] = actor
        return self.all_actors[node_id]

    def get_active_actors(self) -> list[ray.actor.ActorHandle]:
        return list(self.active_actors.values())

    def get_active_node_ids(self) -> list[str]:
        return list(self.active_actors.keys())

    def get_active_actor_addresses(self) -> list[str]:
        return ray.get([self.active_actors[node_id].get_address.remote() for node_id in self.active_actors])

    def get_all_actors(self) -> list[ray.actor.ActorHandle]:
        return list(self.all_actors.values())

    def get_all_node_ids(self) -> list[str]:
        return list(self.all_actors.keys())

    def get_all_actor_addresses(self) -> list[str]:
        return ray.get([self.all_actors[node_id].get_address.remote() for node_id in self.all_actors])

    # Push an object to an actor
    def _push_objects_to_actor(self, node_id: str, objects: list[ray.ObjectRef]) -> ray.ObjectRef:
        actor = self.get_or_create_actor(node_id)
        self.active_actors[node_id] = actor
        return actor.push_partitions.remote(*objects)

    # Push a list of objects to the actors
    def push_objects(self, objects: list[ray.ObjectRef]) -> None:
        object_locations = ray.experimental.get_object_locations(objects)
        assert len(object_locations) == len(objects)

        objects_to_push_per_node = defaultdict(list)
        for object, object_location in object_locations.items():
            if object_location is None or "node_ids" not in object_location or not object_location["node_ids"]:
                # if we don't know where the object is, push it to a random actor
                node_id = random.choice(list(self.all_actors.keys()))
                objects_to_push_per_node[node_id].append(object)
            else:
                # there might be multiple nodes that have the object, so we push it to a random one.
                if len(object_location["node_ids"]) > 1:
                    node_id = random.choice(object_location["node_ids"])
                else:
                    [node_id] = object_location["node_ids"]
                objects_to_push_per_node[node_id].append(object)

        ray.get(
            [self._push_objects_to_actor(node_id, objects) for node_id, objects in objects_to_push_per_node.items()]
        )

    # Wait for all shuffle actors to finish pushing objects to the cache
    def finish_push_objects(self) -> list[PartitionMetadata]:
        metadatas_per_actor = []
        for actor in self.active_actors.values():
            metadatas_per_actor.append(
                actor.finish_push_partitions.options(num_returns=self.num_output_partitions).remote()
            )

        metadatas_per_actor = [ray.get(metadatas) for metadatas in metadatas_per_actor]

        merged_metadatas = []
        for metadatas in zip(*metadatas_per_actor):
            rows = sum([metadata.num_rows for metadata in metadatas])
            bytes = sum([metadata.size_bytes for metadata in metadatas])
            merged_metadatas.append(PartitionMetadata(num_rows=rows, size_bytes=bytes))
        return merged_metadatas

    # Clear the given partitions from the shuffle actors
    def clear_partition(self, partition_idx: int) -> None:
        self.clear_partition_futures.extend(
            self.active_actors[node_id].clear_partition.remote(partition_idx) for node_id in self.active_actors
        )

    # Shutdown the shuffle actor manager and all the shuffle actors
    def shutdown(self) -> None:
        ray.get(self.clear_partition_futures)
        self.clear_partition_futures.clear()

        ray.get([actor.shutdown.remote() for actor in self.all_actors.values()])
        for actor in self.all_actors.values():
            ray.kill(actor)
        self.all_actors.clear()


@ray.remote(num_cpus=0)
class ShuffleActor:
    """ShuffleActor is a per-node actor that manages the map partitions produced on that node.

    It is responsible for:
    - Partitioning the input data into num_partitions
    - Saving the partitions to a cache (for now, it defaults write to disk)
    - Serving a Flight server to fetch the partitions for the reduce phase
    - Cleaning up the cache when the reduce phase is done
    - Shutting down the Flight server
    """

    def __init__(
        self,
        shuffle_actor_manager: ray.actor.ActorHandle,
        shuffle_stage_id: int,
        storage_dirs: list[str],
        num_output_partitions: int,
        partition_by: list[PyExpr] | None = None,
    ):
        self.shuffle_actor_manager = shuffle_actor_manager
        self.node_id = ray.get_runtime_context().get_node_id()
        self.host = ray.util.get_node_ip_address()

        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        # create a shuffle cache to store the partitions
        self.in_progress_shuffle_cache: InProgressShuffleCache | None = InProgressShuffleCache.try_new(
            num_output_partitions,
            storage_dirs,
            self.node_id,
            shuffle_stage_id,
            target_filesize=1024 * 1024 * 10,
            compression=None,
            partition_by=partition_by,
        )
        self.shuffle_cache: ShuffleCache | None = None
        self.client_manager: FlightClientManager | None = None

        self.server: FlightServerConnectionHandle | None = None
        self.port: int | None = None

    def get_address(self) -> str:
        return f"grpc://{self.host}:{self.port}"

    # Push a partition to the shuffle cache asynchronously
    async def push_partitions(self, *partitions: MicroPartition) -> None:
        assert self.in_progress_shuffle_cache is not None, "Shuffle cache not initialized"
        await self.in_progress_shuffle_cache.push_partitions([partition._micropartition for partition in partitions])

    # Wait for all the push partition futures to complete
    async def finish_push_partitions(self) -> list[PartitionMetadata]:
        assert self.in_progress_shuffle_cache is not None, "Shuffle cache not initialized"
        self.shuffle_cache = await self.in_progress_shuffle_cache.close()
        self.server = start_flight_server(self.shuffle_cache, self.host)
        self.port = self.server.port()
        self.in_progress_shuffle_cache = None
        rows_per_partition = self.shuffle_cache.rows_per_partition()
        bytes_per_partition = self.shuffle_cache.bytes_per_partition()
        metadatas = [
            PartitionMetadata(num_rows=rows, size_bytes=bytes)
            for rows, bytes in zip(rows_per_partition, bytes_per_partition)
        ]
        return metadatas

    def initialize_client_manager(self, addresses: list[str], num_parallel_fetches: int = 2) -> None:
        assert self.client_manager is None, "Client manager already initialized"
        assert self.shuffle_cache is not None, "Shuffle cache not initialized"
        schema = self.shuffle_cache.schema()
        self.client_manager = FlightClientManager(addresses, num_parallel_fetches, schema)

    async def fetch_partition(self, partition_idx: int) -> tuple[MicroPartition, ray.ObjectRef]:
        assert self.client_manager is not None, "Client manager not initialized"
        py_mp = await self.client_manager.fetch_partition(partition_idx)
        clear_partition_future = self.shuffle_actor_manager.clear_partition.remote(partition_idx)
        return MicroPartition._from_pymicropartition(py_mp), clear_partition_future

    # Clean up the shuffle files for the given partition
    def clear_partition(self, partition_idx: int) -> None:
        if self.shuffle_cache is not None:
            try:
                self.shuffle_cache.clear_partition(partition_idx)
            except Exception as e:
                print(f"failed to clean up shuffle files: {e} for partition {partition_idx}")

    # Shutdown the shuffle actor
    def shutdown(self) -> None:
        if self.server is not None:
            self.server.shutdown()
        if self.shuffle_cache is not None:
            try:
                self.shuffle_cache.clear_directories()
            except Exception as e:
                print(f"failed to clean up shuffle files: {e}")


def run_map_phase(
    plan: InProgressPhysicalPlan[ray.ObjectRef],
    map_stage_id: int,
    shuffle_actor_manager: ray.actor.ActorHandle,
) -> Generator[
    None | PartitionTask[ray.ObjectRef] | PartitionTaskBuilder[ray.ObjectRef], None, list[PartitionMetadata]
]:
    # Maps tasks we have not emitted
    pending_map_tasks: deque[SingleOutputPartitionTask[ray.ObjectRef]] = deque()
    # Maps tasks we have emitted but not yet completed
    in_flight_map_tasks: dict[str, SingleOutputPartitionTask[ray.ObjectRef]] = {}

    # Collect all the map tasks
    for step in plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.finalize_partition_task_single_output(map_stage_id)
            pending_map_tasks.append(step)
        else:
            yield step

    while pending_map_tasks or in_flight_map_tasks:
        # Get all the map tasks that are done
        done_ids = [id for id in in_flight_map_tasks.keys() if in_flight_map_tasks[id].done()]

        # If there are any map tasks that are done, push them to the actor manager
        if len(done_ids) > 0:
            done_partitions = [in_flight_map_tasks.pop(id).partition() for id in done_ids]
            ray.get(shuffle_actor_manager.push_objects.remote(done_partitions))
        # If there are no map tasks that are done, try to emit more map tasks. Else, yield None to indicate to the scheduler that we need to wait
        else:
            # Max number of maps to emit per wave
            batch_size = next(_ray_num_cpus_provider())
            available_to_yield = min(batch_size - len(in_flight_map_tasks), len(pending_map_tasks))
            if available_to_yield > 0:
                for _ in range(available_to_yield):
                    map_task = pending_map_tasks.popleft()
                    in_flight_map_tasks[map_task.id()] = map_task
                    yield map_task
            else:
                yield None

    # Wait for all actors to complete the map phase
    metadatas = ray.get(shuffle_actor_manager.finish_push_objects.remote())
    return metadatas


def run_reduce_phase(
    num_output_partitions: int,
    shuffle_actor_manager: ray.actor.ActorHandle,
    metadatas: list[PartitionMetadata],
) -> Generator[PartitionTaskBuilder[ray.ObjectRef], None, None]:
    # only the active actors have data from the map
    active_actor_addresses = ray.get(shuffle_actor_manager.get_active_actor_addresses.remote())
    # but we can do reduce on all actors
    all_actors = ray.get(shuffle_actor_manager.get_all_actors.remote())

    # initialize the client manager for all actors
    ray.get([actor.initialize_client_manager.remote(active_actor_addresses) for actor in all_actors])

    clear_partition_futures = []
    for partition_idx in range(num_output_partitions):
        partition_ref, clear_partition_future = (
            all_actors[partition_idx % len(all_actors)].fetch_partition.options(num_returns=2).remote(partition_idx)
        )
        clear_partition_futures.append(clear_partition_future)
        yield PartitionTaskBuilder(
            inputs=[partition_ref],
            partial_metadatas=[metadatas[partition_idx]],
        )

    # we need to wait for all the clear partition futures to complete before shutting down the actor manager
    ray.get(clear_partition_futures)
    ray.get(shuffle_actor_manager.shutdown.remote())
    ray.kill(shuffle_actor_manager)


def flight_shuffle(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    num_output_partitions: int,
    shuffle_dirs: list[str],
    partition_by: list[PyExpr] | None = None,
) -> Generator[None | PartitionTask[ray.ObjectRef] | PartitionTaskBuilder[ray.ObjectRef], None, None]:
    map_stage_id = next(stage_id_counter)
    shuffle_stage_id = next(stage_id_counter)
    # Try to schedule the manager on the current node, which should be the head node
    shuffle_actor_manager = ShuffleActorManager.options(  # type: ignore[attr-defined]
        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    ).remote(
        shuffle_stage_id,
        shuffle_dirs,
        num_output_partitions,
        partition_by,
    )

    # Run the map phase
    metadatas = yield from run_map_phase(
        fanout_plan,
        map_stage_id,
        shuffle_actor_manager,
    )

    # Run the reduce phase
    yield from run_reduce_phase(num_output_partitions, shuffle_actor_manager, metadatas)
