import concurrent.futures
import logging
import os
import random
import shutil
from collections import defaultdict, deque
from typing import Optional

from daft.daft import InProgressShuffleCache, PyExpr, start_flight_server
from daft.execution.execution_step import (
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.partitioning import PartitionMetadata
from daft.runners.ray_runner import _ray_num_cpus_provider

logger = logging.getLogger(__name__)

try:
    import ray
    import ray.experimental
    import ray.util.scheduling_strategies
except ImportError:
    logger.error(
        "Error when importing Ray. Please ensure that getdaft was installed with the Ray extras tag: getdaft[ray] (https://www.getdaft.io/projects/docs/en/latest/learn/install.html)"
    )
    raise


# Get the shuffle directories for the given node and shuffle stage
def get_shuffle_dirs(
    shuffle_dirs: list[str],
    node_id: str,
    shuffle_stage_id: int,
) -> list[str]:
    if not shuffle_dirs:
        raise ValueError("No shuffle directories provided")
    return [f"{dir}/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}" for dir in shuffle_dirs]


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
        shuffle_dirs: list[str],
        num_output_partitions: int,
        partition_by: Optional[list[PyExpr]] = None,
    ):
        self.shuffle_stage_id = shuffle_stage_id
        self.shuffle_dirs = shuffle_dirs
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        self.all_actors: dict[str, ShuffleActor] = {}
        self.active_actors: dict[str, ShuffleActor] = {}

        self.clear_partition_futures: list[ray.ObjectRef] = []

        # Eagerly create actors for all nodes that are currently available
        for node in ray.nodes():
            self.get_or_create_actor(node["NodeID"])

    # Get or create an actor for a given node id
    def get_or_create_actor(self, node_id: str):
        if node_id not in self.all_actors:
            actor = ShuffleActor.options(  # type: ignore[attr-defined]
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False,  # TODO: check if this is dangerous, if so, check if we can do true
                ),
            ).remote(
                self.shuffle_stage_id,
                self.shuffle_dirs,
                self.num_output_partitions,
                self.partition_by,
            )
            self.all_actors[node_id] = actor
        return self.all_actors[node_id]

    def get_active_node_ids(self):
        return list(self.active_actors.keys())

    def get_active_actor_addresses(self):
        return ray.get([self.active_actors[node_id].get_address.remote() for node_id in self.active_actors])

    def get_all_node_ids(self):
        return list(self.all_actors.keys())

    def get_all_actor_addresses(self) -> list[str]:
        return ray.get([self.all_actors[node_id].get_address.remote() for node_id in self.all_actors])  # type: ignore[attr-defined]

    # Push an object to an actor
    def _push_objects_to_actor(self, node_id: str, objects: list[ray.ObjectRef]) -> ray.ObjectRef:
        actor = self.get_or_create_actor(node_id)
        self.active_actors[node_id] = actor
        return actor.push_partitions.remote(*objects)

    # Push a list of objects to the actors
    def push_objects(self, objects: list[ray.ObjectRef]):
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
    def finish_push_objects(self):
        ray.get([actor.finish_push_partitions.remote() for actor in self.active_actors.values()])

    # Clear the given partitions from the shuffle actors
    def clear_partition(self, partition_idx: int):
        self.clear_partition_futures.extend(
            self.active_actors[node_id].clear_partition.remote(partition_idx)  # type: ignore[attr-defined]
            for node_id in self.active_actors
        )

    # Shutdown the shuffle actor manager and all the shuffle actors
    def shutdown(self):
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
        shuffle_stage_id: int,
        shuffle_dirs: list[str],
        num_output_partitions: int,
        partition_by: Optional[list[PyExpr]] = None,
    ):
        self.node_id = ray.get_runtime_context().get_node_id()
        self.host = ray.util.get_node_ip_address()

        self.shuffle_dirs = get_shuffle_dirs(shuffle_dirs, self.node_id, shuffle_stage_id)
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        # create a shuffle cache to store the partitions
        self.in_progress_shuffle_cache = InProgressShuffleCache.try_new(
            num_output_partitions,
            self.shuffle_dirs,
            target_filesize=1024 * 1024 * 10,
            compression=None,
            partition_by=partition_by,
        )

        self.server = None
        self.port = None

    def get_address(self):
        return f"grpc://{self.host}:{self.port}"

    # Push a partition to the shuffle cache asynchronously
    async def push_partitions(self, *partitions: MicroPartition):
        await self.in_progress_shuffle_cache.push_partitions([partition._micropartition for partition in partitions])

    # Wait for all the push partition futures to complete
    async def finish_push_partitions(self):
        shuffle_cache = await self.in_progress_shuffle_cache.close()
        self.server = start_flight_server(shuffle_cache, self.host)
        self.port = self.server.port()
        self.in_progress_shuffle_cache = None

    # Clean up the shuffle files for the given partition
    def clear_partition(self, partition_idx: int):
        for shuffle_dir in self.shuffle_dirs:
            path = os.path.join(shuffle_dir, f"partition_{partition_idx}")
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as e:
            print(f"failed to clean up shuffle files: {e} for partition {partition_idx}")

    # Shutdown the shuffle actor
    def shutdown(self):
        if self.server is not None:
            self.server.shutdown()
        try:
            for shuffle_dir in self.shuffle_dirs:
                if os.path.exists(shuffle_dir):
                    shutil.rmtree(shuffle_dir)
        except Exception as e:
            print(f"failed to clean up shuffle files: {e}")


@ray.remote(num_cpus=0)
def reduce_partitions(
    partitions: list[int],
    shuffle_actor_addresses: list[str],
    shuffle_actor_manager: ShuffleActorManager,
    max_parallel_fetches: int = 2,
    max_workers: int = 4,
):
    """reduce_partitions is task that produces reduced partitions.

    It is responsible for:
    - Fetching the partitions from the shuffle actors
    - Concatenating the partitions
    - Yielding the concatenated partition and its metadata

    We submit the requests in parallel on a threadpool with an async queue as a backpressure mechanism.
    """
    import asyncio

    from daft.dependencies import flight

    # Fetch a single partition from a single actor
    def fetch_partition_from_actor(client: flight.FlightClient, partition_idx: int):
        ticket = flight.Ticket(f"{partition_idx}".encode())
        reader = client.do_get(ticket)
        res = MicroPartition.from_arrow(reader.read_all())
        return res

    # Fetch all the partitions from all the actors
    async def fetch_all_partitions(
        clients: list[flight.FlightClient],
        partition_idxs: list[int],
        executor: concurrent.futures.ThreadPoolExecutor,
        queue: asyncio.Queue,
    ):
        for partition_idx in partition_idxs:
            # Submit all the fetch requests to the thread pool
            futures = [executor.submit(fetch_partition_from_actor, client, partition_idx) for client in clients]
            # Put the futures in the queue, the queue is used as a backpressure mechanism
            await queue.put((partition_idx, futures))
        # Put None in the queue to indicate that the fetch is done
        await queue.put(None)

    clients = [flight.FlightClient(address) for address in shuffle_actor_addresses]

    async def gen_partitions():
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            queue = asyncio.Queue(maxsize=max_parallel_fetches)
            fetch_task = asyncio.create_task(fetch_all_partitions(clients, partitions, executor, queue))

            while True:
                next_item = await queue.get()
                if next_item is None:
                    break
                partition_idx, futures = next_item

                def wait_for_futures(futures):
                    return [future.result() for future in concurrent.futures.as_completed(futures)]

                # Wait for all the futures to complete, run in threadpool so we don't block the event loop
                to_reduce = await loop.run_in_executor(
                    executor,
                    wait_for_futures,
                    futures,
                )
                yield (partition_idx, to_reduce)

            await fetch_task

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async_partition_gen = gen_partitions()
    clear_partitions_futures = []
    try:
        while True:
            try:
                partition_idx, to_reduce = loop.run_until_complete(async_partition_gen.__anext__())
                reduced = MicroPartition.concat(to_reduce)
                metadata = PartitionMetadata.from_table(reduced)

                yield reduced
                clear_partitions_futures.append(shuffle_actor_manager.clear_partition.remote(partition_idx))  # type: ignore[attr-defined]
                yield metadata

            except StopAsyncIteration:
                break
    finally:
        for client in clients:
            client.close()
        loop.close()
        ray.get(clear_partitions_futures)


# Figure out which partitions go to which reduce tasks
# This is a simple round robin assignment
def optimize_reduce_partition_assignment(actor_node_ids, num_partitions):
    # Assign 2x number of reduce tasks as there are actors, this seems to work alright in practice
    num_reduce_tasks = len(actor_node_ids) * 2

    # Map partitions to reduce tasks in round robin fashion
    partition_to_reduce_task = [partition_idx % num_reduce_tasks for partition_idx in range(num_partitions)]

    # Create the reverse mapping
    reduce_task_to_partitions = [[] for _ in range(num_reduce_tasks)]
    for partition_idx, reduce_task_idx in enumerate(partition_to_reduce_task):
        reduce_task_to_partitions[reduce_task_idx].append(partition_idx)

    return reduce_task_to_partitions, partition_to_reduce_task


def run_map_phase(
    plan: InProgressPhysicalPlan[ray.ObjectRef],
    map_stage_id: int,
    shuffle_actor_manager: ShuffleActorManager,
):
    # Maps tasks we have not emitted
    pending_map_tasks: deque[SingleOutputPartitionTask] = deque()
    # Maps tasks we have emitted but not yet completed
    in_flight_map_tasks: dict[str, SingleOutputPartitionTask] = {}

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
            ray.get(shuffle_actor_manager.push_objects.remote(done_partitions))  # type: ignore[attr-defined]
            del done_partitions
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
    ray.get(shuffle_actor_manager.finish_push_objects.remote())  # type: ignore[attr-defined]


def run_reduce_phase(
    num_output_partitions: int,
    shuffle_actor_manager: ShuffleActorManager,
):
    # Calculate how many reduce tasks, and which partitions go to which reduce tasks
    actor_addresses, actor_node_ids = ray.get(
        [
            shuffle_actor_manager.get_active_actor_addresses.remote(),  # type: ignore[attr-defined]
            shuffle_actor_manager.get_active_node_ids.remote(),  # type: ignore[attr-defined]
        ],
    )
    reduce_task_to_partitions, partitions_to_reduce_task = optimize_reduce_partition_assignment(
        actor_node_ids, num_output_partitions
    )

    # Launch the reduce tasks
    reduce_generators = []
    for reduce_task_idx, partitions in enumerate(reduce_task_to_partitions):
        reduce_generators.append(
            reduce_partitions.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=actor_node_ids[reduce_task_idx % len(actor_node_ids)],
                    soft=False,
                )
            ).remote(partitions, actor_addresses, shuffle_actor_manager)
        )

    # Collect the results from the reduce tasks, they are configured as ray generators that yield partition, and then metadata
    for reduce_task_idx in partitions_to_reduce_task:
        reduce_gen = reduce_generators[reduce_task_idx]
        partition = next(reduce_gen)
        metadata = ray.get(next(reduce_gen))
        yield PartitionTaskBuilder(
            inputs=[partition],
            partial_metadatas=[metadata],
        )
    # Wait for all the reduce tasks to complete
    ray.get([reduce_gen.completed() for reduce_gen in reduce_generators])
    del reduce_generators
    # Shutdown the actor manager
    ray.get(shuffle_actor_manager.shutdown.remote())  # type: ignore[attr-defined]
    # Kill the actor manager
    ray.kill(shuffle_actor_manager)


def flight_shuffle(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    num_output_partitions: int,
    shuffle_dirs: list[str],
    partition_by: Optional[list[PyExpr]] = None,
):
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
    yield from run_map_phase(
        fanout_plan,
        map_stage_id,
        shuffle_actor_manager,
    )

    # Run the reduce phase
    yield from run_reduce_phase(num_output_partitions, shuffle_actor_manager)
