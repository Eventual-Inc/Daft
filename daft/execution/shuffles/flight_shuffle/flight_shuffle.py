import concurrent.futures
import logging
import os
import random
import shutil
from collections import deque

from daft.daft import PyExpr
from daft.execution.execution_step import (
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.execution.shuffles.flight_shuffle.flight_server import FlightServer
from daft.execution.shuffles.flight_shuffle.utils import (
    PartitionCache,
    get_shuffle_file_path,
)
from daft.expressions.expressions import Expression, ExpressionsProjection
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


@ray.remote(num_cpus=0)
class ShuffleActorManager:
    def __init__(
        self,
        shuffle_stage_id: int,
        num_output_partitions: int,
        partition_by: ExpressionsProjection | None = None,
    ):
        self.shuffle_stage_id = shuffle_stage_id
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        self.all_actors: dict[str, ShuffleActor] = {}
        self.used_actors: dict[str, ShuffleActor] = {}

        self.push_object_futures = []
        self.clear_partition_futures = []

        for node in ray.nodes():
            self.get_or_create_actor(node["NodeID"])

    def get_or_create_actor(self, node_id: str):
        if node_id not in self.all_actors:
            actor = ShuffleActor.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False,  # TODO: check if this is dangerous, if so, check if we can do true
                ),
            ).remote(self.shuffle_stage_id, self.num_output_partitions, self.partition_by)
            self.all_actors[node_id] = actor
        return self.all_actors[node_id]

    def push_object_to_actor(self, node_id: str, object: ray.ObjectRef) -> ray.ObjectRef:
        actor = self.get_or_create_actor(node_id)
        self.used_actors[node_id] = actor
        return actor.push_partition.remote(object)

    def push_objects(self, objects: list[ray.ObjectRef]):
        object_locations = ray.experimental.get_local_object_locations(objects)
        assert len(object_locations) == len(objects)

        for object, object_location in object_locations.items():
            if object_location is None or "node_ids" not in object_location or not object_location["node_ids"]:
                # if we don't know where the object is, push it to a random actor
                node_id = random.choice(list(self.all_actors.keys()))
                self.push_object_futures.append(self.push_object_to_actor(node_id, object))
            else:
                self.push_object_futures.append(self.push_object_to_actor(object_location["node_ids"][0], object))

    def finish_map(self):
        ray.wait(
            self.push_object_futures,
            num_returns=len(self.push_object_futures),
            fetch_local=False,
        )
        self.push_object_futures.clear()
        ray.get([actor.finish_map.remote() for actor in self.used_actors.values()])
        return

    def get_node_ids(self):
        return list(self.used_actors.keys())

    def get_actor_addresses(self):
        return ray.get([self.used_actors[node_id].get_address.remote() for node_id in self.used_actors])

    def clear_partitions(self, partition_idxs: list[int]):
        self.clear_partition_futures.extend(
            self.used_actors[node_id].clear_partitions.remote(partition_idxs) for node_id in self.used_actors
        )

    def shutdown(self):
        ray.get(self.clear_partition_futures)
        ray.get([actor.shutdown.remote() for actor in self.used_actors.values()])


@ray.remote(num_cpus=0)
class ShuffleActor:
    def __init__(
        self,
        shuffle_stage_id: int,
        num_output_partitions: int,
        partition_by: ExpressionsProjection | None = None,
    ):
        self.shuffle_stage_id = shuffle_stage_id
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        self.node_id = ray.get_runtime_context().get_node_id()
        self.host = ray.util.get_node_ip_address()

        self.partition_cache = PartitionCache(
            self.node_id,
            self.shuffle_stage_id,
            num_output_partitions,
            partition_by=partition_by,
        )

        self.server = FlightServer(self.host, self.node_id, self.shuffle_stage_id, self.partition_cache)
        self.port = self.server.get_port()

        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.push_partition_futures = []

        # clean up any existing shuffle files
        if os.path.exists(get_shuffle_file_path(self.node_id, self.shuffle_stage_id)):
            shutil.rmtree(get_shuffle_file_path(self.node_id, self.shuffle_stage_id))

        # initialize directories
        for partition_idx in range(num_output_partitions):
            os.makedirs(
                get_shuffle_file_path(self.node_id, self.shuffle_stage_id, partition_idx),
                exist_ok=True,
            )

    def get_address(self):
        return f"grpc://{self.host}:{self.port}"

    def push_partition(self, partition: MicroPartition):
        self.push_partition_futures.append(self.executor.submit(self.partition_cache.push_partition, partition))

    def finish_map(self):
        for future in concurrent.futures.as_completed(self.push_partition_futures):
            future.result()
        self.push_partition_futures.clear()

        file_bytes_per_partition = self.partition_cache.flush_partitions()
        return file_bytes_per_partition

    def clear_partitions(self, partition_idxs: list[int]):
        for partition_idx in partition_idxs:
            path = get_shuffle_file_path(self.node_id, self.shuffle_stage_id, partition_idx)
            try:
                if os.path.exists(path):
                    shutil.rmtree(path)
            except Exception as e:
                print(f"failed to clean up shuffle files: {e} for partition {partition_idx}")

    def shutdown(self):
        self.server.shutdown()
        try:
            path = get_shuffle_file_path(self.node_id, self.shuffle_stage_id)
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as e:
            print(f"failed to clean up shuffle files: {e}")


@ray.remote
def reduce_partitions(
    shuffle_stage_id: int,
    partitions: list[int],
    shuffle_actor_addresses: list[str],
    shuffle_actor_manager: ShuffleActorManager,
    max_parallel_fetches: int = 2,
):
    import asyncio

    from daft.dependencies import flight

    def fetch_partition_from_actor(client: flight.FlightClient, partition_idx: int):
        ticket = flight.Ticket(f"{shuffle_stage_id},{partition_idx}".encode())
        reader = client.do_get(ticket)
        res = MicroPartition.from_arrow(reader.read_all())
        return res

    async def fetch_all_partitions(
        clients: list[flight.FlightClient],
        partition_idxs: list[int],
        executor: concurrent.futures.ThreadPoolExecutor,
        queue: asyncio.Queue,
    ):
        for partition_idx in partition_idxs:
            futures = [executor.submit(fetch_partition_from_actor, client, partition_idx) for client in clients]
            await queue.put((partition_idx, futures))
        await queue.put(None)

    clients = [flight.FlightClient(address) for address in shuffle_actor_addresses]

    async def gen_partitions():
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            queue = asyncio.Queue(maxsize=max_parallel_fetches)
            fetch_task = asyncio.create_task(fetch_all_partitions(clients, partitions, executor, queue))

            while True:
                next_item = await queue.get()
                if next_item is None:
                    break
                partition_idx, futures = next_item

                def wait_for_futures(futures):
                    return [future.result() for future in concurrent.futures.as_completed(futures)]

                ready = await loop.run_in_executor(
                    executor,
                    wait_for_futures,
                    futures,
                )
                yield (partition_idx, ready)

            await fetch_task

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Get the async generator
    async_gen = gen_partitions()

    try:
        while True:
            try:
                partition_idx, ready = loop.run_until_complete(async_gen.__anext__())
                concated = MicroPartition.concat(ready)
                yield concated
                shuffle_actor_manager.clear_partitions.remote([partition_idx])
                metadata = PartitionMetadata.from_table(concated)
                yield metadata

            except StopAsyncIteration:
                break
    finally:
        # close clients
        for client in clients:
            client.close()
        loop.close()


# Distribute partitions in round robin fashion across reduce tasks
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
    # Max number of maps to emit per wave
    batch_size = next(_ray_num_cpus_provider())
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

    push_objects_futures = []
    while pending_map_tasks or in_flight_map_tasks:
        # Get all the map tasks that are done
        done_ids = [id for id in in_flight_map_tasks.keys() if in_flight_map_tasks[id].done()]

        # If there are any map tasks that are done, push them to the actor manager
        if len(done_ids) > 0:
            done_partitions = []
            for id in done_ids:
                materialization = in_flight_map_tasks.pop(id)
                done_partitions.append(materialization.partition())

            push_objects_futures.append(shuffle_actor_manager.push_objects.remote(done_partitions))
        # If there are no map tasks that are done, try to emit more map tasks. Else, yield None to indicate to the scheduler that we need to wait
        else:
            available_to_yield = min(batch_size - len(in_flight_map_tasks), len(pending_map_tasks))
            if available_to_yield > 0:
                for _ in range(available_to_yield):
                    map_task = pending_map_tasks.popleft()
                    in_flight_map_tasks[map_task.id()] = map_task
                    yield map_task
            else:
                yield None

    # All the map tasks have been emitted, so the actor manager to finish pushing them to the actors
    ray.wait(
        push_objects_futures,
        num_returns=len(push_objects_futures),
        fetch_local=False,
    )
    # Wait for all actors to complete the map phase
    ray.wait([shuffle_actor_manager.finish_map.remote()], fetch_local=False)


def run_reduce_phase(
    shuffle_stage_id: int,
    num_output_partitions: int,
    shuffle_actor_manager: ShuffleActorManager,
):
    # Calculate how many reduce tasks, and which partitions go to which reduce tasks
    actor_addresses, actor_node_ids = ray.get(
        [
            shuffle_actor_manager.get_actor_addresses.remote(),
            shuffle_actor_manager.get_node_ids.remote(),
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
                scheduling_strategy="SPREAD",
            ).remote(shuffle_stage_id, partitions, actor_addresses, shuffle_actor_manager)
        )

    # Collect the results from the reduce tasks, they are configured as ray generators that yield partition, metadata pairs
    for reduce_task_idx in partitions_to_reduce_task:
        reduce_gen = reduce_generators[reduce_task_idx]
        partition = next(reduce_gen)
        metadata = ray.get(next(reduce_gen))
        yield PartitionTaskBuilder(
            inputs=[partition],
            partial_metadatas=[metadata],
        )

    # Wait for all the reduce tasks to complete
    ray.wait(
        [reduce_gen.completed() for reduce_gen in reduce_generators],
        num_returns=len(reduce_generators),
        fetch_local=False,
    )
    # Shutdown the actor manager
    ray.wait([shuffle_actor_manager.shutdown.remote()], fetch_local=False)


def flight_shuffle(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    num_output_partitions: int,
    partition_by: list[PyExpr] | None = None,
):
    map_stage_id = next(stage_id_counter)
    shuffle_stage_id = next(stage_id_counter)

    # Try to schedule the manager on the current node, which should be the head node
    shuffle_actor_manager = ShuffleActorManager.options(
        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=True,
        )
    ).remote(
        shuffle_stage_id,
        num_output_partitions,
        (ExpressionsProjection([Expression._from_pyexpr(e) for e in partition_by]) if partition_by else None),
    )

    # Run the map phase
    yield from run_map_phase(
        fanout_plan,
        map_stage_id,
        shuffle_actor_manager,
    )

    # Run the reduce phase
    yield from run_reduce_phase(shuffle_stage_id, num_output_partitions, shuffle_actor_manager)
