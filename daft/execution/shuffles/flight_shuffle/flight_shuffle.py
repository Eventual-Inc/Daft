import concurrent.futures
import logging
import os
import random
import shutil
from collections import defaultdict, deque
import time
import gc
import asyncio
from daft.daft import (
    InProgressShuffleCache,
    PyExpr,
    ShuffleFlightClientManager,
    fetch_partitions_from_flight,
    start_flight_server,
)
from daft.execution import execution_step
from daft.execution.execution_step import (
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.expressions.expressions import col
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
    return [
        f"{dir}/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}"
        for dir in shuffle_dirs
    ]


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
        partition_by: list[PyExpr] | None = None,
    ):
        self.shuffle_stage_id = shuffle_stage_id
        self.shuffle_dirs = shuffle_dirs
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        self.all_actors: dict[str, ShuffleActor] = {}
        self.active_actors: dict[str, ShuffleActor] = {}

        self.clear_partition_futures = []

        # Eagerly create actors for all nodes that are currently available
        for node in ray.nodes():
            self.get_or_create_actor(node["NodeID"])

    # Get or create an actor for a given node id
    def get_or_create_actor(self, node_id: str):
        if node_id not in self.all_actors:
            actor = ShuffleActor.options(
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

    def get_active_actors(self):
        return list(self.active_actors.values())

    def get_active_node_ids(self):
        return list(self.active_actors.keys())

    def get_active_actor_addresses(self):
        return ray.get(
            [
                self.active_actors[node_id].get_address.remote()
                for node_id in self.active_actors
            ]
        )

    def get_all_actors(self):
        return list(self.all_actors.values())

    def get_all_node_ids(self):
        return list(self.all_actors.keys())

    def get_all_actor_addresses(self):
        return ray.get(
            [
                self.all_actors[node_id].get_address.remote()
                for node_id in self.all_actors
            ]
        )

    # Push an object to an actor
    def _push_objects_to_actor(
        self, node_id: str, objects: list[ray.ObjectRef]
    ) -> ray.ObjectRef:
        actor = self.get_or_create_actor(node_id)
        self.active_actors[node_id] = actor
        return actor.push_partitions.remote(*objects)

    # Push a list of objects to the actors
    def push_objects(self, objects: list[ray.ObjectRef]):
        object_locations = ray.experimental.get_object_locations(objects)
        assert len(object_locations) == len(objects)

        objects_to_push_per_node = defaultdict(list)
        for object, object_location in object_locations.items():
            if (
                object_location is None
                or "node_ids" not in object_location
                or not object_location["node_ids"]
            ):
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
            [
                self._push_objects_to_actor(node_id, objects)
                for node_id, objects in objects_to_push_per_node.items()
            ]
        )

    # Wait for all shuffle actors to finish pushing objects to the cache
    def finish_push_objects(self):
        ray.get(
            [
                actor.finish_push_partitions.remote()
                for actor in self.active_actors.values()
            ]
        )

    # Clear the given partitions from the shuffle actors
    def clear_partition(self, partition_idx: int):
        self.clear_partition_futures.extend(
            self.active_actors[node_id].clear_partition.remote(partition_idx)
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
        partition_by: list[PyExpr] | None = None,
    ):
        self.node_id = ray.get_runtime_context().get_node_id()
        self.host = ray.util.get_node_ip_address()

        self.shuffle_dirs = get_shuffle_dirs(
            shuffle_dirs, self.node_id, shuffle_stage_id
        )
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
        await self.in_progress_shuffle_cache.push_partitions(
            [partition._micropartition for partition in partitions]
        )

    # Wait for all the push partition futures to complete
    async def finish_push_partitions(self):
        shuffle_cache = await self.in_progress_shuffle_cache.close()
        self.server = start_flight_server(shuffle_cache, self.host)
        self.port = self.server.port()
        self.in_progress_shuffle_cache = None

    def initialize_client_manager(
        self, addresses: list[str], num_parallel_fetches: int = 2
    ):
        self.client_manager = ShuffleFlightClientManager(
            addresses, num_parallel_fetches
        )

    async def fetch_partition(self, partition_idx: int):
        assert self.client_manager is not None, "Client manager not initialized"
        py_mp = await self.client_manager.fetch_partition(partition_idx)
        return MicroPartition._from_pymicropartition(
            py_mp
        ), PartitionMetadata.from_table(py_mp)

    async def fetch_partition2(
        self,
        partitions: list[int],
        shuffle_actor_addresses: list[str],
        shuffle_actor_manager: ShuffleActorManager,
        max_parallel_fetches: int = 2,
    ):
        try:
            clear_partitions_futures = []
            partition_stream = fetch_partitions_from_flight(
                shuffle_actor_addresses, partitions, max_parallel_fetches
            )
            partition_idx = 0
            while True:
                py_micropartition = await partition_stream.next()
                if py_micropartition is None:
                    break
                micropartition = MicroPartition._from_pymicropartition(
                    py_micropartition
                )
                metadata = PartitionMetadata.from_table(micropartition)
                yield micropartition
                clear_partitions_futures.append(
                    shuffle_actor_manager.clear_partition.remote(
                        partitions[partition_idx]
                    )
                )
                yield metadata
                partition_idx += 1
        finally:
            await asyncio.gather(*clear_partitions_futures)

    # Clean up the shuffle files for the given partition
    def clear_partition(self, partition_idx: int):
        gc.collect()
        for shuffle_dir in self.shuffle_dirs:
            path = os.path.join(shuffle_dir, f"partition_{partition_idx}")
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as e:
            print(
                f"failed to clean up shuffle files: {e} for partition {partition_idx}"
            )

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
class Reducer:
    def __init__(self):
        pass

    def reduce(
        self,
        partitions: list[int],
        shuffle_actor_addresses: list[str],
        shuffle_actor_manager: ShuffleActorManager,
        max_parallel_fetches: int = 2,
    ):
        try:
            clear_partitions_futures = []
            partition_stream = fetch_partitions_from_flight(
                shuffle_actor_addresses, partitions, max_parallel_fetches
            )
            for partition_idx, py_micropartition in enumerate(partition_stream):
                micropartition = MicroPartition._from_pymicropartition(
                    py_micropartition
                )
                metadata = PartitionMetadata.from_table(micropartition)
                yield micropartition
                del micropartition
                clear_partitions_futures.append(
                    shuffle_actor_manager.clear_partition.remote(
                        partitions[partition_idx]
                    )
                )
                yield metadata
                del metadata
        finally:
            ray.get(clear_partitions_futures)


@ray.remote(num_cpus=0)
def reduce_partitions(
    partitions: list[int],
    shuffle_actor_addresses: list[str],
    shuffle_actor_manager: ShuffleActorManager,
    max_parallel_fetches: int = 2,
):
    try:
        clear_partitions_futures = []
        partition_stream = fetch_partitions_from_flight(
            shuffle_actor_addresses, partitions, max_parallel_fetches
        )
        for partition_idx, py_micropartition in enumerate(partition_stream):
            micropartition = MicroPartition._from_pymicropartition(py_micropartition)
            metadata = PartitionMetadata.from_table(micropartition)
            yield micropartition
            del micropartition
            clear_partitions_futures.append(
                shuffle_actor_manager.clear_partition.remote(partitions[partition_idx])
            )
            yield metadata
            del metadata
    finally:
        ray.get(clear_partitions_futures)


# Figure out which partitions go to which reduce tasks
# This is a simple round robin assignment
def optimize_reduce_partition_assignment(actor_node_ids, num_partitions):
    # Assign 2x number of reduce tasks as there are actors, this seems to work alright in practice
    num_reduce_tasks = len(actor_node_ids) * 2

    # Map partitions to reduce tasks in round robin fashion
    partition_to_reduce_task = [
        partition_idx % num_reduce_tasks for partition_idx in range(num_partitions)
    ]

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
        done_ids = [
            id for id in in_flight_map_tasks.keys() if in_flight_map_tasks[id].done()
        ]

        # If there are any map tasks that are done, push them to the actor manager
        if len(done_ids) > 0:
            done_partitions = [
                in_flight_map_tasks.pop(id).partition() for id in done_ids
            ]
            ray.get(shuffle_actor_manager.push_objects.remote(done_partitions))
        # If there are no map tasks that are done, try to emit more map tasks. Else, yield None to indicate to the scheduler that we need to wait
        else:
            # Max number of maps to emit per wave
            batch_size = next(_ray_num_cpus_provider())
            available_to_yield = min(
                batch_size - len(in_flight_map_tasks), len(pending_map_tasks)
            )
            if available_to_yield > 0:
                for _ in range(available_to_yield):
                    map_task = pending_map_tasks.popleft()
                    in_flight_map_tasks[map_task.id()] = map_task
                    yield map_task
            else:
                yield None

    # Wait for all actors to complete the map phase
    ray.get(shuffle_actor_manager.finish_push_objects.remote())


def run_reduce_phase(
    num_output_partitions: int,
    shuffle_stage_id: int,
    shuffle_actor_manager: ShuffleActorManager,
):
    # actor_addresses = ray.get(shuffle_actor_manager.get_active_actor_addresses.remote())
    # print(f"actor_addresses: {actor_addresses}")
    # actors = ray.get(shuffle_actor_manager.get_active_actors.remote())
    # ray.get(
    #     [actor.initialize_client_manager.remote(actor_addresses) for actor in actors]
    # )

    # partition_gens = deque()
    # for partition_idx in range(num_output_partitions):
    #     partition_gen = actors[partition_idx % len(actors)].fetch_partition.remote(
    #         partition_idx
    #     )
    #     partition_gens.append(partition_gen)

    # while partition_gens:
    #     partition_gen = partition_gens.popleft()
    #     inputs = [obj for obj in partition_gen]
    #     yield PartitionTaskBuilder(
    #         inputs=inputs,
    #         partial_metadatas=None,
    #     ).add_instruction(execution_step.ReduceMerge())

    # actor_addresses = ray.get(shuffle_actor_manager.get_active_actor_addresses.remote())
    # actors = ray.get(shuffle_actor_manager.get_active_actors.remote())
    # node_ids = ray.get(shuffle_actor_manager.get_active_node_ids.remote())
    # actor_to_node_id = {actor: node_id for actor, node_id in zip(actors, node_ids)}
    # actor_to_partitions = defaultdict(list)
    # for partition_idx in range(num_output_partitions):
    #     actor_to_partitions[actors[partition_idx % len(actors)]].append(partition_idx)

    # reduce_generators = []
    # reducers = []
    # for actor, partitions in actor_to_partitions.items():
    #     reducer = Reducer.options(
    #         scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
    #             node_id=actor_to_node_id[actor],
    #             soft=False,
    #         )
    #     ).remote()
    #     reducers.append(reducer)
    #     reduce_generators.append(
    #         reducer.reduce.remote(partitions, actor_addresses, shuffle_actor_manager)
    #     )

    # try:
    #     # Collect the results from the reduce tasks, they are configured as ray generators that yield partition, and then metadata
    #     for reduce_idx in range(num_output_partitions):
    #         reduce_gen = reduce_generators[reduce_idx % len(reduce_generators)]
    #         partition = next(reduce_gen)
    #         metadata = ray.get(next(reduce_gen))
    #         yield PartitionTaskBuilder(
    #             inputs=[partition],
    #             partial_metadatas=[metadata],
    #         ).add_instruction(execution_step.Aggregate(to_agg=[col("ints").count()], group_by=None))
    #         del partition
    # except StopIteration:
    #     pass
    # finally:

    #     # Wait for all the reduce tasks to complete
    #     ray.get([reduce_gen.completed() for reduce_gen in reduce_generators])
    #     del reduce_generators
    #     del reducers
    #     # Shutdown the actor manager
    #     ray.get(shuffle_actor_manager.shutdown.remote())
    #     # Kill the actor manager
    #     ray.kill(shuffle_actor_manager)

    # print(f"total_bytes: {total_bytes}")
    # if shuffle_stage_id == 27:
    #     print(f"pausing for 60 seconds")
    #     time.sleep(60)
    # Calculate how many reduce tasks, and which partitions go to which reduce tasks
    # actor_addresses, actor_node_ids = ray.get(
    #     [
    #         shuffle_actor_manager.get_active_actor_addresses.remote(),
    #         shuffle_actor_manager.get_active_node_ids.remote(),
    #     ],
    # )
    # reduce_task_to_partitions, partitions_to_reduce_task = optimize_reduce_partition_assignment(
    #     actor_node_ids, num_output_partitions
    # )

    # # Launch the reduce tasks
    # reduce_generators = []
    # for reduce_task_idx, partitions in enumerate(reduce_task_to_partitions):
    #     reduce_generators.append(
    #         reduce_partitions.options(
    #             scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
    #                 node_id=actor_node_ids[reduce_task_idx % len(actor_node_ids)],
    #                 soft=False,
    #             )
    #         ).remote(partitions, actor_addresses, shuffle_actor_manager)
    #     )

    # try:
    #     # Collect the results from the reduce tasks, they are configured as ray generators that yield partition, and then metadata
    #     for reduce_task_idx in partitions_to_reduce_task:
    #         reduce_gen = reduce_generators[reduce_task_idx]
    #         partition = next(reduce_gen)
    #         metadata = ray.get(next(reduce_gen))
    #         yield PartitionTaskBuilder(
    #             inputs=[partition],
    #             partial_metadatas=[metadata],
    #         )                
    # except StopIteration:
    #     pass
    # finally:
    #     # Wait for all the reduce tasks to complete
    #     ray.get([reduce_gen.completed() for reduce_gen in reduce_generators])
    #     del reduce_generators
    #     # Shutdown the actor manager
    #     ray.get(shuffle_actor_manager.shutdown.remote())
    #     # Kill the actor manager
    #     ray.kill(shuffle_actor_manager)
    #     print(f"pausing for 60")
    #     if shuffle_stage_id == 27:
    #         time.sleep(600)

    actor_addresses = ray.get(shuffle_actor_manager.get_active_actor_addresses.remote())
    actors = ray.get(shuffle_actor_manager.get_active_actors.remote())

    ray.get(
        [actor.initialize_client_manager.remote(actor_addresses) for actor in actors]
    )

    clear_partition_futures = []
    for partition_idx in range(num_output_partitions):
        partition_ref, metadata = (
            actors[partition_idx % len(actors)]
            .fetch_partition.options(num_returns=2)
            .remote(partition_idx)
        )
        if shuffle_stage_id == 27:
            print(f"partition ref: {partition_ref}")
        metadata = ray.get(metadata)
        yield PartitionTaskBuilder(
            inputs=[partition_ref],
            partial_metadatas=[metadata],
        )
        
        clear_partition_futures.append(
            shuffle_actor_manager.clear_partition.remote(partition_idx)
        )

    ray.get(clear_partition_futures)
    ray.get(shuffle_actor_manager.shutdown.remote())
    ray.kill(shuffle_actor_manager)

    if shuffle_stage_id == 27:
        print(f"pausing for 60")
        time.sleep(600)


def flight_shuffle(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    num_output_partitions: int,
    shuffle_dirs: list[str],
    partition_by: list[PyExpr] | None = None,
):
    map_stage_id = next(stage_id_counter)
    shuffle_stage_id = next(stage_id_counter)
    print(f"shuffle_stage_id: {shuffle_stage_id}")
    # Try to schedule the manager on the current node, which should be the head node
    shuffle_actor_manager = ShuffleActorManager.options(
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
    yield from run_reduce_phase(
        num_output_partitions, shuffle_stage_id, shuffle_actor_manager
    )
