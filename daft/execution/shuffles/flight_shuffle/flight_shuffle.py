import concurrent.futures
import os
import shutil
import time
from collections import defaultdict

import pyarrow.flight as flight
import ray
import ray.experimental
import ray.util.scheduling_strategies

from daft.daft import PyExpr
from daft.execution.execution_step import (
    PartitionTaskBuilder,
    SingleOutputPartitionTask,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.execution.shuffles.flight_shuffle.flight_server import FlightServer
from daft.execution.shuffles.flight_shuffle.utils import PartitionCache, get_shuffle_file_path
from daft.expressions.expressions import Expression, ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.partitioning import PartitionMetadata


@ray.remote(num_cpus=0)
class ShuffleActorManager:
    def __init__(
        self,
        shuffle_stage_id: int,
        num_output_partitions: int,
        partition_by: ExpressionsProjection | None = None,
    ):
        self.node_id_to_actors = {}
        self.actor_futures = []
        self.shuffle_stage_id = shuffle_stage_id
        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

    def get_or_create_actor(self, node_id: str):
        if node_id not in self.node_id_to_actors:
            actor = ShuffleActor.options(
                scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=node_id,
                    soft=False,  # TODO: check if this is dangerous, if so, check if we can do true
                )
            ).remote(self.shuffle_stage_id, self.num_output_partitions, self.partition_by)
            self.node_id_to_actors[node_id] = actor
        return self.node_id_to_actors[node_id]

    def push_objects_to_actor(self, node_id: str, objects: list[ray.ObjectRef]):
        actor = self.get_or_create_actor(node_id)
        self.actor_futures.append(actor.map_partitions.remote(*objects))

    def push_objects(self, objects: list[ray.ObjectRef]):
        object_locations = ray.experimental.get_local_object_locations(objects)
        node_id_to_objects = defaultdict(list)
        for object, object_location in object_locations.items():
            if object_location is None or "node_ids" not in object_location or not object_location["node_ids"]:
                node_id = ray.get_runtime_context().get_node_id()
                node_id_to_objects[ray.get_runtime_context().get_node_id()].append(object)
            else:
                node_id_to_objects[object_location["node_ids"][0]].append(object)

        for node_id, objects in node_id_to_objects.items():
            self.push_objects_to_actor(node_id, objects)

    def wait_for_maps(self):
        ray.get(self.actor_futures)
        self.actor_futures = []
        ray.get([actor.wait_for_maps.remote() for actor in self.node_id_to_actors.values()])

    def get_actor_addresses(self):
        return ray.get([actor.get_address.remote() for actor in self.node_id_to_actors.values()])

    def shutdown(self):
        ray.get([actor.shutdown.remote() for actor in self.node_id_to_actors.values()])


@ray.remote
class ShuffleActor:
    def __init__(
        self,
        shuffle_stage_id: int,
        num_output_partitions: int,
        partition_by: ExpressionsProjection | None = None,
    ):
        self.node_id = ray.get_runtime_context().get_node_id()
        self.host = ray.util.get_node_ip_address()
        self.shuffle_stage_id = shuffle_stage_id
        self.partition_cache = PartitionCache(self.node_id, self.shuffle_stage_id, num_output_partitions)
        self.server = FlightServer(self.host, self.node_id, self.shuffle_stage_id)
        self.port = self.server.get_port()

        self.num_output_partitions = num_output_partitions
        self.partition_by = partition_by

        self.thread_pool = concurrent.futures.ThreadPoolExecutor()
        self.map_futures = []

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

    def map_partitions(self, *partitions: MicroPartition):
        def map_partitions_inner(*partitions: MicroPartition):
            concated = MicroPartition.concat(partitions)
            if self.partition_by is None:
                partitioned = concated.partition_by_random(self.num_output_partitions, 0)
            else:
                partitioned = concated.partition_by_hash(self.partition_by, self.num_output_partitions)

            self.partition_cache.add_partitions(partitioned)

        new_future = self.thread_pool.submit(map_partitions_inner, *partitions)
        self.map_futures.append(new_future)

    def wait_for_maps(self):
        for future in concurrent.futures.as_completed(self.map_futures):
            future.result()

        self.partition_cache.flush_partitions()

    def shutdown(self):
        self.server.shutdown()
        try:
            path = get_shuffle_file_path(self.node_id, self.shuffle_stage_id)
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as e:
            print(f"failed to clean up shuffle files: {e}")


@ray.remote
def reduce_partitions(shuffle_stage_id: int, partitions: list[int], shuffle_actor_addresses: list[str]):
    start_time = time.time()
    metadata_yield_times = []
    partition_yield_times = []
    fetch_wait_times = []

    def fetch_partition(client: flight.FlightClient, partition_idx: int):
        ticket = flight.Ticket(f"{shuffle_stage_id},{partition_idx}".encode())
        reader = client.do_get(ticket)
        res = MicroPartition.from_arrow(reader.read_all())
        return res

    def cleanup_partition(client: flight.FlightClient, partition_idx: int):
        list(client.do_action(flight.Action("drop_partition", f"{partition_idx}".encode())))

    clients = [flight.FlightClient(address) for address in shuffle_actor_addresses]
    futures_per_partition = defaultdict(list)
    cleanup_futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for partition in partitions:
            for client in clients:
                futures_per_partition[partition].append(executor.submit(fetch_partition, client, partition))

        for partition in partitions:
            futures = futures_per_partition[partition]
            fetch_start_time = time.time()
            ready = [future.result() for future in concurrent.futures.as_completed(futures)]
            fetch_wait_times.append(time.time() - fetch_start_time)
            concated = MicroPartition.concat(ready)
            metadata = PartitionMetadata.from_table(concated)
            metadata_yield_start_time = time.time()
            yield metadata
            metadata_yield_times.append(time.time() - metadata_yield_start_time)
            partition_yield_start_time = time.time()
            yield concated
            partition_yield_times.append(time.time() - partition_yield_start_time)
            cleanup_futures.extend(
                [
                    executor.submit(
                        cleanup_partition,
                        client,
                        partition,
                    )
                    for client in clients
                ]
            )

        try:
            for future in concurrent.futures.as_completed(cleanup_futures):
                future.result()
        except Exception as e:
            print(f"error in cleanup: {e}")

    print("total time: ", time.time() - start_time)
    print(f"num partitions: {len(partitions)}")
    # print average, total, min, max, and percentiles of metadata yield times, partition yield times, and fetch times
    print(
        f"metadata yield total time: {sum(metadata_yield_times)}, metadata yield average: {sum(metadata_yield_times) / len(metadata_yield_times)}, metadata yield min: {min(metadata_yield_times)}, metadata yield max: {max(metadata_yield_times)}, metadata yield percent of total: {sum(metadata_yield_times) / (time.time() - start_time)}"
    )
    print(
        f"partition yield total time: {sum(partition_yield_times)}, partition yield average: {sum(partition_yield_times) / len(partition_yield_times)}, partition yield min: {min(partition_yield_times)}, partition yield max: {max(partition_yield_times)}, partition yield percent of total: {sum(partition_yield_times) / (time.time() - start_time)}"
    )
    print(
        f"fetch wait total time: {sum(fetch_wait_times)}, fetch wait average: {sum(fetch_wait_times) / len(fetch_wait_times)}, fetch wait min: {min(fetch_wait_times)}, fetch wait max: {max(fetch_wait_times)}, fetch wait percent of total: {sum(fetch_wait_times) / (time.time() - start_time)}"
    )


def optimize_reduce_partition_assignment(num_shuffle_actors, num_partitions):
    # make 1 reduce task per cpu
    # round robin assignment of partitions to reduce tasks
    num_reduce_tasks = int(max(1, min(ray.available_resources()["CPU"], num_partitions))) // num_shuffle_actors
    reduce_task_to_partitions = [[] for _ in range(num_reduce_tasks)]
    partition_to_reduce_task = []
    for partition_idx in range(num_partitions):
        reduce_task_idx = partition_idx % num_reduce_tasks
        reduce_task_to_partitions[reduce_task_idx].append(partition_idx)
        partition_to_reduce_task.append(reduce_task_idx)
    return reduce_task_to_partitions, partition_to_reduce_task


def run_map_phase(
    plan: InProgressPhysicalPlan[ray.ObjectRef],
    map_stage_id: int,
    shuffle_actor_manager: ShuffleActorManager,
):
    no_more_input = False
    materializations: dict[str, SingleOutputPartitionTask] = {}
    push_objects_futures = []
    while True:
        done_ids = [id for id in materializations.keys() if materializations[id].done()]

        if len(done_ids) > 4 or (len(done_ids) == len(materializations) and no_more_input):
            done_partitions = []
            for id in done_ids:
                materialization = materializations.pop(id)
                done_partitions.append(materialization.partition())

            push_objects_futures.append(shuffle_actor_manager.push_objects.remote(done_partitions))

            if not materializations:
                break
        else:
            try:
                step = next(plan)
                if isinstance(step, PartitionTaskBuilder):
                    step = step.finalize_partition_task_single_output(map_stage_id)
                    materializations[step.id()] = step
                yield step
            except StopIteration:
                no_more_input = True
                yield None

    ray.get(push_objects_futures)
    ray.get(shuffle_actor_manager.wait_for_maps.remote())


def run_reduce_phase(
    shuffle_stage_id: int,
    num_output_partitions: int,
    shuffle_actor_manager: ShuffleActorManager,
):
    actor_addresses = ray.get(shuffle_actor_manager.get_actor_addresses.remote())
    reduce_task_to_partitions, partition_to_reduce_task = optimize_reduce_partition_assignment(
        len(actor_addresses), num_output_partitions
    )

    reduce_generators = []
    for partitions in reduce_task_to_partitions:
        reduce_generators.append(
            reduce_partitions.options(scheduling_strategy="SPREAD").remote(
                shuffle_stage_id, partitions, actor_addresses
            )
        )

    total_size_bytes = 0
    for partition_idx in range(num_output_partitions):
        reduce_gen = reduce_generators[partition_to_reduce_task[partition_idx]]
        metadata = ray.get(next(reduce_gen))
        total_size_bytes += metadata.size_bytes
        partition = next(reduce_gen)
        yield PartitionTaskBuilder(
            inputs=[partition],
            partial_metadatas=[metadata],
        )
    print("total size bytes: ", total_size_bytes)


def flight_shuffle(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    num_output_partitions: int,
    partition_by: list[PyExpr] | None = None,
):
    map_stage_id = next(stage_id_counter)
    shuffle_stage_id = next(stage_id_counter)

    expressions_projection = (
        ExpressionsProjection([Expression._from_pyexpr(e) for e in partition_by]) if partition_by else None
    )

    shuffle_actor_manager = ShuffleActorManager.options(
        scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=True,
        )
    ).remote(shuffle_stage_id, num_output_partitions, expressions_projection)

    yield from run_map_phase(
        fanout_plan,
        map_stage_id,
        shuffle_actor_manager,
    )

    yield from run_reduce_phase(shuffle_stage_id, num_output_partitions, shuffle_actor_manager)

    ray.get(shuffle_actor_manager.shutdown.remote())
