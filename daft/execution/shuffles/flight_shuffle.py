import concurrent.futures
import os
import uuid
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.flight as flight
import ray

from daft.execution.execution_step import (
    FanoutInstruction,
    MultiOutputPartitionTask,
    PartitionTaskBuilder,
    ReduceInstruction,
)
from daft.execution.physical_plan import InProgressPhysicalPlan, stage_id_counter
from daft.recordbatch.micropartition import MicroPartition
from daft.runners.partitioning import PartialPartitionMetadata


def get_partition_path(node_id: str, shuffle_stage_id: str, partition_id: int):
    return f"/tmp/daft_shuffle/{node_id}/{shuffle_stage_id}/partition_{partition_id}"


class FlightServer(pa.flight.FlightServerBase):
    def __init__(self, host: str, node_id: str, shuffle_stage_id: str, **kwargs):
        location = f"grpc://{host}:0"
        super(FlightServer, self).__init__(location, **kwargs)
        self.node_id = node_id
        self.shuffle_stage_id = shuffle_stage_id

    def get_port(self):
        return self.port

    def do_get(self, context, ticket):
        shuffle_stage_id, partition = ticket.ticket.decode("utf-8").split(",")
        assert shuffle_stage_id == self.shuffle_stage_id

        path = get_partition_path(self.node_id, self.shuffle_stage_id, partition)
        files = os.listdir(path)
        files = [f for f in files if f.endswith(".arrow")]
        files = sorted(files, key=lambda x: int(x.split(".")[0]))

        def read_tables():
            first_file = files[0]
            first_table = feather.read_table(f"{path}/{first_file}")
            yield first_table.schema

            if len(first_table) > 0:
                yield first_table
            for file in files[1:]:
                table = feather.read_table(f"{path}/{file}")
                if len(table) > 0:
                    yield table

        generator = read_tables()
        return pa.flight.GeneratorStream(next(generator), generator)


@ray.remote(num_cpus=0)
class ShuffleActor:
    def __init__(self, shuffle_stage_id: str):
        self.node_id = ray.get_runtime_context().get_node_id()
        self.host = ray.util.get_node_ip_address()
        self.server = FlightServer(self.host, self.node_id, shuffle_stage_id)
        self.port = self.server.get_port()

    def get_address(self):
        return f"grpc://{self.host}:{self.port}"


@dataclass(frozen=True)
class WriteShuffleFiles(FanoutInstruction):
    shuffle_stage_id: str
    mapper_id: int

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        node_id = ray.get_runtime_context().get_node_id()

        for i, partition in enumerate(inputs):
            self.write_partition(node_id, partition, i)

        return [
            MicroPartition.from_pydict(
                {
                    "node_id": [node_id],
                }
            )
        ]

    def write_partition(self, node_id: str, partition: MicroPartition, partition_id: int):
        dir_path = get_partition_path(node_id, self.shuffle_stage_id, partition_id)
        os.makedirs(dir_path, exist_ok=True)

        arrow_table = partition.to_arrow()
        path = f"{dir_path}/{self.mapper_id}.arrow"
        feather.write_feather(arrow_table, path)

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        return [PartialPartitionMetadata(size_bytes=0, num_rows=0)]


@dataclass(frozen=True)
class ReadShuffleFiles(ReduceInstruction):
    shuffle_actors: dict[str, ShuffleActor]
    shuffle_stage_id: str
    partition: int

    def run(self, inputs: list[MicroPartition]) -> list[MicroPartition]:
        parts = [None] * len(self.shuffle_actors)  # Pre-allocate list with correct size

        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Create a dictionary mapping futures to their indices
            future_to_index = {executor.submit(self.fetch, address): i for i, address in enumerate(self.shuffle_actors)}

            # As futures complete, store results at the correct index
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                parts[index] = future.result()

        return [MicroPartition.concat(parts)]

    def fetch(self, address: str):
        client = flight.FlightClient(address)
        ticket = flight.Ticket(f"{self.shuffle_stage_id},{self.partition}".encode())
        reader = client.do_get(ticket)
        client.close()
        return MicroPartition.from_arrow(reader.read_all())

    def run_partial_metadata(self, input_metadatas: list[PartialPartitionMetadata]) -> list[PartialPartitionMetadata]:
        return [
            PartialPartitionMetadata(
                num_rows=0,
                size_bytes=0,
            )
        ]


def run_map_phase(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    map_stage_id: int,
    shuffle_stage_id: str,
):
    materializations: list[MultiOutputPartitionTask] = []
    mapper_id_counter = 0
    for step in fanout_plan:
        if isinstance(step, PartitionTaskBuilder):
            step = step.add_instruction(
                WriteShuffleFiles(
                    _num_outputs=1,
                    shuffle_stage_id=shuffle_stage_id,
                    mapper_id=mapper_id_counter,
                )
            )
            step = step.finalize_partition_task_multi_output(stage_id=map_stage_id)
            mapper_id_counter += 1
            materializations.append(step)
        yield step

    while any(not step._results for step in materializations):
        yield None

    map_results = ray.get([partition for m in materializations for partition in m.partitions()])
    return map_results


def run_reduce_phase(
    map_results: list[MicroPartition],
    shuffle_stage_id: str,
    num_output_partitions: int,
):
    node_ids = set()
    for result in map_results:
        data = result.to_pydict()
        node_ids.add(data["node_id"][0])

    shuffle_actors = [
        ShuffleActor.options(
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=node_id,
                soft=False,
            ),
        ).remote(shuffle_stage_id)
        for node_id in node_ids
    ]
    shuffle_addresses = ray.get([actor.get_address.remote() for actor in shuffle_actors])
    shuffle_actors = {address: shuffle_actors[i] for i, address in enumerate(shuffle_addresses)}

    for partition in range(num_output_partitions):
        yield PartitionTaskBuilder(
            inputs=[],
            partial_metadatas=[],
        ).add_instruction(
            ReadShuffleFiles(
                shuffle_actors=shuffle_actors,
                shuffle_stage_id=shuffle_stage_id,
                partition=partition,
            )
        )


def flight_shuffle(
    fanout_plan: InProgressPhysicalPlan[ray.ObjectRef],
    num_output_partitions: int,
):
    map_stage_id = next(stage_id_counter)
    shuffle_stage_id = str(uuid.uuid4())

    map_results = yield from run_map_phase(fanout_plan, map_stage_id, shuffle_stage_id)

    reduce_phase = run_reduce_phase(map_results, shuffle_stage_id, num_output_partitions)
    yield from reduce_phase
