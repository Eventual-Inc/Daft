import os

from daft.daft import ShuffleCache
from daft.dependencies import flight, pa
from daft.execution.shuffles.flight_shuffle.utils import (
    get_shuffle_file_path,
)


class FlightServer(flight.FlightServerBase):
    def __init__(
        self,
        host: str,
        node_id: str,
        shuffle_stage_id: int,
        shuffle_cache: ShuffleCache,
        **kwargs,
    ):
        location = f"grpc://{host}:0"
        super().__init__(location, **kwargs)
        self.node_id = node_id
        self.shuffle_stage_id = shuffle_stage_id
        self.shuffle_cache = shuffle_cache
        self.schema = None

    def get_port(self):
        return self.port

    def do_get(self, context, ticket):
        shuffle_stage_id, partition_idx = ticket.ticket.decode("utf-8").split(",")
        shuffle_stage_id = int(shuffle_stage_id)
        partition_idx = int(partition_idx)
        assert (
            shuffle_stage_id == self.shuffle_stage_id
        ), f"Shuffle stage id mismatch, expected {self.shuffle_stage_id}, got {shuffle_stage_id}"

        if self.schema is None:
            self.schema = self.shuffle_cache.schema().to_pyarrow_schema()
            assert (
                self.schema is not None
            ), f"Schema is not set in shuffle cache, for node {self.node_id}, shuffle stage {self.shuffle_stage_id}, partition {partition_idx}"

        def read_tables():
            path = get_shuffle_file_path(self.node_id, self.shuffle_stage_id, partition_idx)
            files = os.listdir(path)
            if len(files) == 0:
                return

            for file in files:
                # with pa.memory_map(f"{path}/{file}") as source:
                #     yield pa.ipc.open_file(source).read_all()
                with pa.OSFile(f"{path}/{file}", "rb") as source:
                    yield pa.ipc.open_file(source).read_all()

        return pa.flight.GeneratorStream(self.schema, read_tables())
