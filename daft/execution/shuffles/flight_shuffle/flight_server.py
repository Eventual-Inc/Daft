from daft.daft import ShuffleCache
from daft.dependencies import flight, pa


class FlightServer(flight.FlightServerBase):
    def __init__(
        self,
        host: str,
        node_id: str,
        shuffle_cache: ShuffleCache,
        **kwargs,
    ):
        location = f"grpc://{host}:0"
        super().__init__(location, **kwargs)
        self.node_id = node_id
        self.shuffle_cache = shuffle_cache
        self.schema = None

    def get_port(self):
        return self.port

    def do_get(self, context, ticket):
        partition_idx = ticket.ticket.decode("utf-8")

        if self.schema is None:
            self.schema = self.shuffle_cache.schema().to_pyarrow_schema()
            assert (
                self.schema is not None
            ), f"Schema is not set in shuffle cache, for node {self.node_id}, shuffle stage {self.shuffle_stage_id}, partition {partition_idx}"

        file_paths = self.shuffle_cache.file_paths(int(partition_idx))

        def stream_tables(file_paths):
            for file_path in file_paths:
                yield pa.ipc.open_stream(pa.input_stream(file_path))

        return pa.flight.GeneratorStream(self.schema, stream_tables(file_paths))
