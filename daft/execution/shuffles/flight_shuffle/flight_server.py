from daft.daft import ShuffleCache
from daft.dependencies import flight, pa


class FlightServer(flight.FlightServerBase):
    def __init__(
        self,
        host: str,
        node_id: str,
        **kwargs,
    ):
        location = f"grpc://{host}:0"
        super().__init__(location, **kwargs)
        self.node_id = node_id
        self.schema = None
        self.shuffle_cache = None

    def get_port(self):
        return self.port

    def set_shuffle_cache(self, shuffle_cache: ShuffleCache):
        self.shuffle_cache = shuffle_cache

    def do_get(self, context, ticket):
        partition_idx = ticket.ticket.decode("utf-8")

        if self.schema is None:
            self.schema = self.shuffle_cache.schema().to_pyarrow_schema()

        file_paths = self.shuffle_cache.file_paths(int(partition_idx))

        def stream_tables(file_paths):
            for file_path in file_paths:
                yield pa.ipc.open_stream(pa.input_stream(file_path))

        return pa.flight.GeneratorStream(self.schema, stream_tables(file_paths))
