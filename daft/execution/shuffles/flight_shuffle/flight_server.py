import os
import shutil

import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.flight as flight

from daft.execution.shuffles.flight_shuffle.utils import (
    get_shuffle_file_path,
)


class FlightServer(flight.FlightServerBase):
    def __init__(
        self,
        host: str,
        node_id: str,
        shuffle_stage_id: int,
        **kwargs,
    ):
        location = f"grpc://{host}:0"
        super(FlightServer, self).__init__(location, **kwargs)
        self.node_id = node_id
        self.shuffle_stage_id = shuffle_stage_id

    def get_port(self):
        return self.port

    def do_get(self, context, ticket):
        shuffle_stage_id, partition = ticket.ticket.decode("utf-8").split(",")
        shuffle_stage_id = int(shuffle_stage_id)
        partition_idx = int(partition)
        assert shuffle_stage_id == self.shuffle_stage_id

        path = get_shuffle_file_path(self.node_id, self.shuffle_stage_id, partition_idx)
        files = os.listdir(path)

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

    def do_action(self, context, action):
        if action.type == "drop_partition":
            self.do_drop_partition(action.body.to_pybytes().decode("utf-8"))
        else:
            raise NotImplementedError

    def do_drop_partition(self, partition):
        path = get_shuffle_file_path(self.node_id, self.shuffle_stage_id, int(partition))
        try:
            if os.path.exists(path):
                shutil.rmtree(path)
        except Exception as e:
            print(f"Error dropping partition {partition}: {e}")
