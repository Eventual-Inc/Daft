from daft.daft import ShuffleWriter
from daft.expressions.expressions import ExpressionsProjection
from daft.recordbatch.micropartition import MicroPartition


def get_shuffle_file_path(
    node_id: str,
    shuffle_stage_id: int,
    partition_id: int | None = None,
    mapper_id: int | None = None,
):
    if partition_id is None:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}"
    elif mapper_id is None:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}/partition_{partition_id}"
    else:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}/partition_{partition_id}/{mapper_id}.arrow"


class PartitionCache:
    def __init__(
        self,
        node_id: str,
        shuffle_stage_id: int,
        num_output_partitions: int,
        target_file_size_bytes: int = 1024 * 1024 * 10,
        partition_by: ExpressionsProjection | None = None,
        compression: str | None = None,
    ):
        self.node_id = node_id
        self.shuffle_stage_id = shuffle_stage_id
        self.num_output_partitions = num_output_partitions
        self.shuffle_writer = ShuffleWriter.try_new(
            num_partitions=num_output_partitions,
            dir=get_shuffle_file_path(node_id, shuffle_stage_id),
            target_filesize=target_file_size_bytes,
            compression=compression,
            partition_by=[p._expr for p in partition_by] if partition_by else None,
        )
        self.arrow_schema = None
        self.partition_by = partition_by

    def schema(self):
        return self.arrow_schema

    def push_partition(self, partition: MicroPartition):
        if not self.arrow_schema:
            self.arrow_schema = partition.schema().to_pyarrow_schema()
        self.shuffle_writer.push_partition(partition._micropartition)

    def flush_partitions(self):
        bytes_per_file = self.shuffle_writer.close()
        return bytes_per_file
