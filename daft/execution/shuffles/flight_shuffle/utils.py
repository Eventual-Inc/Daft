import threading
import time

from daft.daft import write_many_ipc_files
from daft.recordbatch.micropartition import MicroPartition


def get_shuffle_file_path(
    node_id: str,
    shuffle_stage_id: int,
    partition_id: int | None = None,
    mapper_id: int | None = None,
):
    if partition_id is None:
        return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}"
    else:
        if mapper_id is None:
            return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}/partition_{partition_id}"
        else:
            return f"/tmp/daft_shuffle/node_{node_id}/shuffle_stage_{shuffle_stage_id}/partition_{partition_id}/{mapper_id}.arrow"


class PartitionCache:
    def __init__(
        self,
        node_id: str,
        shuffle_stage_id: int,
        num_output_partitions: int,
        target_file_size_bytes: int = 1024 * 1024,
    ):
        self.lock = threading.Lock()
        self.node_id = node_id
        self.shuffle_stage_id = shuffle_stage_id
        self.num_output_partitions = num_output_partitions
        self.in_memory_partition_cache = [None for _ in range(num_output_partitions)]
        self.in_memory_partition_cache_size_bytes = [0] * num_output_partitions
        self.write_counter = 0
        self.files_per_partition = [0] * num_output_partitions
        self.target_file_size_bytes = target_file_size_bytes

    def add_partitions(self, partitioned: list[MicroPartition]):
        submit_to_write = []
        with self.lock:
            # first add all partitions to the in-memory cache
            for partition_idx, partition in enumerate(partitioned):
                partition_size_bytes = partition.size_bytes() or 0
                if self.in_memory_partition_cache[partition_idx] is None:
                    self.in_memory_partition_cache[partition_idx] = partition
                    self.in_memory_partition_cache_size_bytes[partition_idx] = partition_size_bytes
                else:
                    self.in_memory_partition_cache[partition_idx] = MicroPartition.concat(
                        [self.in_memory_partition_cache[partition_idx], partition]
                    )
                    self.in_memory_partition_cache_size_bytes[partition_idx] += partition_size_bytes

                if self.in_memory_partition_cache_size_bytes[partition_idx] >= self.target_file_size_bytes:
                    submit_to_write.append(
                        (
                            self.in_memory_partition_cache[partition_idx]._micropartition,
                            get_shuffle_file_path(
                                self.node_id,
                                self.shuffle_stage_id,
                                partition_idx,
                                self.write_counter,
                            ),
                        )
                    )
                    self.files_per_partition[partition_idx] += 1
                    self.in_memory_partition_cache[partition_idx] = None
                    self.in_memory_partition_cache_size_bytes[partition_idx] = 0

            # increment the write counter while in the lock
            if submit_to_write:
                self.write_counter += 1

        # write the partitions to disk, outside of the lock
        if submit_to_write:
            write_many_ipc_files(submit_to_write)

    def flush_partitions(self):
        start_time = time.time()
        submit_to_write = []
        with self.lock:
            for partition_idx, partition in enumerate(self.in_memory_partition_cache):
                if partition is not None:
                    submit_to_write.append(
                        (
                            partition._micropartition,
                            get_shuffle_file_path(
                                self.node_id,
                                self.shuffle_stage_id,
                                partition_idx,
                                self.write_counter,
                            ),
                        )
                    )
                    self.files_per_partition[partition_idx] += 1
                    self.in_memory_partition_cache[partition_idx] = None
                    self.in_memory_partition_cache_size_bytes[partition_idx] = 0

            self.write_counter += 1
        print(f"Flushing {len(submit_to_write)} partitions, took {time.time() - start_time} seconds")
        start_time = time.time()
        if submit_to_write:
            write_many_ipc_files(submit_to_write)
        print(f"Flushed partitions in {time.time() - start_time} seconds")
