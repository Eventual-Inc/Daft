from abc import abstractmethod
from typing import Any, Dict, List, Optional

import numpy as np

from daft.runners.blocks import DataBlock
from daft.runners.partitioning import vPartition


class ShuffleOp:
    def __init__(self, map_args: Optional[Dict[str, Any]] = None, reduce_args: Optional[Dict[str, Any]] = None) -> None:
        self._map_args = map_args
        self._reduce_args = reduce_args

    @staticmethod
    @abstractmethod
    def map_fn(input: vPartition, output_partitions: int) -> List[vPartition]:
        ...

    @staticmethod
    @abstractmethod
    def reduce_fn(mapped_outputs: List[vPartition]) -> vPartition:
        ...


class RepartitionRandom(ShuffleOp):
    @staticmethod
    def map_fn(input: vPartition, output_partitions: int, seed: Optional[int] = None) -> List[vPartition]:
        if seed is None:
            seed = input.partition_id
        else:
            seed += input.partition_id

        rng = np.random.default_rng(seed=seed)
        target_idx = DataBlock.make_block(data=rng.integers(low=0, high=output_partitions, size=len(input)))
        return input.split_by_index(num_partitions=output_partitions, target_partition_indices=target_idx)

    @staticmethod
    def reduce_fn(mapped_outputs: List[vPartition]) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs)
