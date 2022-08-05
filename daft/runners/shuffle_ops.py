from abc import abstractmethod
from typing import Any, Dict, List, Optional

import numpy as np

from daft.expressions import Expression
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


class RepartitionRandomOp(ShuffleOp):
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


class SortOp(ShuffleOp):
    @staticmethod
    def map_fn(
        input: vPartition,
        output_partitions: int,
        expr: Optional[Expression] = None,
        boundaries: Optional[DataBlock] = None,
        desc: Optional[bool] = None,
    ) -> List[vPartition]:
        assert expr is not None and boundaries is not None and desc is not None
        sort_key = input.eval_expression(expr).block
        argsort_idx = sort_key.argsort()
        sorted_input = input.take(argsort_idx)
        sorted_keys = sort_key.take(argsort_idx)
        target_idx = sorted_keys.search_sorted(boundaries)
        if desc:
            target_idx = (output_partitions - 1) - target_idx
        return sorted_input.split_by_index(num_partitions=output_partitions, target_partition_indices=target_idx)

    @staticmethod
    def reduce_fn(
        mapped_outputs: List[vPartition], expr: Optional[Expression] = None, desc: Optional[bool] = None
    ) -> vPartition:
        assert expr is not None and desc is not None
        return vPartition.merge_partitions(mapped_outputs).sort(expr, desc=desc)
