from __future__ import annotations

from abc import abstractmethod
from typing import Any

import numpy as np

from daft.runners.blocks import ArrowArrType, DataBlock
from daft.runners.partitioning import PartID, vPartition

from ..logical.schema import ExpressionList


class ShuffleOp:
    def __init__(
        self,
        map_args: dict[str, Any] | None = None,
        reduce_args: dict[str, Any] | None = None,
    ) -> None:
        self._map_args = map_args
        self._reduce_args = reduce_args

    @staticmethod
    @abstractmethod
    def map_fn(input: vPartition, output_partitions: int) -> dict[PartID, vPartition]:
        ...

    @staticmethod
    @abstractmethod
    def reduce_fn(mapped_outputs: list[vPartition]) -> vPartition:
        ...


class RepartitionRandomOp(ShuffleOp):
    @staticmethod
    def map_fn(input: vPartition, output_partitions: int, seed: int = 0) -> dict[PartID, vPartition]:
        rng = np.random.default_rng(seed=seed)
        target_idx: DataBlock[ArrowArrType] = DataBlock.make_block(
            data=rng.integers(low=0, high=output_partitions, size=len(input))
        )
        new_parts = input.split_by_index(num_partitions=output_partitions, target_partition_indices=target_idx)
        return {PartID(i): part for i, part in enumerate(new_parts)}

    @staticmethod
    def reduce_fn(mapped_outputs: list[vPartition]) -> vPartition:
        return vPartition.concat(mapped_outputs)


class RepartitionHashOp(ShuffleOp):
    @staticmethod
    def map_fn(
        input: vPartition, output_partitions: int, exprs: ExpressionList | None = None
    ) -> dict[PartID, vPartition]:
        assert exprs is not None
        new_parts = input.split_by_hash(exprs, num_partitions=output_partitions)
        return {PartID(i): part for i, part in enumerate(new_parts)}

    @staticmethod
    def reduce_fn(mapped_outputs: list[vPartition]) -> vPartition:
        return vPartition.concat(mapped_outputs)


class SortOp(ShuffleOp):
    @staticmethod
    def map_fn(
        input: vPartition,
        output_partitions: int,
        exprs: ExpressionList | None = None,
        boundaries: vPartition | None = None,
        descending: list[bool] | None = None,
    ) -> dict[PartID, vPartition]:
        assert exprs is not None and boundaries is not None and descending is not None
        assert len(exprs) == len(descending)
        if output_partitions == 1:
            return {PartID(0): input}
        sort_keys = input.eval_expression_list(exprs)
        target_idx = boundaries.search_sorted(sort_keys, input_reversed=descending)
        new_parts = input.split_by_index(num_partitions=output_partitions, target_partition_indices=target_idx)
        return {PartID(i): part for i, part in enumerate(new_parts)}

    @staticmethod
    def reduce_fn(
        mapped_outputs: list[vPartition],
        exprs: ExpressionList | None = None,
        descending: list[bool] | None = None,
    ) -> vPartition:
        assert exprs is not None and descending is not None
        assert len(exprs) == len(descending)
        return vPartition.concat(mapped_outputs).sort(exprs, descending=descending)
