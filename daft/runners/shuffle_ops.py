from __future__ import annotations

import math
from abc import abstractmethod
from typing import Any

import numpy as np

from daft.resource_request import ResourceRequest
from daft.runners.blocks import ArrowArrType, DataBlock
from daft.runners.partitioning import PartID, PartitionSet, vPartition

from ..logical.schema import ExpressionList


class ShuffleOp:
    def __init__(
        self,
        expr_eval_resource_request: ResourceRequest,
        map_args: dict[str, Any] | None = None,
        reduce_args: dict[str, Any] | None = None,
    ) -> None:
        self._map_args = map_args
        self._reduce_args = reduce_args
        self._expr_eval_resource_request = expr_eval_resource_request

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
    def map_fn(input: vPartition, output_partitions: int, seed: int | None = None) -> dict[PartID, vPartition]:
        if seed is None:
            seed = input.partition_id
        else:
            seed += input.partition_id

        rng = np.random.default_rng(seed=seed)
        target_idx: DataBlock[ArrowArrType] = DataBlock.make_block(
            data=rng.integers(low=0, high=output_partitions, size=len(input))
        )
        new_parts = input.split_by_index(num_partitions=output_partitions, target_partition_indices=target_idx)
        return {PartID(i): part for i, part in enumerate(new_parts)}

    @staticmethod
    def reduce_fn(mapped_outputs: list[vPartition]) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs)


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
        return vPartition.merge_partitions(mapped_outputs)


class CoalesceOp(ShuffleOp):
    @staticmethod
    def map_fn(
        input: vPartition, output_partitions: int, num_input_partitions: int | None = None
    ) -> dict[PartID, vPartition]:
        assert num_input_partitions is not None
        assert output_partitions <= num_input_partitions

        tgt_idx = math.floor((output_partitions / num_input_partitions) * input.partition_id)
        return {PartID(tgt_idx): input}

    @staticmethod
    def reduce_fn(mapped_outputs: list[vPartition]) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs, verify_partition_id=False)


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
        return vPartition.merge_partitions(mapped_outputs).sort(exprs, descending=descending)


class Shuffler(ShuffleOp):
    @abstractmethod
    def run(self, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        raise NotImplementedError()
