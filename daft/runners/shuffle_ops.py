import dataclasses
import math
from abc import abstractmethod
from typing import Dict, List, Optional, Protocol

import numpy as np

from daft.expressions import Expression
from daft.resource_request import ResourceRequest
from daft.runners.blocks import ArrowArrType, DataBlock
from daft.runners.partitioning import PartID, PartitionSet, vPartition

from ..logical.schema import ExpressionList


class ShuffleOp(Protocol):
    def map_resource_request(self) -> ResourceRequest:
        """How much resources this ShuffleOp requires during the map stage"""
        ...

    def reduce_resource_request(self) -> ResourceRequest:
        """How much resources this ShuffleOp requires during the reduce stage"""
        ...

    def map_fn(self, input: vPartition, output_partitions: int) -> Dict[PartID, vPartition]:
        """Execution of the map function on a single vPartition"""
        ...

    def reduce_fn(self, mapped_outputs: List[vPartition]) -> vPartition:
        """Execution of the reduce function on a list of incoming vPartitions that were mapped to the same PartID"""
        ...


@dataclasses.dataclass(frozen=True)
class RepartitionRandomOp(ShuffleOp):

    seed: Optional[int] = None

    def map_resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def reduce_resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def map_fn(self, input: vPartition, output_partitions: int) -> Dict[PartID, vPartition]:
        seed = self.seed
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

    def reduce_fn(self, mapped_outputs: List[vPartition]) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs)


@dataclasses.dataclass(frozen=True)
class RepartitionHashOp(ShuffleOp):

    exprs: ExpressionList

    def map_resource_request(self) -> ResourceRequest:
        return self.exprs.resource_request()

    def reduce_resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def map_fn(self, input: vPartition, output_partitions: int) -> Dict[PartID, vPartition]:
        new_parts = input.split_by_hash(self.exprs, num_partitions=output_partitions)
        return {PartID(i): part for i, part in enumerate(new_parts)}

    def reduce_fn(self, mapped_outputs: List[vPartition]) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs)


@dataclasses.dataclass(frozen=True)
class CoalesceOp(ShuffleOp):

    num_input_partitions: int

    def map_resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def reduce_resource_request(self) -> ResourceRequest:
        return ResourceRequest.default()

    def map_fn(self, input: vPartition, output_partitions: int) -> Dict[PartID, vPartition]:
        assert output_partitions <= self.num_input_partitions

        tgt_idx = math.floor((output_partitions / self.num_input_partitions) * input.partition_id)
        return {PartID(tgt_idx): input}

    def reduce_fn(self, mapped_outputs: List[vPartition]) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs, verify_partition_id=False)


@dataclasses.dataclass(frozen=True)
class SortOp(ShuffleOp):

    expr: Expression
    boundaries: DataBlock
    desc: bool

    def map_resource_request(self) -> ResourceRequest:
        return self.expr.resource_request()

    def reduce_resource_request(self) -> ResourceRequest:
        return self.expr.resource_request()

    def map_fn(
        self,
        input: vPartition,
        output_partitions: int,
    ) -> Dict[PartID, vPartition]:
        assert len(self.boundaries) == (output_partitions - 1)
        if output_partitions == 1:
            return {PartID(0): input}
        sort_key = input.eval_expression(self.expr).block
        argsort_idx = sort_key.argsort()
        sorted_input = input.take(argsort_idx)
        sorted_keys = sort_key.take(argsort_idx)
        target_idx = sorted_keys.search_sorted(self.boundaries, reverse=self.desc)
        new_parts = sorted_input.split_by_index(num_partitions=output_partitions, target_partition_indices=target_idx)
        return {PartID(i): part for i, part in enumerate(new_parts)}

    def reduce_fn(
        self,
        mapped_outputs: List[vPartition],
    ) -> vPartition:
        return vPartition.merge_partitions(mapped_outputs).sort(self.expr, desc=self.desc)


class Shuffler:
    """A Shuffler is a class that takes as input a ShuffleOp and runs it. Subclasses of Shufflers
    implement shuffling ShuffleOps on different backends.
    """

    @abstractmethod
    def run(self, shuffle_op: ShuffleOp, input: PartitionSet, num_target_partitions: int) -> PartitionSet:
        raise NotImplementedError()
