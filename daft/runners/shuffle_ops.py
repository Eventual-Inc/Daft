from abc import abstractmethod
from typing import List, Optional

from daft.runners.partitioning import PartID, PartitionSet, vPartition


class ShuffleOp:
    def __init__(self, target_partitions: int, input: PartitionSet) -> None:
        self._target_partitions = target_partitions
        self._input = input
        self._source_partitions = input.num_partitions()
        self._mapped_partitions: Optional[List[List[vPartition]]] = None

    @classmethod
    @abstractmethod
    def map_fn(cls, partition: vPartition) -> List[vPartition]:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def reduce_fn(cls, partitions: List[vPartition]) -> vPartition:
        raise NotImplementedError()

    def map(self) -> None:
        assert self._mapped_partitions is None
        map_fn = self.__class__.map_fn
        mapped_partitions = []
        for i in range(self._source_partitions):
            map_output = map_fn(self._input.partitions[i])
            assert len(map_output) == self._target_partitions
            mapped_partitions.append(map_output)

        self._mapped_partitions = mapped_partitions

    def reduce(self) -> PartitionSet:
        assert self._mapped_partitions is not None
        assert len(self._mapped_partitions) == self._source_partitions
        reduce_fn = self.__class__.reduce_fn
        new_partitions = []
        for t in range(self._target_partitions):
            to_reduce = [self._mapped_partitions[s][t] for s in range(self._source_partitions)]
            new_partitions.append(reduce_fn(to_reduce))
        return PartitionSet({PartID(i): part for i, part in enumerate(new_partitions)})
