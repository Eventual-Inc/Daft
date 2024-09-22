from __future__ import annotations

import dataclasses
from typing import Any, Iterator, Literal, Protocol, TypeVar

ShuffleData = TypeVar("ShuffleData")
IngestResult = TypeVar("IngestResult")


@dataclasses.dataclass(frozen=True)
class PartitioningSpec:
    type_: Literal["hash"] | Literal["range"]

    def to_hash_pspec(self) -> HashPartitioningSpec:
        assert self.type_ == "hash" and isinstance(self, HashPartitioningSpec)
        return self

    def to_range_pspec(self) -> RangePartitioningSpec:
        assert self.type_ == "range" and isinstance(self, RangePartitioningSpec)
        return self


@dataclasses.dataclass(frozen=True)
class HashPartitioningSpec(PartitioningSpec):
    num_partitions: int
    columns: list[str]


@dataclasses.dataclass(frozen=True)
class RangePartitioningSpec(PartitioningSpec):
    boundaries: list[Any]
    columns: list[str]


@dataclasses.dataclass(frozen=True)
class PartitionRequest:
    type_: Literal["hash"] | Literal["range"]

    def to_hash_request(self) -> HashPartitionRequest:
        assert self.type_ == "hash" and isinstance(self, HashPartitionRequest)
        return self

    def to_range_request(self) -> RangePartitionRequest:
        assert self.type_ == "range" and isinstance(self, RangePartitionRequest)
        return self


@dataclasses.dataclass(frozen=True)
class HashPartitionRequest(PartitionRequest):
    bucket: int


@dataclasses.dataclass(frozen=True)
class RangePartitionRequest(PartitionRequest):
    start_end_values: list[tuple[Any, Any]]


class ShuffleServiceInterface(Protocol[ShuffleData, IngestResult]):
    """An interface to a ShuffleService

    The job of a shuffle service is to `.ingest` results from the previous stage, perform partitioning on the data,
    and then expose a `.read` to consumers of the "shuffled" data.

    NOTE: `.read` should throw an error before the ShuffleService is informed of the target partitioning. This
    is because the ShuffleService needs to know how to partition the data before it can emit results.

    See BigQuery/Dremel video from CMU: https://www.youtube.com/watch?v=JxeITDS-xh0&ab_channel=CMUDatabaseGroup
    """

    ###
    # INGESTION:
    # These endpoints allow the ShuffleService to ingest data from the previous stage of the query
    ###

    def ingest(self, data: Iterator[ShuffleData]) -> list[IngestResult]:
        """Receive some data.

        NOTE: This will throw an error if called after `.close_ingest` has been called.
        """
        ...

    def set_input_stage_completed(self) -> None:
        """Inform the ShuffleService that all data from the previous stage has been ingested"""
        ...

    def is_input_stage_completed(self) -> bool:
        """Query whether or not the previous stage has completed ingestion"""
        ...

    ###
    # READ:
    # These endpoints allow clients to request data from the ShuffleService
    ###

    def read(self, request: PartitionRequest, max_num_rows: int) -> Iterator[ShuffleData]:
        """Retrieves ShuffleData from the shuffle service for the specified partition.

        This returns an iterator of ShuffleData

        When all data is guaranteed to be exhausted for the given request, the iterator will raise
        a StopIteration.
        """
        ...

    # TODO: Dynamic Partitioning
    #
    # We could have the ShuffleService expose running statistics (as data is being collected)
    # so that the coordinator can dynamically decide on an appropriate output partitioning scheme
    # before it attempts to request for output ShuffleData
    #
    # def get_current_statistics(self) -> ShuffleStatistics:
    #     """Retrieves the current statistics from the ShuffleService's currently ingested data"""
    #     ...
    #
    # def set_output_partitioning(self, spec: PartitioningSpec) -> None:
    #     """Sets the intended output partitioning scheme that should be emitted"""
    #     ...
