from abc import ABC, abstractmethod
from typing import Generic, Iterator, TypeVar

from daft.logical.schema import Schema
from daft.recordbatch import MicroPartition

T = TypeVar("T")


class WriteOutput(Generic[T]):
    """Wrapper for output of the DataSink's `.write()` method."""

    _output: T


class DataSink(ABC, Generic[T]):
    """Interface for writing data to a sink that is not built-in."""

    def name(self) -> str:
        """Optional custom sink name."""
        return "Custom Data Sink"

    @abstractmethod
    def schema(self) -> Schema:
        """The expected schema for the micropartition returned by the `.finalize()` method of this DataSink.

        If this given schema does not match the actual schema of the micropartition at runtime, we throw an error.
        """
        raise NotImplementedError

    def start(self) -> None:
        """Optional callback for when a write starts. For example, this can be used to start a transaction."""
        pass

    @abstractmethod
    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteOutput[T]]:
        raise NotImplementedError

    @abstractmethod
    def finalize(self, results: list[WriteOutput[T]]) -> MicroPartition:
        raise NotImplementedError
