from abc import ABC, abstractmethod
from typing import Generic, Iterator, List, TypeVar

from daft.recordbatch import MicroPartition

T = TypeVar("T")
R = TypeVar("R")


class DataSink(ABC, Generic[T, R]):
    """Interface for writing data to a sink that is not built-in."""

    def name(self) -> str:
        """Optional custom sink name."""
        return "Custom Data Sink"

    def start(self) -> None:
        """Optional callback for when a write starts. For example, this can be used to start a transaction."""
        pass

    @abstractmethod
    def write(self, micropartitions: Iterator[MicroPartition], **kwargs) -> Iterator[T]:
        raise NotImplementedError

    @abstractmethod
    def finish(self, results: List[T]) -> R:
        raise NotImplementedError
