from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.recordbatch import MicroPartition
    from daft.schema import Schema

WriteResultType = TypeVar("WriteResultType")


@dataclass
class WriteResult(Generic[WriteResultType]):
    """Wrapper for result of the DataSink's `.write()` method.

    Attributes:
        result: The result from the write operation
        bytes_written: Size of the written data in bytes
        rows_written: Number of rows written
    """

    result: WriteResultType
    bytes_written: int
    rows_written: int


class DataSink(ABC, Generic[WriteResultType]):
    """Interface for writing data to a sink that is not built-in.

    When a DataFrame is written using the `.write_sink()` method, the following sequence occurs:

    1. The sink's `.start()` method is called once at the beginning of the write process.
    2. The DataFrame is executed, and its output is split into micropartitions.
    3. The sink's `.write()` method is invoked on each micropartition, potentially in parallel
       and distributed across multiple tasks or workers.
    4. After all writes complete, the resulting `WriteOutput` objects are gathered on a single node.
    5. The `.finalize()` method is then called with all write outputs to produce a final `MicroPartition`.

    Warning:
        This API is early in its development and is subject to change.
    """

    def name(self) -> str:
        """Optional custom sink name."""
        return "User-defined Data Sink"

    @abstractmethod
    def schema(self) -> Schema:
        """The expected schema for the micropartition returned by the `.finalize()` method of this DataSink.

        If this given schema does not match the actual schema of the micropartition at runtime, we throw an error.
        """
        raise NotImplementedError

    def start(self) -> None:
        """Optional callback for when a write operation begins.

        For example, this can be used to initialize resources, open connections, start a transaction etc.
        The default implementation does nothing.
        """
        pass

    @abstractmethod
    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[WriteResultType]]:
        """Writes a stream of micropartitions to the sink.

        This method should handle the ingestion of each micropartition and yield a result
        (e.g. metadata) for each successful write.

        Args:
            micropartitions (Iterator[MicroPartition]): An iterator of micropartitions to be written.

        Returns:
            Iterator[WriteResult[WriteResultType]]: An iterator of write results wrapped in a WriteOutput.
        """
        raise NotImplementedError

    def safe_write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[WriteResultType]]:
        """This method wraps the abstract `write()` method with a try block to reraise potentially unserializable exceptions.

        Args:
            micropartitions (Iterator[MicroPartition]): An iterator of micropartitions to be written.

        Returns:
            Iterator[WriteResult[WriteResultType]]: An iterator of write results wrapped in a WriteOutput.

        Raises:
            Exception: Any exception that occurs during the write operation.
        """
        try:
            yield from self.write(micropartitions)
        except Exception as e:
            raise RuntimeError(f"Exception occurred while writing to {self.name()}: {type(e).__name__}: {e!s}") from e

    @abstractmethod
    def finalize(self, write_results: list[WriteResult[WriteResultType]]) -> MicroPartition:
        """Finalizes the write process and returns a resulting micropartition.

        For example, this can be used to merge, summarize, or commit the results of individual writes
        into a single output micropartition.

        Args:
            write_results (list[WriteResult[WriteResultType]]): The list of results from the calls to `.write()`.

        Returns:
            MicroPartition: A final, single micropartition representing the result of all writes.
        """
        raise NotImplementedError
