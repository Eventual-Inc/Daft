from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, TypeVar

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.pushdowns import Pushdowns
    from daft.recordbatch import RecordBatch
    from daft.schema import Schema


__all__ = [
    "DataFrameSource",
    "DataFrameSourceTask",
]

R = TypeVar("R")


class DataFrameSource(ABC):
    """DataFrameSource is an interface for reading data into DataFrames.

    When a DataFrameSource is read, it is split into multiple tasks which can be distributed
    for parallel processing. Each task is responsible for reading a specific portion of
    the data (e.g., a file partition, a range of rows, or a subset of a database table)
    and converting it into RecordBatches. Implementations should ensure that tasks
    are appropriately sized to balance parallelism.

    Warning:
        This API is early in its development and is subject to change.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the source name which is useful for debugging."""
        ...

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema shared by each task's record batches."""
        ...

    @abstractmethod
    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataFrameSourceTask]:
        """Returns an iterable of tasks for this source.

        Returns:
            Iterable[DataFrameSourceTask]: An iterable of tasks that can be processed independently.
        """
        ...

    def read(self) -> DataFrame:
        """Reads this source into a DataFrame."""
        from daft.io.__shim import _read

        return _read(self)


class DataFrameSourceTask(ABC):
    """DataFrameSourceTask represents a partition of data that can be processed independently.

    Warning:
        This API is early in its development and is subject to change.
    """

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema shared by each RecordBatch."""
        ...

    @abstractmethod
    def get_record_batches(self) -> Iterator[RecordBatch]:
        """Executes this task and produces record batches from the source data.

        Returns:
            An iterable of RecordBatch objects containing the data for this task.
        """
        ...
