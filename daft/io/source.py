from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Protocol

if TYPE_CHECKING:
    from daft.io.pushdowns import Pushdowns
    from daft.recordbatch import RecordBatch
    from daft.schema import Schema


__all__ = [
    "DataSource",
    "DataSourceTask",
]


class DataSource(ABC):
    """DataSource is the primary abstraction for reading data.

    When a DataSource is read, it is split into multiple tasks which can be distributed
    for parallel processing. Each task is responsible for reading a specific portion of
    the data (e.g., a file partition, a range of rows, or a subset of a database table)
    and converting it into RecordBatch objects. Implementations should ensure that tasks
    are appropriately sized to balance parallelism.

    Warning:
        This API is early in its development and is subject to change.
    """

    @abstractmethod
    def name(self) -> str:
        """Returns the source name which is useful for debugging."""
        ...

    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema shared by each task's record batches."""
        ...

    @abstractmethod
    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        """Returns an iterable of tasks for this source.

        Args:
            pushdowns (Pushdowns): Pushdowns to be used at planning time.

        Returns:
            Iterable[DataSourceTask]: An iterable of tasks that can be processed independently.
        """
        ...


class DataSourceTask(ABC):
    """DataSourceTask represents a partition of data that can be processed independently.

    Warning:
        This API is early in its development and is subject to change.
    """

    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema shared by each RecordBatch."""
        ...

    @abstractmethod
    def get_batches(self) -> Iterator[RecordBatch]:
        """Executes this task and produces record batches from the source data.

        Returns:
            An iterable of RecordBatch objects containing the data for this task.
        """
        ...


class WithSchema(Protocol):
    def schema(self) -> Schema: ...

class DataSourceWithSchema(DataSource, WithSchema):
    ...

class DataSourceTaskWithSchema(DataSourceTask, WithSchema):
    ...
