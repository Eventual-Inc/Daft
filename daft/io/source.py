from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator

if TYPE_CHECKING:
    from daft.dataframe import DataFrame
    from daft.io.pushdowns import Pushdowns
    from daft.recordbatch import MicroPartition
    from daft.schema import Schema


__all__ = [
    "DataFrameSource",
    "DataFrameSourceTask",
]


class DataFrameSource(ABC):
    """DataFrameSource is a low-level interface for reading data into DataFrames.

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
        """Returns an iterator of tasks for this source.

        Returns:
            Iterable[DataFrameSourceTask]: An iterable of tasks that can be processed independently.
        """
        ...

    def read(self) -> DataFrame:
        """Reads a DataFrameSource as a DataFrame."""
        from daft.daft import ScanOperatorHandle
        from daft.dataframe import DataFrame
        from daft.io.__shim import _DataFrameSourceShim
        from daft.logical.builder import LogicalPlanBuilder

        scan = _DataFrameSourceShim(self)
        handle = ScanOperatorHandle.from_python_scan_operator(scan)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)


class DataFrameSourceTask(ABC):
    """DataFrameSourceTask represents a partition of data that can be processed independently.

    Warning:
        This API is early in its development and is subject to change.
    """

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema shared by each MicroPartition."""
        ...

    @abstractmethod
    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        """Executes this task to produce MicroPartitions.

        Returns:
            An iterable of MicroPartition objects containing the data for this task.
        """
        ...
