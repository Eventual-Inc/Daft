from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from daft.daft import PyDataSourceTask, StorageConfig

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

    from daft.dataframe import DataFrame
    from daft.io.partitioning import PartitionField
    from daft.io.pushdowns import Pushdowns
    from daft.recordbatch import RecordBatch
    from daft.recordbatch.micropartition import MicroPartition
    from daft.schema import Schema


__all__ = [
    "DataSource",
    "DataSourceTask",
]


class DataSource(ABC):
    """DataSource is a low-level interface for reading data into DataFrames.

    When a DataSource is read, it is split into multiple tasks which can be distributed
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

    def get_partition_fields(self) -> list[PartitionField]:
        """Returns the partitioning fields for this data source."""
        return []

    @abstractmethod
    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        """Yields tasks as they are discovered. Called during execution, not planning."""
        ...
        # https://mypy.readthedocs.io/en/latest/more_types.html#typing-async-generators
        # This tricks the type checker into treating this as an async generator.
        yield  # type: ignore[misc]

    def read(self) -> DataFrame:
        """Reads a DataSource as a DataFrame."""
        from daft.daft import ScanOperatorHandle
        from daft.dataframe import DataFrame
        from daft.logical.builder import LogicalPlanBuilder

        handle = ScanOperatorHandle.from_data_source(self)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)


class DataSourceTask(ABC):
    """DataSourceTask represents a partition of data that can be processed independently.

    Warning:
        This API is early in its development and is subject to change.
    """

    @property
    @abstractmethod
    def schema(self) -> Schema:
        """Returns the schema of the record batches produced by this task."""
        ...

    async def read(self) -> AsyncIterator[RecordBatch]:
        """Yields record batches. Called from an async execution context.

        The default implementation delegates to the deprecated
        get_micro_partitions for backwards compatibility.
        New subclasses should override this method directly.
        """
        try:
            parts = self.get_micro_partitions()
        except NotImplementedError:
            raise NotImplementedError(
                f"{type(self).__name__} must implement async def read(self) -> AsyncIterator[RecordBatch]"
            )
        warnings.warn(
            f"{type(self).__name__}.get_micro_partitions() is deprecated — override 'async def read()' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        for mp in parts:
            for rb in mp.get_record_batches():
                yield rb

    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        """Deprecated: override read instead."""
        raise NotImplementedError

    @staticmethod
    def parquet(
        path: str,
        schema: Schema,
        *,
        pushdowns: Pushdowns | None = None,
        num_rows: int | None = None,
        size_bytes: int | None = None,
        partition_values: RecordBatch | None = None,
        stats: RecordBatch | None = None,
        storage_config: StorageConfig | None = None,
    ) -> DataSourceTask:
        """Create a task that reads a Parquet file using the native reader.

        This is the recommended way to create scan tasks for Parquet files when
        building custom DataSource implementations (e.g., catalog
        connectors like Iceberg or Paimon).

        Partition pruning is the DataSource's responsibility — decide
        which files to yield in get_tasks rather than
        relying on the task factory to filter them out.

        Args:
            path: Path or URI of the Parquet file (e.g., ``"s3://bucket/file.parquet"``).
            schema: Schema to read the file with.
            pushdowns: Query pushdowns (filters, column projection, limit). Pass
                through the pushdowns received by DataSource.get_tasks.
            num_rows: Exact row count, if known. Enables metadata-only optimizations.
            size_bytes: On-disk file size in bytes. Used for task coalescing heuristics.
            partition_values: Single-row RecordBatch of partition column values to inject.
            stats: Column statistics as a RecordBatch for predicate pushdown evaluation.
            storage_config: Optional StorageConfig for IO credentials/settings.
                Defaults to ``StorageConfig(multithreaded_io=True)``.

        Example:
            class MyCatalogSource(DataSource):
                async def get_tasks(self, pushdowns):
                    for file in self.list_files():
                        yield DataSourceTask.parquet(
                            path=file.uri,
                            schema=self.schema,
                            pushdowns=pushdowns,
                            num_rows=file.row_count,
                            size_bytes=file.size_bytes,
                        )

        Returns:
            A DataSourceTask executed by the native Parquet reader.
        """
        inner = PyDataSourceTask.parquet(
            path=path,
            schema=schema._schema,
            pushdowns=pushdowns._to_pypushdowns() if pushdowns is not None else None,
            num_rows=num_rows,
            size_bytes=size_bytes,
            partition_values=partition_values._recordbatch if partition_values is not None else None,
            stats=stats._recordbatch if stats is not None else None,
            storage_config=storage_config,
        )

        return _RustDataSourceTask(inner)


class _RustDataSourceTask(DataSourceTask):
    """Wraps a Rust-backed ``PyDataSourceTask`` as a Python ``DataSourceTask``.

    This follows the same pattern as ``_RustTable(Table)`` in ``daft.catalog``:
    the Python ABC subclass delegates to the Rust object so that
    ``isinstance(task, DataSourceTask)`` holds.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: PyDataSourceTask) -> None:
        self._inner = inner

    @property
    def schema(self) -> Schema:
        from daft.schema import Schema

        return Schema._from_pyschema(self._inner.schema())

    async def read(self) -> AsyncIterator[RecordBatch]:
        raise NotImplementedError("Native scan tasks are executed by the Rust engine, not via read().")
        yield  # pragma: no cover
