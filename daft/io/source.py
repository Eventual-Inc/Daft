from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from dataclasses import replace
from typing import TYPE_CHECKING

from daft.daft import FileFormatConfig, ParquetSourceConfig, ScanTask, StorageConfig

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
        from daft.io.__shim import _DataSourceShim
        from daft.logical.builder import LogicalPlanBuilder

        scan = _DataSourceShim(self)
        handle = ScanOperatorHandle.from_python_scan_operator(scan)
        builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
        return DataFrame(builder)


class DataSourceTask(ABC):
    """DataSourceTask represents a partition of data that can be processed independently.

    - :meth:`DataSourceTask.parquet` — read a Parquet file with the native reader

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
        :meth:`get_micro_partitions` for backwards compatibility.
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
        """Deprecated: override :meth:`read` instead."""
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
        building custom :class:`DataSource` implementations (e.g., catalog
        connectors like Iceberg or Paimon).

        Partition pruning is the :class:`DataSource`'s responsibility — decide
        which files to yield in :meth:`~DataSource.get_tasks` rather than
        relying on the task factory to filter them out.

        Args:
            path: Path or URI of the Parquet file (e.g., ``"s3://bucket/file.parquet"``).
            schema: Schema to read the file with.
            pushdowns: Query pushdowns (filters, column projection, limit). Pass
                through the pushdowns received by :meth:`DataSource.get_tasks`.
            num_rows: Exact row count, if known. Enables metadata-only optimizations.
            size_bytes: On-disk file size in bytes. Used for task coalescing heuristics.
            partition_values: Single-row RecordBatch of partition column values to inject.
            stats: Column statistics as a RecordBatch for predicate pushdown evaluation.
            storage_config: Optional :class:`StorageConfig` for IO credentials/settings.
                Defaults to ``StorageConfig(multithreaded_io=True)``.

        Returns:
            A DataSourceTask executed by the native Parquet reader.

        Example::

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
        """
        sc = storage_config if storage_config is not None else StorageConfig(multithreaded_io=True, io_config=None)

        # Strip partition_filters before constructing the ScanTask. In the
        # DataSource model, partition pruning is the DataSource's job (it
        # decides which files to yield in get_tasks). We keep the execution
        # pushdowns (filters, columns, limit) so the native reader can apply
        # them per-file. Without partition_filters, catalog_scan_task never
        # returns None.
        if pushdowns is not None:
            py_pd = replace(pushdowns, partition_filters=None)._to_pypushdowns()
        else:
            py_pd = None

        st = ScanTask.catalog_scan_task(
            file=path,
            file_format=FileFormatConfig.from_parquet_config(ParquetSourceConfig()),
            schema=schema._schema,
            storage_config=sc,
            num_rows=num_rows,
            size_bytes=size_bytes,
            iceberg_delete_files=None,
            pushdowns=py_pd,
            partition_values=partition_values._recordbatch if partition_values is not None else None,
            stats=stats._recordbatch if stats is not None else None,
        )
        assert st is not None, "catalog_scan_task returned None unexpectedly (partition_filters were stripped)"
        return _PyDataSourceTask(st, schema)


class _PyDataSourceTask(DataSourceTask):
    """Internal wrapper around a pre-built native ScanTask.

    This is not exactly our traditional _Py* wrapper class because we do
    not have the equivalent trait fully plumbed on the Rust side yet. This
    is a way to wrap the existing native ScanTasks in a DataSourceTask interface
    so we can use them in the DataSource model, and our shim is able to
    unwrap them into the native ScanTasks for execution.
    """

    __slots__ = ("_task", "_schema")

    def __init__(self, task: ScanTask, schema: Schema) -> None:
        self._task = task
        self._schema = schema

    def unwrap(self) -> ScanTask:
        """Unwraps the native ScanTask from this DataSourceTask, used by the shim."""
        return self._task

    @property
    def schema(self) -> Schema:
        return self._schema

    async def read(self) -> AsyncIterator[RecordBatch]:
        """We will actually implement this in the near future, but this path is unused for now."""
        raise NotImplementedError(
            "Native scan tasks are executed by the Rust engine. Subclass DataSourceTask for Python-based reading."
        )
        yield  # pragma: no cover
