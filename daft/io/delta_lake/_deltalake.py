# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations
import os
import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Union

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.dependencies import unity_catalog
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from datetime import datetime

    import deltalake
    import pyarrow as pa

    from daft.catalog.__unity._client import UnityCatalogTable


@PublicAPI
def read_deltalake(
    table: Union[str, "UnityCatalogTable"],
    version: Union[int, str, "datetime"] | None = None,
    io_config: IOConfig | None = None,
    ignore_deletion_vectors: bool = False,
    _multithreaded_io: bool | None = None,
) -> DataFrame:
    """Create a DataFrame from a Delta Lake table.

    Args:
        table: Either a URI for the Delta Lake table (supports remote URLs to object stores such as ``s3://`` or ``gs://``)
            or a ``UnityCatalogTable`` instance from a Unity Catalog client.
        version (optional): If int is passed, read the table with specified version number. Otherwise if string or datetime,
            read the timestamp version of the table. Strings must be RFC 3339 and ISO 8601 date and time format.
            Datetimes are assumed to be UTC timezone unless specified. By default, read the latest version of the table.
        io_config (optional): A custom :class:`~daft.daft.IOConfig` to use when accessing Delta Lake object storage data. Defaults to None.
        ignore_deletion_vectors (optional): Whether to skip checking for deletion vectors when reading the table. Defaults to False.
        _multithreaded_io (optional): Whether to use multithreading for IO threads. Setting this to False can be helpful in reducing
            the amount of system resources (number of connections and thread contention) when running in the Ray runner.
            Defaults to None, which will let Daft decide based on the runner it is currently using.

    Returns:
        DataFrame: A DataFrame with the schema converted from the specified Delta Lake table.

    Note:
        This function requires the use of [deltalake](https://delta-io.github.io/delta-rs/), a Python library for interacting with Delta Lake.

    Examples:
        Read a Delta Lake table from a local path:
        >>> df = daft.read_deltalake("some-table-uri")
        >>>
        >>> # Filters on this dataframe can now be pushed into the read operation from Delta Lake.
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()

        Read a Delta Lake table from a public S3 bucket:
        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_deltalake("s3://daft-oss-public-data/test_fixtures/delta_table/", io_config=io_config)
        >>> df.show()
    """
    from daft.io.delta_lake.delta_lake_scan import DeltaLakeScanOperator

    # If running on Ray, we want to limit the amount of concurrency and requests being made.
    # This is because each Ray worker process receives its own pool of thread workers and connections
    multithreaded_io = (
        (runners.get_or_create_runner().name != "ray") if _multithreaded_io is None else _multithreaded_io
    )

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    storage_config = StorageConfig(multithreaded_io, io_config)

    if isinstance(table, str):
        table_uri = os.path.expanduser(table)
    elif unity_catalog.module_available() and isinstance(table, unity_catalog.UnityCatalogTable):
        table_uri = table.table_uri

        # Override the storage_config with the one provided by Unity catalog
        recordbatch_io_config = table.io_config
        if recordbatch_io_config is not None:
            storage_config = StorageConfig(multithreaded_io, recordbatch_io_config)
    else:
        raise ValueError(
            f"table argument must be a table URI string or UnityCatalogTable instance, but got: {type(table)}, {table}"
        )
    delta_lake_operator = DeltaLakeScanOperator(
        table_uri, storage_config=storage_config, version=version, ignore_deletion_vectors=ignore_deletion_vectors
    )

    handle = ScanOperatorHandle.from_python_scan_operator(delta_lake_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    df = DataFrame(builder)
    df._resolved_deltalake_path = table_uri
    df._resolved_deltalake_io_config = io_config
    return df


def _resolve_deltalake_table_and_storage_options(
    table: Union[str, "UnityCatalogTable", "deltalake.DeltaTable"],
    io_config: IOConfig | None,
) -> tuple["deltalake.DeltaTable", dict[str, str]]:
    import deltalake

    from daft.io.object_store_options import io_config_to_storage_options

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    if isinstance(table, deltalake.DeltaTable):
        table_uri = table.table_uri
        storage_options = table._storage_options or {}
        new_storage_options = io_config_to_storage_options(io_config, table_uri)
        storage_options.update(new_storage_options or {})
        return table, storage_options

    if isinstance(table, str):
        table_uri = os.path.expanduser(table)
    elif unity_catalog.module_available() and isinstance(table, unity_catalog.UnityCatalogTable):
        table_uri = table.table_uri
        io_config = table.io_config
    else:
        raise ValueError(
            f"table argument must be a table URI string, DeltaTable, or UnityCatalogTable instance, but got: {type(table)}"
        )

    if io_config is None:
        raise ValueError("io_config was not provided and could not be retrieved from defaults.")

    storage_options = io_config_to_storage_options(io_config, table_uri) or {}
    return deltalake.DeltaTable(table_uri, storage_options=storage_options), storage_options


@PublicAPI
def history_deltalake(
    table: Union[str, "UnityCatalogTable", "deltalake.DeltaTable"],
    limit: int | None = None,
    io_config: IOConfig | None = None,
    parse_operation_metrics: bool = True,
) -> list[dict[str, Any]]:
    """Return commit history for a Delta Lake table.

    Args:
        table: Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
        limit: Maximum number of commits to return. ``None`` returns full history.
        io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
        parse_operation_metrics: If ``True``, parse JSON-encoded ``operationMetrics`` into dictionaries.

    Returns:
        list[dict[str, Any]]: Delta commit history entries.
    """
    resolved_table, _ = _resolve_deltalake_table_and_storage_options(table, io_config)
    history = resolved_table.history(limit=limit)

    if not parse_operation_metrics:
        return history

    normalized_history: list[dict[str, Any]] = []
    for entry in history:
        normalized = dict(entry)
        operation_metrics = normalized.get("operationMetrics")
        if isinstance(operation_metrics, str):
            try:
                normalized["operationMetrics"] = json.loads(operation_metrics)
            except json.JSONDecodeError:
                pass
        normalized_history.append(normalized)

    return normalized_history


@PublicAPI
def delete_deltalake(
    table: Union[str, "UnityCatalogTable", "deltalake.DeltaTable"],
    predicate: str | None = None,
    io_config: IOConfig | None = None,
    custom_metadata: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Delete rows from a Delta Lake table.

    Args:
        table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
        predicate: SQL predicate that selects rows to delete. If ``None``, deletes all rows.
        io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
        custom_metadata: Optional key-value metadata to attach to the Delta commit.

    Returns:
        dict[str, Any]: Delta-rs metrics from the delete operation.
    """
    from deltalake import CommitProperties

    resolved_table, _ = _resolve_deltalake_table_and_storage_options(table, io_config)
    commit_properties = CommitProperties(custom_metadata=custom_metadata)
    return resolved_table.delete(predicate=predicate, commit_properties=commit_properties)


@PublicAPI
def update_deltalake(
    table: Union[str, "UnityCatalogTable", "deltalake.DeltaTable"],
    updates: "Mapping[str, str]",
    predicate: str | None = None,
    io_config: IOConfig | None = None,
    custom_metadata: dict[str, str] | None = None,
    safe_cast: bool = True,
) -> dict[str, Any]:
    """Update rows in a Delta Lake table.

    Args:
        table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
        updates: Mapping from column name to SQL update expression.
        predicate: SQL predicate that selects rows to update. If ``None``, updates all rows.
        io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
        custom_metadata: Optional key-value metadata to attach to the Delta commit.
        safe_cast: If ``True``, safely cast update expressions to target column types when needed.

    Returns:
        dict[str, Any]: Delta-rs metrics from the update operation.
    """
    from deltalake import CommitProperties

    resolved_table, _ = _resolve_deltalake_table_and_storage_options(table, io_config)
    commit_properties = CommitProperties(custom_metadata=custom_metadata)
    return resolved_table.update(
        updates=dict(updates),
        predicate=predicate,
        error_on_type_mismatch=not safe_cast,
        commit_properties=commit_properties,
    )


@PublicAPI
def merge_deltalake(
    table: Union[str, "UnityCatalogTable", "deltalake.DeltaTable"],
    source: Union[DataFrame, "pa.Table"],
    predicate: str,
    io_config: IOConfig | None = None,
    source_alias: str = "source",
    target_alias: str = "target",
    custom_metadata: dict[str, str] | None = None,
    safe_cast: bool = True,
    merge_schema: bool = False,
    writer_properties: "deltalake.WriterProperties | None" = None,
    streamed_exec: bool = True,
    max_spill_size: int | None = None,
    max_temp_directory_size: int | None = None,
    post_commithook_properties: "deltalake.PostCommitHookProperties | None" = None,
) -> "DeltaMergeBuilder":
    """Create a Delta Lake MERGE operation builder for composable merge clauses.

    Uses Daft's ``DataSink`` interface to distribute the merge across workers.
    Each worker partition merges its chunk of source data against the Delta table
    via delta-rs, so the full source never needs to be collected to the head node.

    Returns a builder for chaining merge clauses, then call ``.execute()`` to run.

    Args:
        table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
        source: Source records to merge from, as a Daft DataFrame or PyArrow table.
        predicate: SQL merge predicate between ``target_alias`` and ``source_alias``.
        io_config: Optional :class:`~daft.daft.IOConfig` used for object storage access.
        source_alias: SQL alias for the source side of the merge predicate.
        target_alias: SQL alias for the target side of the merge predicate.
        custom_metadata: Optional key-value metadata to attach to the Delta commit.
        safe_cast: If ``True``, safely cast source expressions to target column types when needed.
        merge_schema: If ``True``, allow schema evolution during merge.
        writer_properties: Optional Arrow writer properties to use when writing files.
        streamed_exec: If ``True``, use the streamed execution path.
        max_spill_size: Maximum spill size in bytes for streamed execution.
        max_temp_directory_size: Maximum temporary directory size in bytes for streamed execution.
        post_commithook_properties: Optional post-commit hook properties.

    Returns:
        DeltaMergeBuilder: A builder object for chaining merge clauses with ``.execute()`` finalizer that returns a DataFrame.

    Note:
        The returned DataFrame from ``.execute()`` contains merge metrics as columns and stores the raw metrics dict in ``_metadata["merge_metrics"]``.

        Because each partition merges independently, ``when_not_matched_by_source_update``
        and ``when_not_matched_by_source_delete`` are **not supported**. Use
        :func:`distributed_merge_deltalake` for those clauses.

    Examples:
        Basic upsert (update matching rows, insert new rows)::

            result = (
                merge_deltalake(
                    table="path/to/table",
                    source=source_df,
                    predicate="target.id = source.id"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            metrics = result._metadata["merge_metrics"]
            print(f"Inserted: {metrics['num_target_rows_inserted']}")
            print(f"Updated: {metrics['num_target_rows_updated']}")

        State tracking with multiple conditions::

            result = (
                merge_deltalake(
                    table="path/to/table",
                    source=source_df,
                    predicate="target.id = source.id"
                )
                .when_matched_update(
                    predicate="source.attributes != target.attributes",
                    updates={"attributes": "source.attributes", "status": "'UPDATED'"}
                )
                .when_matched_update(
                    predicate="source.attributes = target.attributes",
                    updates={"status": "'UNCHANGED'"}
                )
                .when_not_matched_insert_all()
                .execute()
            )
            metrics = result._metadata["merge_metrics"]
    """
    resolved_table, storage_options = _resolve_deltalake_table_and_storage_options(table, io_config)

    # If source is a PyArrow table, wrap it so we have a uniform DataFrame path
    if not isinstance(source, DataFrame):
        source = DataFrame._from_arrow(source)

    return DeltaMergeBuilder(
        table_uri=resolved_table.table_uri,
        storage_options=storage_options,
        source=source,
        predicate=predicate,
        source_alias=source_alias,
        target_alias=target_alias,
        custom_metadata=custom_metadata,
        safe_cast=safe_cast,
        merge_schema=merge_schema,
        writer_properties=writer_properties,
        streamed_exec=streamed_exec,
        max_spill_size=max_spill_size,
        max_temp_directory_size=max_temp_directory_size,
        post_commithook_properties=post_commithook_properties,
    )


class _DeltaLakeMergeSink:
    """DataSink that performs delta-rs merge on each worker partition.

    Each worker receives a chunk of source data and merges it against the
    full Delta table via delta-rs. This distributes the merge across workers
    so the head node never needs to hold the entire source in memory.

    Note: ``when_not_matched_by_source_*`` clauses are NOT supported because
    each partition only sees a subset of source rows — those clauses require
    full-source visibility.
    """

    def __init__(
        self,
        table_uri: str,
        storage_options: dict[str, str],
        predicate: str,
        source_alias: str,
        target_alias: str,
        custom_metadata: dict[str, str] | None,
        safe_cast: bool,
        merge_schema: bool,
        writer_properties: Any,
        streamed_exec: bool,
        max_spill_size: int | None,
        max_temp_directory_size: int | None,
        post_commithook_properties: Any,
        matched_updates: list,
        matched_deletes: list,
        not_matched_inserts: list,
    ) -> None:
        self._table_uri = table_uri
        self._storage_options = storage_options
        self._predicate = predicate
        self._source_alias = source_alias
        self._target_alias = target_alias
        self._custom_metadata = custom_metadata
        self._safe_cast = safe_cast
        self._merge_schema = merge_schema
        self._writer_properties = writer_properties
        self._streamed_exec = streamed_exec
        self._max_spill_size = max_spill_size
        self._max_temp_directory_size = max_temp_directory_size
        self._post_commithook_properties = post_commithook_properties
        self._matched_updates = matched_updates
        self._matched_deletes = matched_deletes
        self._not_matched_inserts = not_matched_inserts

    def name(self) -> str:
        return "DeltaLakeMergeSink"

    def schema(self):
        from daft.datatype import DataType as DaftDataType
        from daft.logical.schema import Schema

        return Schema._from_field_name_and_types([("merge_metrics", DaftDataType.python())])

    def start(self) -> None:
        pass

    def write(self, micropartitions):
        from daft.io.sink import WriteResult

        for micropartition in micropartitions:
            num_bytes = micropartition.size_bytes()
            num_rows = len(micropartition)
            if num_rows == 0:
                continue

            arrow_table = micropartition.to_arrow()

            import deltalake
            from deltalake import CommitProperties

            dt = deltalake.DeltaTable(self._table_uri, storage_options=self._storage_options)
            commit_properties = CommitProperties(custom_metadata=self._custom_metadata)

            # Compute spill defaults
            _max_spill = self._max_spill_size
            _max_temp = self._max_temp_directory_size
            if _max_spill is None or _max_temp is None:
                import shutil

                if _max_spill is None:
                    try:
                        total_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
                    except (AttributeError, ValueError):
                        total_mem = 10 * 1024 * 1024 * 1024
                    _max_spill = int(total_mem * 0.7)
                if _max_temp is None:
                    try:
                        disk_usage = shutil.disk_usage("/tmp")
                        _max_temp = int(disk_usage.free * 0.7)
                    except OSError:
                        _max_temp = 100 * 1024 * 1024 * 1024

            merger = dt.merge(
                source=arrow_table,
                predicate=self._predicate,
                source_alias=self._source_alias,
                target_alias=self._target_alias,
                merge_schema=self._merge_schema,
                error_on_type_mismatch=not self._safe_cast,
                writer_properties=self._writer_properties,
                streamed_exec=self._streamed_exec,
                max_spill_size=_max_spill,
                max_temp_directory_size=_max_temp,
                commit_properties=commit_properties,
                post_commithook_properties=self._post_commithook_properties,
            )

            # Replay the builder clauses
            for clause_type, args in self._matched_updates:
                if clause_type == "update":
                    merger = merger.when_matched_update(args["updates"], args.get("predicate"))
                elif clause_type == "update_all":
                    merger = merger.when_matched_update_all(args.get("predicate"), args.get("except_cols"))

            for clause_type, args in self._matched_deletes:
                merger = merger.when_matched_delete(args.get("predicate"))

            for clause_type, args in self._not_matched_inserts:
                if clause_type == "insert":
                    merger = merger.when_not_matched_insert(args["updates"], args.get("predicate"))
                elif clause_type == "insert_all":
                    merger = merger.when_not_matched_insert_all(args.get("predicate"), args.get("except_cols"))

            raw_metrics = merger.execute()

            yield WriteResult(
                result=raw_metrics,
                bytes_written=num_bytes,
                rows_written=num_rows,
            )

    def safe_write(self, micropartitions):
        try:
            yield from self.write(micropartitions)
        except Exception as e:
            raise RuntimeError(f"Exception in {self.name()}: {type(e).__name__}: {e!s}") from e

    def finalize(self, write_results):
        from daft.recordbatch import MicroPartition

        # Aggregate metrics across all partitions
        aggregated: dict[str, int] = {}
        for wr in write_results:
            metrics = wr.result
            if isinstance(metrics, dict):
                for k, v in metrics.items():
                    if isinstance(v, (int, float)):
                        aggregated[k] = aggregated.get(k, 0) + int(v)

        return MicroPartition.from_pydict({"merge_metrics": [aggregated]})


class DeltaMergeBuilder:
    """Builder for Delta Lake merge using Daft's DataSink interface.

    Each worker partition merges its chunk of source data against the Delta
    table via delta-rs. The full source never needs to be collected to the
    head node.
    """

    def __init__(
        self,
        table_uri: str,
        storage_options: dict[str, str],
        source: DataFrame,
        predicate: str,
        source_alias: str,
        target_alias: str,
        custom_metadata: dict[str, str] | None,
        safe_cast: bool,
        merge_schema: bool,
        writer_properties: Any,
        streamed_exec: bool,
        max_spill_size: int | None,
        max_temp_directory_size: int | None,
        post_commithook_properties: Any,
    ) -> None:
        self._table_uri = table_uri
        self._storage_options = storage_options
        self._source = source
        self._predicate = predicate
        self._source_alias = source_alias
        self._target_alias = target_alias
        self._custom_metadata = custom_metadata
        self._safe_cast = safe_cast
        self._merge_schema = merge_schema
        self._writer_properties = writer_properties
        self._streamed_exec = streamed_exec
        self._max_spill_size = max_spill_size
        self._max_temp_directory_size = max_temp_directory_size
        self._post_commithook_properties = post_commithook_properties

        # Clauses stored as (clause_type, args_dict) tuples for serialization
        self._matched_updates: list[tuple[str, dict]] = []
        self._matched_deletes: list[tuple[str, dict]] = []
        self._not_matched_inserts: list[tuple[str, dict]] = []

    def when_matched_update(
        self,
        updates: "Mapping[str, str]",
        predicate: str | None = None,
    ) -> "DeltaMergeBuilder":
        """Add a ``when_matched_update`` clause to the merge.

        Args:
            updates: Mapping from column name to SQL update expression.
            predicate: Optional SQL predicate for matched rows.

        Returns:
            Self for method chaining.
        """
        self._matched_updates.append(("update", {"updates": dict(updates), "predicate": predicate}))
        return self

    def when_matched_update_all(
        self,
        predicate: str | None = None,
        except_cols: list[str] | None = None,
    ) -> "DeltaMergeBuilder":
        """Add a ``when_matched_update_all`` clause to the merge.

        Args:
            predicate: Optional SQL predicate for matched rows.
            except_cols: List of columns to exclude from update.

        Returns:
            Self for method chaining.
        """
        self._matched_updates.append(("update_all", {"predicate": predicate, "except_cols": except_cols}))
        return self

    def when_matched_delete(self, predicate: str | None = None) -> "DeltaMergeBuilder":
        """Add a ``when_matched_delete`` clause to the merge."""
        self._matched_deletes.append(("delete", {"predicate": predicate}))
        return self

    def when_not_matched_insert(
        self,
        updates: "Mapping[str, str]",
        predicate: str | None = None,
    ) -> "DeltaMergeBuilder":
        """Add a ``when_not_matched_insert`` clause to the merge.

        Args:
            updates: Mapping from column name to SQL insert expression.
            predicate: Optional SQL predicate for unmatched rows.

        Returns:
            Self for method chaining.
        """
        self._not_matched_inserts.append(("insert", {"updates": dict(updates), "predicate": predicate}))
        return self

    def when_not_matched_insert_all(
        self,
        predicate: str | None = None,
        except_cols: list[str] | None = None,
    ) -> "DeltaMergeBuilder":
        """Add a ``when_not_matched_insert_all`` clause to the merge.

        Args:
            predicate: Optional SQL predicate for unmatched rows.
            except_cols: List of columns to exclude from insert.

        Returns:
            Self for method chaining.
        """
        self._not_matched_inserts.append(("insert_all", {"predicate": predicate, "except_cols": except_cols}))
        return self

    def when_not_matched_by_source_update(
        self,
        updates: "Mapping[str, str]",
        predicate: str | None = None,
    ) -> "DeltaMergeBuilder":
        """Not supported in per-partition merge. Use :func:`distributed_merge_deltalake` instead."""
        raise NotImplementedError(
            "when_not_matched_by_source_update is not supported with per-partition merge. "
            "Use daft.distributed_merge_deltalake() which performs a full outer join."
        )

    def when_not_matched_by_source_delete(self, predicate: str | None = None) -> "DeltaMergeBuilder":
        """Not supported in per-partition merge. Use :func:`distributed_merge_deltalake` instead."""
        raise NotImplementedError(
            "when_not_matched_by_source_delete is not supported with per-partition merge. "
            "Use daft.distributed_merge_deltalake() which performs a full outer join."
        )

    def execute(self) -> "DataFrame":
        """Execute the merge operation using Daft's DataSink interface.

        Each worker partition merges its source chunk against the Delta table
        via delta-rs. Metrics are aggregated across all partitions.

        Returns:
            DataFrame: A single-row DataFrame with columns for each merge metric.
        """
        sink = _DeltaLakeMergeSink(
            table_uri=self._table_uri,
            storage_options=self._storage_options,
            predicate=self._predicate,
            source_alias=self._source_alias,
            target_alias=self._target_alias,
            custom_metadata=self._custom_metadata,
            safe_cast=self._safe_cast,
            merge_schema=self._merge_schema,
            writer_properties=self._writer_properties,
            streamed_exec=self._streamed_exec,
            max_spill_size=self._max_spill_size,
            max_temp_directory_size=self._max_temp_directory_size,
            post_commithook_properties=self._post_commithook_properties,
            matched_updates=self._matched_updates,
            matched_deletes=self._matched_deletes,
            not_matched_inserts=self._not_matched_inserts,
        )

        sink.start()
        from daft.logical.builder import LogicalPlanBuilder

        builder = self._source._builder.write_datasink(sink.name(), sink)
        write_df = DataFrame(builder)
        write_df.collect()

        results = write_df.to_pydict()
        assert "write_results" in results
        finalized = sink.finalize(results["write_results"])

        # Extract aggregated metrics
        raw_metrics = finalized.to_pydict()["merge_metrics"][0]
        if not isinstance(raw_metrics, dict):
            raw_metrics = {}

        return _format_merge_metrics_as_dataframe(raw_metrics)


def _format_merge_metrics_as_dataframe(raw_metrics: dict[str, Any]) -> "DataFrame":
    """Format merge metrics as a single-row DataFrame with metrics in metadata.

    Args:
        raw_metrics: Raw metrics dict from deltalake merge operation.

    Returns:
        A DataFrame with a single row containing all metrics as columns.
        The metrics dict is also stored in _metadata.
    """
    import pyarrow as pa

    # Create a single-row DataFrame with all metrics
    metrics_data = {
        "num_source_rows": pa.array([raw_metrics.get("num_source_rows", 0)], type=pa.int64()),
        "num_target_rows_inserted": pa.array([raw_metrics.get("num_target_rows_inserted", 0)], type=pa.int64()),
        "num_target_rows_updated": pa.array([raw_metrics.get("num_target_rows_updated", 0)], type=pa.int64()),
        "num_target_rows_deleted": pa.array([raw_metrics.get("num_target_rows_deleted", 0)], type=pa.int64()),
        "num_target_rows_copied": pa.array([raw_metrics.get("num_target_rows_copied", 0)], type=pa.int64()),
        "num_output_rows": pa.array([raw_metrics.get("num_output_rows", 0)], type=pa.int64()),
        "num_target_files_added": pa.array([raw_metrics.get("num_target_files_added", 0)], type=pa.int64()),
        "num_target_files_removed": pa.array([raw_metrics.get("num_target_files_removed", 0)], type=pa.int64()),
        "execution_time_ms": pa.array([raw_metrics.get("execution_time_ms", 0)], type=pa.int64()),
        "scan_time_ms": pa.array([raw_metrics.get("scan_time_ms", 0)], type=pa.int64()),
        "rewrite_time_ms": pa.array([raw_metrics.get("rewrite_time_ms", 0)], type=pa.int64()),
    }

    df = DataFrame._from_arrow(pa.table(metrics_data))
    # Store the raw metrics dict in metadata for programmatic access
    df._metadata = {"merge_metrics": raw_metrics}
    return df




@PublicAPI
def distributed_merge_deltalake(
    table: Union[str, "UnityCatalogTable", "deltalake.DeltaTable"],
    source: DataFrame,
    predicate: str,
    on: Union[str, list[str], None] = None,
    io_config: IOConfig | None = None,
    source_alias: str = "source",
    target_alias: str = "target",
    custom_metadata: dict[str, str] | None = None,
) -> "DistributedDeltaMergeBuilder":
    """Create a distributed Delta Lake MERGE builder that uses Daft's distributed join.

    Unlike :func:`merge_deltalake` which runs the entire merge on a single process
    via delta-rs/DataFusion, this function distributes the join across all workers
    in your cluster. The merged result is written back via streaming overwrite.

    Returns a builder for chaining merge clauses, then call ``.execute()`` to run.

    Args:
        table: Destination Delta table URI, ``deltalake.DeltaTable``, or ``UnityCatalogTable``.
        source: Source Daft DataFrame to merge from.
        predicate: SQL merge predicate (e.g. ``"target.id = source.id"``).
            Join keys are extracted from equality conditions. Any additional
            conditions (e.g. ``"AND source.region = target.region"``) are applied
            as a post-join filter on matched rows.
        on: Explicit join key column name(s). If ``None``, keys are parsed from
            the ``predicate``. Providing ``on`` overrides predicate-derived keys.
        io_config: Optional :class:`~daft.daft.IOConfig` for object storage access.
        source_alias: Alias for the source side (used in update expressions).
        target_alias: Alias for the target side (used in update expressions).
        custom_metadata: Optional metadata to attach to the Delta commit.

    Returns:
        DistributedDeltaMergeBuilder: A builder for chaining merge clauses.

    Note:
        This distributes the join across all workers but writes back as a full
        table overwrite. Best for cases where source is large relative to target,
        or when single-node merge causes OOM.

    Examples:
        Using predicate only (keys auto-extracted)::

            result = (
                daft.distributed_merge_deltalake(
                    table="s3://bucket/table",
                    source=source_df,
                    predicate="target.entity_id = source.entity_id",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

        With explicit join keys::

            result = (
                daft.distributed_merge_deltalake(
                    table="s3://bucket/table",
                    source=source_df,
                    predicate="target.entity_id = source.entity_id",
                    on="entity_id",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
    """
    # Parse predicate to extract join keys and residual conditions
    join_keys, residual_predicates = _parse_merge_predicate(predicate, source_alias, target_alias)

    if on is not None:
        # Explicit keys override predicate-derived keys
        if isinstance(on, str):
            on = [on]
    elif join_keys:
        on = join_keys
    else:
        raise ValueError(
            f"Could not extract join keys from predicate '{predicate}'. "
            f"Provide explicit `on` parameter with the join key column name(s)."
        )

    resolved_table, storage_options = _resolve_deltalake_table_and_storage_options(table, io_config)

    return DistributedDeltaMergeBuilder(
        resolved_table=resolved_table,
        storage_options=storage_options,
        source=source,
        on=on,
        io_config=io_config,
        source_alias=source_alias,
        target_alias=target_alias,
        custom_metadata=custom_metadata,
        residual_predicates=residual_predicates,
    )


def _parse_merge_predicate(
    predicate: str,
    source_alias: str,
    target_alias: str,
) -> tuple[list[str], list[str]]:
    """Parse a merge predicate into join keys and residual conditions.

    Extracts equi-join keys from "target.col = source.col" patterns.
    Returns remaining conditions as residual predicates.

    Returns:
        (join_keys, residual_predicates): list of column names for join,
        and list of unparsed predicate fragments.
    """
    join_keys: list[str] = []
    residual_predicates: list[str] = []

    # Split on AND (case-insensitive)
    import re
    original_parts = [p.strip() for p in re.split(r"\s+[Aa][Nn][Dd]\s+", predicate)]

    for part in original_parts:
        part = part.strip()
        if not part:
            continue

        # Try to match "target.col = source.col" or "source.col = target.col"
        if " = " in part and " != " not in part and " IS " not in part.upper():
            left, right = part.split(" = ", 1)
            left = left.strip()
            right = right.strip()

            # target.X = source.X
            if left.startswith(f"{target_alias}.") and right.startswith(f"{source_alias}."):
                target_col = left[len(target_alias) + 1:]
                source_col = right[len(source_alias) + 1:]
                if target_col == source_col:
                    join_keys.append(target_col)
                    continue

            # source.X = target.X
            if left.startswith(f"{source_alias}.") and right.startswith(f"{target_alias}."):
                source_col = left[len(source_alias) + 1:]
                target_col = right[len(target_alias) + 1:]
                if target_col == source_col:
                    join_keys.append(target_col)
                    continue

        # Not a simple equi-join key — keep as residual
        residual_predicates.append(part)

    return join_keys, residual_predicates


class DistributedDeltaMergeBuilder:
    """Builder for distributed Delta Lake merge using Daft's distributed join engine.

    Mirrors the :class:`DeltaMergeBuilder` API but executes the join distributed
    across all workers instead of on a single driver process.
    """

    # Type aliases for predicate/update values: str (parsed) or Expression (native)
    _Pred = Union[str, "Expression", None]
    _UpdateVal = Union[str, "Expression"]
    _UpdateMap = Union[Mapping[str, str], Mapping[str, "Expression"], Mapping[str, Union[str, "Expression"]]]

    def __init__(
        self,
        resolved_table: "deltalake.DeltaTable",
        storage_options: dict[str, str],
        source: DataFrame,
        on: list[str],
        io_config: IOConfig | None,
        source_alias: str,
        target_alias: str,
        custom_metadata: dict[str, str] | None,
        residual_predicates: list[str] | None = None,
    ) -> None:
        self._resolved_table = resolved_table
        self._storage_options = storage_options
        self._source = source
        self._on = on
        self._io_config = io_config
        self._source_alias = source_alias
        self._target_alias = target_alias
        self._custom_metadata = custom_metadata
        self._residual_predicates = residual_predicates or []

        self._matched_updates: list[tuple[Mapping[str, Any] | None, Any]] = []
        self._matched_deletes: list[Any] = []
        self._not_matched_inserts: list[tuple[Mapping[str, Any] | None, Any]] = []
        self._not_matched_by_source_updates: list[tuple[Mapping[str, Any] | None, Any]] = []
        self._not_matched_by_source_deletes: list[Any] = []
        self._insert_all: bool = False
        self._update_all: bool = False

    def source_col(self, name: str) -> "Expression":
        """Reference a source column in the joined result.

        After the outer join, source columns are suffixed with ``.__src__``
        (except join keys which are unified). This helper abstracts that away.

        Args:
            name: Column name in the source DataFrame.

        Returns:
            Expression referencing the source column.

        Example::

            builder.when_matched_update(
                predicate=builder.source_col("hash") != builder.target_col("hash"),
                updates={"value": builder.source_col("value")},
            )
        """
        from daft import col

        if name in self._on:
            return col(name)
        return col(f"{name}.__src__")

    def target_col(self, name: str) -> "Expression":
        """Reference a target column in the joined result.

        Args:
            name: Column name in the target Delta table.

        Returns:
            Expression referencing the target column.
        """
        from daft import col

        return col(name)

    def when_matched_update(
        self,
        updates: "_UpdateMap",
        predicate: "_Pred" = None,
    ) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN MATCHED THEN UPDATE clause.

        Args:
            updates: Mapping from column name to value expression. Values can be
                     SQL-like strings (``"source.value"``) or native Daft expressions
                     (``builder.source_col("value")``).
            predicate: Optional condition for matched rows. Can be a SQL-like string
                       or a native Daft boolean expression.

        Returns:
            Self for method chaining.
        """
        self._matched_updates.append((dict(updates), predicate))
        return self

    def when_matched_update_all(
        self,
        predicate: "_Pred" = None,
    ) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN MATCHED THEN UPDATE ALL clause (update all source columns).

        Args:
            predicate: Optional condition for matched rows.

        Returns:
            Self for method chaining.
        """
        self._update_all = True
        self._matched_updates.append((None, predicate))
        return self

    def when_matched_delete(self, predicate: "_Pred" = None) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN MATCHED THEN DELETE clause."""
        self._matched_deletes.append(predicate)
        return self

    def when_not_matched_insert(
        self,
        updates: "_UpdateMap",
        predicate: "_Pred" = None,
    ) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN NOT MATCHED THEN INSERT clause.

        Args:
            updates: Mapping from column name to expression for the new row.
            predicate: Optional condition for unmatched source rows.

        Returns:
            Self for method chaining.
        """
        self._not_matched_inserts.append((dict(updates), predicate))
        return self

    def when_not_matched_insert_all(
        self,
        predicate: "_Pred" = None,
    ) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN NOT MATCHED THEN INSERT ALL clause.

        Args:
            predicate: Optional condition for unmatched source rows.

        Returns:
            Self for method chaining.
        """
        self._insert_all = True
        self._not_matched_inserts.append((None, predicate))
        return self

    def when_not_matched_by_source_update(
        self,
        updates: "_UpdateMap",
        predicate: "_Pred" = None,
    ) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN NOT MATCHED BY SOURCE THEN UPDATE clause.

        Args:
            updates: Mapping from column name to expression for target-only rows.
            predicate: Optional condition for target-only rows.

        Returns:
            Self for method chaining.
        """
        self._not_matched_by_source_updates.append((dict(updates), predicate))
        return self

    def when_not_matched_by_source_delete(self, predicate: "_Pred" = None) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN NOT MATCHED BY SOURCE THEN DELETE clause."""
        self._not_matched_by_source_deletes.append(predicate)
        return self

    @staticmethod
    def _build_partition_filter(partition_cols: list[str], affected_parts: dict) -> Any:
        """Build a Daft expression that matches rows in any affected partition."""
        from daft import col, lit

        combined = lit(False)
        num_rows = len(affected_parts[partition_cols[0]])
        for i in range(num_rows):
            row_cond = lit(True)
            for pcol in partition_cols:
                row_cond = row_cond & (col(pcol) == lit(affected_parts[pcol][i]))
            combined = combined | row_cond
        return combined

    def execute(self) -> DataFrame:
        """Execute the distributed merge and return a DataFrame with metrics.

        Performs a distributed FULL OUTER JOIN across all workers, applies merge
        semantics using Daft expressions, and writes the result back to the Delta
        table as a streaming overwrite.

        Returns:
            DataFrame: A single-row DataFrame with merge metrics columns.
                       Raw metrics dict also stored in ``._metadata["merge_metrics"]``.
        """
        import pyarrow as pa

        import daft
        from daft import DataType, col, lit
        from daft.expressions.expressions import Expression
        from daft.functions import when

        def _resolve_predicate(pred: Any) -> Any:
            """Resolve a predicate: pass Expression through, parse strings."""
            if pred is None:
                return None
            if isinstance(pred, Expression):
                return pred
            return _parse_predicate(pred, self._source_alias, self._target_alias, self._on)

        def _resolve_update_val(val: Any) -> Any:
            """Resolve an update value: pass Expression through, parse strings."""
            if isinstance(val, Expression):
                return val
            return _resolve_expr(val, self._source_alias, self._target_alias, self._on)

        # Read target as a distributed DataFrame
        target = daft.read_deltalake(self._resolved_table.table_uri, io_config=self._io_config)
        target_schema = target.schema()
        target_columns = [f.name for f in target_schema]

        # Count target rows for metrics
        _SRC_MARKER = "__daft_dm_src__"
        _TGT_MARKER = "__daft_dm_tgt__"

        source_tagged = self._source.with_column(_SRC_MARKER, lit(True))
        target_tagged = target.with_column(_TGT_MARKER, lit(True))

        # Source columns (non-key)
        source_schema = self._source.schema()
        source_columns = [f.name for f in source_schema]

        # Distributed FULL OUTER JOIN
        joined = target_tagged.join(
            source_tagged,
            on=self._on,
            how="outer",
            suffix=".__src__",
        )

        # Row categories
        is_matched = col(_TGT_MARKER).not_null() & col(_SRC_MARKER).not_null()
        is_source_only = col(_TGT_MARKER).is_null() & col(_SRC_MARKER).not_null()
        is_target_only = col(_TGT_MARKER).not_null() & col(_SRC_MARKER).is_null()

        # Apply residual predicate conditions from the merge predicate.
        # Rows that join on keys but fail the residual condition are treated as
        # "not matched" (source-only for source side, target-only for target side).
        if self._residual_predicates:
            residual_cond = lit(True)
            for rp in self._residual_predicates:
                residual_cond = residual_cond & _resolve_predicate(rp)
            # Rows that joined but fail residual → reclassify
            is_matched = is_matched & residual_cond
            # Joined rows that fail residual: treat target side as target-only, source side as source-only
            joined_but_no_match = col(_TGT_MARKER).not_null() & col(_SRC_MARKER).not_null() & ~residual_cond
            is_source_only = is_source_only | joined_but_no_match
            is_target_only = is_target_only | joined_but_no_match

        # Determine which matched rows to delete
        matched_delete_cond = lit(False)
        for pred_str in self._matched_deletes:
            if pred_str is not None:
                matched_delete_cond = matched_delete_cond | _resolve_predicate(pred_str)
            else:
                matched_delete_cond = is_matched  # delete all matched

        # Determine which target-only rows to delete
        target_only_delete_cond = lit(False)
        for pred_str in self._not_matched_by_source_deletes:
            if pred_str is not None:
                target_only_delete_cond = target_only_delete_cond | _resolve_predicate(pred_str)
            else:
                target_only_delete_cond = is_target_only  # delete all unmatched

        # Build keep condition (rows NOT deleted)
        delete_cond = (is_matched & matched_delete_cond) | (is_target_only & target_only_delete_cond)

        # Build output expressions for each target column
        output_exprs = []
        for col_name in target_columns:
            src_col = f"{col_name}.__src__" if col_name not in self._on else col_name
            has_src_col = col_name in source_columns

            # Start with target value as default
            expr = col(col_name)

            # Apply WHEN MATCHED UPDATE
            for updates, pred_str in self._matched_updates:
                if updates is None and self._update_all and has_src_col and col_name not in self._on:
                    # update_all: use source column
                    cond = is_matched
                    if pred_str is not None:
                        resolved_pred = _resolve_predicate(pred_str)
                        cond = cond & resolved_pred
                    expr = when(cond, col(src_col)).otherwise(expr)
                elif updates and col_name in updates:
                    cond = is_matched
                    if pred_str is not None:
                        resolved_pred = _resolve_predicate(pred_str)
                        cond = cond & resolved_pred
                    update_expr = _resolve_update_val(updates[col_name])
                    expr = when(cond, update_expr).otherwise(expr)

            # Apply WHEN NOT MATCHED BY SOURCE UPDATE
            for updates, pred_str in self._not_matched_by_source_updates:
                if updates and col_name in updates:
                    cond = is_target_only
                    if pred_str is not None:
                        resolved_pred = _resolve_predicate(pred_str)
                        cond = cond & resolved_pred
                    update_expr = _resolve_update_val(updates[col_name])
                    expr = when(cond, update_expr).otherwise(expr)

            # Apply WHEN NOT MATCHED INSERT (source-only rows)
            for updates, pred_str in self._not_matched_inserts:
                if updates is None and self._insert_all and has_src_col:
                    # insert_all: use source column
                    cond = is_source_only
                    if pred_str is not None:
                        resolved_pred = _resolve_predicate(pred_str)
                        cond = cond & resolved_pred
                    if col_name in self._on:
                        pass  # join key already unified
                    else:
                        expr = when(cond, col(src_col)).otherwise(expr)
                elif updates and col_name in updates:
                    cond = is_source_only
                    if pred_str is not None:
                        resolved_pred = _resolve_predicate(pred_str)
                        cond = cond & resolved_pred
                    insert_expr = _resolve_update_val(updates[col_name])
                    expr = when(cond, insert_expr).otherwise(expr)
                elif updates is None and self._insert_all and not has_src_col and col_name not in self._on:
                    # Column exists in target but not source — NULL for inserts
                    cond = is_source_only
                    expr = when(cond, lit(None).cast(target_schema[col_name].dtype)).otherwise(expr)

            output_exprs.append(expr.alias(col_name))

        # Add markers for metrics counting
        output_exprs.append(is_matched.alias("__matched__"))
        output_exprs.append(is_source_only.alias("__inserted__"))
        output_exprs.append(is_target_only.alias("__target_only__"))

        # Select merged result
        merged = joined.select(*output_exprs)

        # Filter out deleted rows using the post-select marker columns
        if self._matched_deletes or self._not_matched_by_source_deletes:
            post_matched = col("__matched__")
            post_target_only = col("__target_only__")

            post_matched_del = lit(False)
            for pred_str in self._matched_deletes:
                if pred_str is not None:
                    post_matched_del = post_matched_del | _resolve_predicate(pred_str)
                else:
                    post_matched_del = post_matched

            post_target_del = lit(False)
            for pred_str in self._not_matched_by_source_deletes:
                if pred_str is not None:
                    post_target_del = post_target_del | _resolve_predicate(pred_str)
                else:
                    post_target_del = post_target_only

            post_delete_cond = (post_matched & post_matched_del) | (post_target_only & post_target_del)
            merged = merged.where(~post_delete_cond)

        # Compute metrics inline during the write pass (avoid double materialization).
        # We add marker columns to the merged result, write back, and count
        # metrics from the marker columns after the write completes.
        _MODIFIED_MARKER = "__daft_dm_modified__"
        is_modified = col("__matched__") | col("__inserted__")

        # For not_matched_by_source_update, those target-only rows are also modified
        if self._not_matched_by_source_updates:
            is_modified = is_modified | col("__target_only__")

        final = merged.select(
            *[col(c) for c in target_columns],
            is_modified.alias(_MODIFIED_MARKER),
            col("__matched__"),
            col("__inserted__"),
            col("__target_only__"),
        )

        # Write back — use surgical write if possible
        from deltalake import CommitProperties, write_deltalake

        commit_properties = CommitProperties(custom_metadata={
            **(self._custom_metadata or {}),
            "daft.operation": "DISTRIBUTED_MERGE",
        })

        # Collect final into memory once — used for both metrics and writing
        final_collected = final.collect()

        # Check if table is partitioned — use partition-scoped overwrite if so
        partition_cols = self._resolved_table.metadata().partition_columns

        if partition_cols:
            # Partition-aware overwrite: only rewrite partitions that have changes
            # 1. Find affected partition values
            affected_parts = (
                final_collected.where(col(_MODIFIED_MARKER))
                .select(*[col(p) for p in partition_cols])
                .distinct()
                .to_pydict()
            )

            # 2. Build partition predicate for overwrite
            if affected_parts and affected_parts[partition_cols[0]]:
                # Build predicate like: "part_col IN ('val1', 'val2', ...)"
                predicates = []
                for pcol in partition_cols:
                    vals = affected_parts[pcol]
                    if all(isinstance(v, str) for v in vals):
                        val_list = ", ".join(f"'{v}'" for v in vals)
                    else:
                        val_list = ", ".join(str(v) for v in vals)
                    predicates.append(f"{pcol} IN ({val_list})")
                partition_predicate = " AND ".join(predicates)

                # 3. Write only rows from affected partitions
                write_df = final_collected.where(col(_MODIFIED_MARKER) | self._build_partition_filter(partition_cols, affected_parts))
                write_df = write_df.select(*[col(c) for c in target_columns])

                arrow_schema = write_df.schema().to_pyarrow_schema()
                result_iter = write_df.to_arrow_iter()

                write_deltalake(
                    self._resolved_table,
                    pa.RecordBatchReader.from_batches(arrow_schema, result_iter),
                    mode="overwrite",
                    predicate=partition_predicate,
                    storage_options=self._storage_options,
                    commit_properties=commit_properties,
                )
            else:
                # No modifications — nothing to write
                pass
        else:
            # Non-partitioned table — must do full overwrite
            write_df = final_collected.select(*[col(c) for c in target_columns])
            arrow_schema = write_df.schema().to_pyarrow_schema()
            result_iter = write_df.to_arrow_iter()

            write_deltalake(
                self._resolved_table,
                pa.RecordBatchReader.from_batches(arrow_schema, result_iter),
                mode="overwrite",
                schema_mode="overwrite",
                storage_options=self._storage_options,
                commit_properties=commit_properties,
            )

        # Compute metrics from the already-collected data (no extra pass)
        metrics_row = final_collected.select(
            col("__matched__").cast(DataType.int64()).sum().alias("num_matched"),
            col("__inserted__").cast(DataType.int64()).sum().alias("num_inserted"),
            col("__target_only__").cast(DataType.int64()).sum().alias("num_target_only"),
        ).to_pydict()
        num_matched = metrics_row["num_matched"][0] or 0
        num_inserted = metrics_row["num_inserted"][0] or 0
        num_target_only = metrics_row["num_target_only"][0] or 0

        # Return metrics as DataFrame (matches delta-rs semantics)
        raw_metrics = {
            "num_source_rows": num_matched + num_inserted,
            "num_target_rows_inserted": num_inserted,
            "num_target_rows_updated": num_matched,
            "num_target_rows_deleted": 0,
            "num_target_rows_copied": num_target_only,
            "num_output_rows": num_matched + num_inserted + num_target_only,
            "num_target_files_added": 0,
            "num_target_files_removed": 0,
            "execution_time_ms": 0,
            "scan_time_ms": 0,
            "rewrite_time_ms": 0,
        }
        return _format_merge_metrics_as_dataframe(raw_metrics)


def _resolve_expr(
    expr_str: str,
    source_alias: str,
    target_alias: str,
    on: list[str],
) -> Any:
    """Resolve a simple SQL-like expression string to a Daft expression.

    Supports:
      - "source.col_name" → col("col_name.__src__") or col("col_name") for keys
      - "target.col_name" → col("col_name")
      - "'literal'" → lit("literal")
      - "true" / "false" → lit(True) / lit(False)
      - "NULL" → lit(None)
      - "arrow_cast(...)" → passthrough as lit (for timestamp literals)
    """
    from daft import DataType, col, lit

    expr_str = expr_str.strip()

    # Boolean literals
    if expr_str.lower() == "true":
        return lit(True)
    if expr_str.lower() == "false":
        return lit(False)

    # NULL
    if expr_str.upper() == "NULL":
        return lit(None)

    # Handle arrow_cast('value', 'Timestamp(Microsecond, None)')
    if expr_str.lower().startswith("arrow_cast("):
        import re

        match = re.match(
            r"arrow_cast\(\s*'([^']+)'\s*,\s*'Timestamp\(Microsecond,\s*None\)'\s*\)",
            expr_str,
            re.IGNORECASE,
        )
        if match:
            from datetime import datetime as _dt
            from datetime import timezone as _tz

            ts_str = match.group(1)
            dt = _dt.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_tz.utc)
            us = int(dt.timestamp() * 1_000_000)
            return lit(us).cast(DataType.timestamp("us"))

    # String literal
    if expr_str.startswith("'") and expr_str.endswith("'"):
        return lit(expr_str[1:-1])

    # Source column reference
    if expr_str.startswith(f"{source_alias}."):
        col_name = expr_str[len(source_alias) + 1:]
        if col_name in on:
            return col(col_name)
        return col(f"{col_name}.__src__")

    # Target column reference
    if expr_str.startswith(f"{target_alias}."):
        col_name = expr_str[len(target_alias) + 1:]
        return col(col_name)

    # Fallback: treat as literal string
    return lit(expr_str)


def _parse_predicate(
    pred_str: str,
    source_alias: str,
    target_alias: str,
    on: list[str] | None = None,
) -> Any:
    """Parse a simple predicate string into a Daft boolean expression.

    Supports compound AND predicates, e.g.:
      "source.col != target.col AND source.id IS NOT NULL"
    Each clause supports: ``=``, ``!=``, ``IS NULL``, ``IS NOT NULL``.
    """
    import re

    from daft import col, lit

    on = on or []
    pred_str = pred_str.strip()

    # Split on AND (case-insensitive) and combine with &
    parts = [p.strip() for p in re.split(r"\s+[Aa][Nn][Dd]\s+", pred_str)]
    if len(parts) > 1:
        combined = _parse_single_predicate(parts[0], source_alias, target_alias, on)
        for part in parts[1:]:
            combined = combined & _parse_single_predicate(part, source_alias, target_alias, on)
        return combined

    return _parse_single_predicate(pred_str, source_alias, target_alias, on)


def _parse_single_predicate(
    pred_str: str,
    source_alias: str,
    target_alias: str,
    on: list[str] | None = None,
) -> Any:
    """Parse a single predicate clause (no AND) into a Daft boolean expression."""
    from daft import col, lit

    on = on or []
    pred_str = pred_str.strip()

    # Handle "col1 != col2"
    if " != " in pred_str:
        left, right = pred_str.split(" != ", 1)
        left_expr = _resolve_expr(left.strip(), source_alias, target_alias, on)
        right_expr = _resolve_expr(right.strip(), source_alias, target_alias, on)
        return left_expr != right_expr

    # Handle "col1 = col2" (equality)
    if " = " in pred_str and " != " not in pred_str and "IS" not in pred_str.upper():
        left, right = pred_str.split(" = ", 1)
        left_expr = _resolve_expr(left.strip(), source_alias, target_alias, on)
        right_expr = _resolve_expr(right.strip(), source_alias, target_alias, on)
        return left_expr == right_expr

    # Handle "col IS NOT NULL"
    if " IS NOT NULL" in pred_str.upper():
        col_expr = _resolve_expr(pred_str.split(" IS ")[0].strip(), source_alias, target_alias, on)
        return col_expr.not_null()

    # Handle "col IS NULL"
    if " IS NULL" in pred_str.upper():
        col_expr = _resolve_expr(pred_str.split(" IS ")[0].strip(), source_alias, target_alias, on)
        return col_expr.is_null()

    # Fallback: always true
    return lit(True)


def delta_schema_to_pyarrow(schema: "deltalake.Schema") -> "pa.Schema":
    import deltalake
    from packaging.version import parse

    if parse(deltalake.__version__) < parse("1.0.0"):
        return schema.to_pyarrow()
    else:
        import pyarrow as pa

        return pa.schema(schema.to_arrow())
