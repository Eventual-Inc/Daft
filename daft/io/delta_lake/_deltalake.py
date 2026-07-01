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

    Returns a merge builder that mirrors the underlying ``deltalake`` merge API.
    Call ``.execute()`` on the builder to perform the merge and return a DataFrame with operation metrics.

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
    from deltalake import CommitProperties

    if isinstance(source, DataFrame):
        import pyarrow as pa

        if streamed_exec:
            arrow_schema = source.schema().to_pyarrow_schema()
            # Use a small buffer (2) to avoid accumulating many partitions on the
            # driver while DataFusion slowly consumes them for the merge join.
            source_data = pa.RecordBatchReader.from_batches(
                arrow_schema, source.to_arrow_iter(results_buffer_size=2)
            )
        else:
            source.collect()
            arrow_schema = source.schema().to_pyarrow_schema()
            source_data = pa.RecordBatchReader.from_batches(arrow_schema, source.to_arrow_iter())
    else:
        source_data = source

    resolved_table, _ = _resolve_deltalake_table_and_storage_options(table, io_config)
    commit_properties = CommitProperties(custom_metadata=custom_metadata)

    # Apply defaults for spill configuration using Daft's execution config.
    import shutil

    exec_config = context.get_context().daft_execution_config
    try:
        shuffle_dirs = exec_config.flight_shuffle_dirs
    except AttributeError:
        shuffle_dirs = []

    # Fallback to DAFT_FLIGHT_SHUFFLE_DIR env var if config doesn't have dirs.
    if not shuffle_dirs:
        env_dir = os.environ.get("DAFT_FLIGHT_SHUFFLE_DIR")
        if env_dir:
            shuffle_dirs = [env_dir]

    # Point DataFusion's temp dir at the flight shuffle directory for spilling.
    if shuffle_dirs:
        os.environ.setdefault("TMPDIR", shuffle_dirs[0])

    # Default: 70% of total system memory.
    try:
        import resource
        total_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError):
        total_mem = 10 * 1024 * 1024 * 1024  # fallback 10 GB
    max_spill_size = int(total_mem * 0.7)

    # Default: 70% of available disk on the spill directory.
    spill_path = shuffle_dirs[0] if shuffle_dirs else "/tmp"
    try:
        disk_usage = shutil.disk_usage(spill_path)
        max_temp_directory_size = int(disk_usage.free * 0.7)
    except OSError:
        max_temp_directory_size = 100 * 1024 * 1024 * 1024  # fallback 100 GB
    print(f"Delta Lake merge spill configuration: max_spill_size={max_spill_size}, max_temp_directory_size={max_temp_directory_size}")
    
    # Create the merge builder
    merger = resolved_table.merge(
        source=source_data,
        predicate=predicate,
        source_alias=source_alias,
        target_alias=target_alias,
        merge_schema=merge_schema,
        error_on_type_mismatch=not safe_cast,
        writer_properties=writer_properties,
        streamed_exec=streamed_exec,
        max_spill_size=max_spill_size,
        max_temp_directory_size=max_temp_directory_size,
        commit_properties=commit_properties,
        post_commithook_properties=post_commithook_properties,
    )

    return DeltaMergeBuilder(merger)


class DeltaMergeBuilder:
    """Wrapper around deltalake.TableMerger that returns merge results as a DataFrame."""

    def __init__(self, merger: "deltalake.TableMerger") -> None:
        self._merger = merger

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
        self._merger = self._merger.when_matched_update(dict(updates), predicate)
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
        self._merger = self._merger.when_matched_update_all(predicate, except_cols)
        return self

    def when_matched_delete(self, predicate: str | None = None) -> "DeltaMergeBuilder":
        """Add a ``when_matched_delete`` clause to the merge."""
        self._merger = self._merger.when_matched_delete(predicate)
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
        self._merger = self._merger.when_not_matched_insert(dict(updates), predicate)
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
        self._merger = self._merger.when_not_matched_insert_all(predicate, except_cols)
        return self

    def when_not_matched_by_source_update(
        self,
        updates: "Mapping[str, str]",
        predicate: str | None = None,
    ) -> "DeltaMergeBuilder":
        """Add a ``when_not_matched_by_source_update`` clause to the merge.

        Args:
            updates: Mapping from column name to SQL update expression.
            predicate: Optional SQL predicate for rows not matched by source.

        Returns:
            Self for method chaining.
        """
        self._merger = self._merger.when_not_matched_by_source_update(dict(updates), predicate)
        return self

    def when_not_matched_by_source_delete(self, predicate: str | None = None) -> "DeltaMergeBuilder":
        """Add a ``when_not_matched_by_source_delete`` clause to the merge.

        Args:
            predicate: Optional SQL predicate for rows not matched by source.

        Returns:
            Self for method chaining.
        """
        self._merger = self._merger.when_not_matched_by_source_delete(predicate)
        return self

    def execute(self) -> "DataFrame":
        """Execute the merge operation and return a DataFrame with metrics in metadata.

        Returns a single-row DataFrame containing all merge metrics as columns.
        The raw metrics dictionary is also stored in the DataFrame's _metadata.

        Returns:
            DataFrame: A single-row DataFrame with columns for each merge metric.
        """
        import pyarrow as pa

        raw_metrics = self._merger.execute()
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




def delta_schema_to_pyarrow(schema: "deltalake.Schema") -> "pa.Schema":
    import deltalake
    from packaging.version import parse

    if parse(deltalake.__version__) < parse("1.0.0"):
        return schema.to_pyarrow()
    else:
        import pyarrow as pa

        return pa.schema(schema.to_arrow())
