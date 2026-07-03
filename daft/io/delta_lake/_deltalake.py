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
        This runs a single atomic delta-rs merge: the source is streamed through
        one process and committed once, and only the files touched by the merge
        are rewritten. All clause types are supported, including
        ``when_not_matched_by_source_update`` / ``when_not_matched_by_source_delete``.
        For a source that is very large relative to the target, see
        :func:`distributed_merge_deltalake`, which distributes the join across workers.

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
    # setdefault: never clobber a TMPDIR the caller already configured.
    if shuffle_dirs:
        os.environ.setdefault("TMPDIR", shuffle_dirs[0])

    # Default max_spill_size to 70% of total system memory when not supplied.
    if max_spill_size is None:
        try:
            total_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        except (AttributeError, ValueError):
            total_mem = 10 * 1024 * 1024 * 1024  # fallback 10 GB
        max_spill_size = int(total_mem * 0.7)

    # Default max_temp_directory_size to 70% of free disk on the spill dir when not supplied.
    if max_temp_directory_size is None:
        spill_path = shuffle_dirs[0] if shuffle_dirs else "/tmp"
        try:
            max_temp_directory_size = int(shutil.disk_usage(spill_path).free * 0.7)
        except OSError:
            max_temp_directory_size = 100 * 1024 * 1024 * 1024  # fallback 100 GB

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
        self._executed = False

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
        if self._executed:
            raise RuntimeError(
                "This merge has already been executed. Build a new merge_deltalake(...) to run it again."
            )
        self._executed = True

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
    validate_unique_keys: bool = True,
    broadcast_join: bool | None = None,
    materialize_source: bool = True,
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
            Only equality (equi-join) conditions of the form
            ``target.col = source.col`` are supported; the join keys are
            extracted from them. Non-equality ("residual") conditions raise a
            ``ValueError`` — pre-filter or pre-join the source instead.
        on: Explicit join key column name(s). If ``None``, keys are parsed from
            the ``predicate``. Providing ``on`` overrides predicate-derived keys.
        io_config: Optional :class:`~daft.daft.IOConfig` for object storage access.
        source_alias: Alias for the source side (used in update expressions).
        target_alias: Alias for the target side (used in update expressions).
        custom_metadata: Optional metadata to attach to the Delta commit.
        validate_unique_keys: If ``True`` (default), verify the source has unique
            join keys before merging and raise ``ValueError`` on duplicates. Set
            to ``False`` to skip the check when uniqueness is guaranteed upstream.
        broadcast_join: Join strategy hint. ``True`` decomposes the full outer
            join into a broadcast-friendly LEFT join plus a keys-only ANTI join
            (avoids shuffling the target when the source is small); ``False``
            uses a plain full outer join; ``None`` (default) decomposes only on
            the Ray runner **and** only when the materialized source is small
            enough to broadcast safely. Never pass ``True`` for a large source —
            it is copied to every worker.
        materialize_source: If ``True`` (default), collect the source once and
            reuse it across the guard and both execution passes. Set to
            ``False`` for very large sources (e.g. 100M+ rows): the source plan
            is then re-executed per pass (with column pruning) instead of being
            pinned in cluster memory.

    Returns:
        DistributedDeltaMergeBuilder: A builder for chaining merge clauses.

    Note:
        The join is distributed across all workers and the result is written
        back via Daft's native Delta writer, so data files are produced on
        workers and never funneled through the driver. Execution is two-pass
        streaming: a narrow statistics pass (metrics + affected partitions)
        followed by a write pass — the joined table is never materialized in
        memory. Best for cases where the source is large relative to the
        target, or when single-node merge causes OOM.

        Writes are **incremental on partitioned tables**: only partitions
        containing an insert, update, or delete are rewritten (including the
        pre-image partition when an update moves a row across partitions), and
        a merge that modifies nothing skips the commit entirely. Unpartitioned
        tables are rewritten in full.

        The source must have **unique join keys** (multiple source rows matching
        one target row is rejected with a ``ValueError`` unless
        ``validate_unique_keys=False``).

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

    # Non-equality ("residual") conditions cannot be honored by the join-based
    # merge: a key-joined row that fails such a condition would need to be split
    # into a preserved target row *and* a separately-inserted source row, which a
    # single joined row cannot represent. Fail fast rather than corrupt data.
    if residual_predicates:
        raise ValueError(
            "distributed_merge_deltalake only supports equality (equi-join) predicate conditions "
            f"of the form '{target_alias}.col = {source_alias}.col'. Unsupported residual "
            f"(non-equality) condition(s): {residual_predicates}. Pre-filter or pre-join the source "
            "to express these instead."
        )

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
        validate_unique_keys=validate_unique_keys,
        broadcast_join=broadcast_join,
        materialize_source=materialize_source,
    )


def _split_sql_quotes(sql: str) -> "list[tuple[str, bool]]":
    """Split sql into (segment, is_quoted_literal) pieces, quote-aware."""
    parts: list[tuple[str, bool]] = []
    buf: list[str] = []
    in_quote = False
    for ch in sql:
        if ch == "'":
            if in_quote:
                buf.append(ch)
                parts.append(("".join(buf), True))
                buf = []
                in_quote = False
                continue
            if buf:
                parts.append(("".join(buf), False))
            buf = [ch]
            in_quote = True
            continue
        buf.append(ch)
    if buf:
        parts.append(("".join(buf), in_quote))
    return parts


def _translate_arrow_cast(sql: str) -> str:
    """Rewrite DataFusion-style arrow_cast('v', 'Timestamp(...)') to SQL CAST."""
    import re

    return re.sub(
        r"arrow_cast\(\s*('[^']*')\s*,\s*'Timestamp\([^)]*\)'\s*\)",
        r"CAST(\1 AS TIMESTAMP)",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_merge_sql(
    sql: str,
    source_alias: str,
    target_alias: str,
    on: "list[str]",
    suffixed_cols: "set[str] | None",
) -> str:
    """Rewrite alias-qualified refs to the joined frame's actual column names.

    ``source.x`` -> ``"x.__src__"`` when x collides with a target column,
    else ``"x"``; ``target.x`` -> ``"x"``. Quoted string literals are left
    untouched; alias matching is case-insensitive (SQL identifier rules).
    """
    import re

    # NOTE: Daft's SQL parser resolves an unquoted dotted name (``x.__src__``)
    # as a single flat column name, while double-quoted identifiers keep the
    # quotes as part of the name — so bare names are emitted here.
    def _src_name(name: str) -> str:
        if name in on:
            return name
        if suffixed_cols is None or name in suffixed_cols:
            return f"{name}.__src__"
        return name

    src_pat = re.compile(rf"\b{re.escape(source_alias)}\.(\w+)", re.IGNORECASE)
    tgt_pat = re.compile(rf"\b{re.escape(target_alias)}\.(\w+)", re.IGNORECASE)

    out = []
    for segment, quoted in _split_sql_quotes(sql):
        if quoted:
            out.append(segment)
            continue
        segment = src_pat.sub(lambda m: _src_name(m.group(1)), segment)
        segment = tgt_pat.sub(lambda m: m.group(1), segment)
        out.append(segment)
    return "".join(out)


def _parse_merge_predicate(
    predicate: str,
    source_alias: str,
    target_alias: str,
) -> tuple[list[str], list[str]]:
    """Parse a merge predicate into join keys and residual conditions.

    Extracts equi-join keys from "target.col = source.col" patterns
    (alias matching is case-insensitive, splitting is quote-aware).
    Returns remaining conditions as residual predicates.

    Returns:
        (join_keys, residual_predicates): list of column names for join,
        and list of unparsed predicate fragments.
    """
    import re

    join_keys: list[str] = []
    residual_predicates: list[str] = []

    # Replace quoted literals with placeholders so AND-splitting is quote-safe.
    literals: list[str] = []
    masked_parts: list[str] = []
    for seg, quoted in _split_sql_quotes(predicate):
        if quoted:
            masked_parts.append(f"\x00{len(literals)}\x00")
            literals.append(seg)
        else:
            masked_parts.append(seg)
    masked = "".join(masked_parts)

    def _unmask(s: str) -> str:
        for i, lit_str in enumerate(literals):
            s = s.replace(f"\x00{i}\x00", lit_str)
        return s

    eq_re = re.compile(
        rf"^\s*(?:(?P<t1>{re.escape(target_alias)})|(?P<s1>{re.escape(source_alias)}))\.(?P<c1>\w+)\s*=\s*"
        rf"(?:(?P<t2>{re.escape(target_alias)})|(?P<s2>{re.escape(source_alias)}))\.(?P<c2>\w+)\s*$",
        re.IGNORECASE,
    )

    for part in re.split(r"\s+[Aa][Nn][Dd]\s+", masked):
        part = part.strip()
        if not part:
            continue
        m = eq_re.match(part)
        if m and m.group("c1") == m.group("c2"):
            one_target = bool(m.group("t1")) != bool(m.group("t2"))
            one_source = bool(m.group("s1")) != bool(m.group("s2"))
            if one_target and one_source:
                join_keys.append(m.group("c1"))
                continue
        residual_predicates.append(_unmask(part))

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
        validate_unique_keys: bool = True,
        broadcast_join: bool | None = None,
        materialize_source: bool = True,
    ) -> None:
        self._resolved_table = resolved_table
        self._storage_options = storage_options
        self._source = source
        self._on = on
        self._io_config = io_config
        self._source_alias = source_alias
        self._target_alias = target_alias
        self._custom_metadata = custom_metadata
        self._validate_unique_keys = validate_unique_keys
        self._broadcast_join = broadcast_join
        self._materialize_source = materialize_source

        # Target column names, used to decide which source columns collide (and
        # therefore receive the ".__src__" join suffix). Read from the Delta log
        # schema — no data scan.
        self._target_columns = list(delta_schema_to_pyarrow(resolved_table.schema()).names)

        # Globally-ordered WHEN clauses: (kind, updates-or-None, predicate).
        # SQL MERGE fires the FIRST matching clause per row category, so the
        # declaration order across kinds (update vs delete) must be preserved.
        self._clauses: list[tuple[str, Mapping[str, Any] | None, Any]] = []

    def source_col(self, name: str) -> "Expression":
        """Reference a source column in the joined result.

        After the outer join, only source columns whose name *collides* with a
        target column are suffixed with ``.__src__``; join keys are unified and
        source-only columns keep their bare name. This helper abstracts that away.

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
        # Suffixed only when the name also exists on the target side (collision).
        if name in self._target_columns:
            return col(f"{name}.__src__")
        return col(name)

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
        self._clauses.append(("matched_update", dict(updates), predicate))
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
        self._clauses.append(("matched_update", None, predicate))
        return self

    def when_matched_delete(self, predicate: "_Pred" = None) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN MATCHED THEN DELETE clause."""
        self._clauses.append(("matched_delete", None, predicate))
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
        self._clauses.append(("insert", dict(updates), predicate))
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
        self._clauses.append(("insert", None, predicate))
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
        self._clauses.append(("by_source_update", dict(updates), predicate))
        return self

    def when_not_matched_by_source_delete(self, predicate: "_Pred" = None) -> "DistributedDeltaMergeBuilder":
        """Add a WHEN NOT MATCHED BY SOURCE THEN DELETE clause."""
        self._clauses.append(("by_source_delete", None, predicate))
        return self

    def _decomposed_outer_join(
        self,
        target_tagged: DataFrame,
        source_tagged: DataFrame,
    ) -> DataFrame:
        """Full outer join decomposed into broadcast-friendly LEFT + ANTI joins.

        A full outer join cannot use a broadcast strategy, so when the source is
        small this decomposition avoids shuffling the (large) target:

          - ``target LEFT JOIN broadcast(source)`` → matched + target-only rows
          - ``source ANTI JOIN target-keys``       → source-only rows

        The union of the two is exactly the full outer join, with an identical
        schema. On runners without broadcast support (native), the strategy hint
        falls back to a hash join and the result is still correct.
        """
        from daft import col, lit

        left = target_tagged.join(
            source_tagged, on=self._on, how="left", strategy="broadcast", suffix=".__src__"
        )
        target_keys = target_tagged.select(*[col(k) for k in self._on])
        source_only = source_tagged.join(target_keys, on=self._on, how="anti")

        # Align source_only to the left-join schema (names, dtypes, order).
        source_names = {f.name for f in source_tagged.schema()}
        target_names = set(self._target_columns)
        aligned = []
        for f in left.schema():
            name = f.name
            if name.endswith(".__src__"):
                # Collision column: source side value.
                aligned.append(col(name[: -len(".__src__")]).cast(f.dtype).alias(name))
            elif name in self._on or (name in source_names and name not in target_names):
                # Join keys, source-only columns, and the source marker.
                aligned.append(col(name).cast(f.dtype).alias(name))
            else:
                # Target-only columns and the target marker: NULL of the target dtype.
                aligned.append(lit(None).cast(f.dtype).alias(name))
        return left.concat(source_only.select(*aligned))

    def _write_partition_scoped(
        self,
        write_df: DataFrame,
        partition_cols: list[str],
        affected: "set[tuple]",
        pinned_version: int,
        merge_metadata: dict[str, str],
    ) -> None:
        """Distributed copy-on-write commit scoped to the affected partitions.

        Data files are written by workers via Daft's native Delta writer; the
        single atomic commit removes only files in the affected partitions
        (``partitions_filters``), leaving all other partitions untouched.
        """
        from deltalake import CommitProperties

        io_config = (
            self._io_config
            if self._io_config is not None
            else context.get_context().daft_planning_config.default_io_config
        )
        builder = write_df._builder.write_deltalake(
            self._resolved_table.table_uri,
            "overwrite",
            pinned_version + 1,
            True,
            io_config=io_config,
            partition_cols=list(partition_cols),
        )
        wdf = DataFrame(builder)
        wdf.collect()  # workers write data files; only add-action metadata is collected
        add_actions = wdf.to_pydict().get("add_action", [])

        # delta-rs accepts only a single conjunction filter; the caller has
        # already expanded `affected` to its per-column cartesian closure, so
        # per-column filters cover exactly the same partitions the write pass
        # rewrote ("=" for a single value keeps the filter minimal).
        filters = []
        for i, pc in enumerate(partition_cols):
            values = sorted({_delta_partition_value_str(tup[i]) for tup in affected})
            filters.append((pc, "=", values[0]) if len(values) == 1 else (pc, "in", values))
        self._resolved_table._table.create_write_transaction(
            add_actions,
            "overwrite",
            list(partition_cols),
            self._resolved_table.schema(),
            filters,
            CommitProperties(custom_metadata=merge_metadata),
        )
        self._resolved_table.update_incremental()

    def execute(self) -> DataFrame:
        """Execute the distributed merge and return a DataFrame with metrics.

        Two-pass streaming execution over a version-pinned target snapshot:

        1. **Stats pass** — the (lazy) joined+merged plan is aggregated per
           partition to compute merge metrics and the set of *affected*
           partitions (any partition containing an insert, update, or delete —
           including the pre-image partition of rows that migrate). Only tiny
           aggregates reach the driver.
        2. **Write pass** — the same plan is re-executed, filtered to affected
           partitions, and written distributed via Daft's native Delta writer
           with a partition-scoped atomic commit. Untouched partitions' files
           are preserved as-is. Unpartitioned tables fall back to a streaming
           full overwrite, and a merge that modifies nothing skips the commit.

        The joined table is never materialized in memory.

        Returns:
            DataFrame: A single-row DataFrame with merge metrics columns.
                       Raw metrics dict also stored in ``._metadata["merge_metrics"]``.
        """
        import daft
        from daft import DataType, col, lit
        from daft.expressions.expressions import Expression
        from daft.functions import when

        on = self._on

        # Materialize the source once when small enough to hold: it is reused by
        # the guard and by both execution passes. For very large sources
        # (materialize_source=False), keep it lazy — each pass re-executes the
        # source plan (column-pruned) instead of pinning it in cluster memory.
        source_size_bytes: int | None = None
        if self._materialize_source:
            source = self._source.collect()
            try:
                source_size_bytes = source._result.size_bytes() if source._result is not None else None
            except Exception:
                source_size_bytes = None
        else:
            source = self._source
        source_columns = [f.name for f in source.schema()]

        # Guard (#11): source join keys must be unique — multiple source rows
        # matching one target row is ambiguous and would duplicate target rows
        # through the join. Single-pass groupby; skippable when uniqueness is
        # guaranteed upstream.
        if self._validate_unique_keys:
            dup = (
                source.groupby(*on)
                .agg(col(on[0]).count().alias("__daft_dm_dup_cnt__"))
                .where(col("__daft_dm_dup_cnt__") > 1)
                .limit(1)
                .to_pydict()
            )
            if dup["__daft_dm_dup_cnt__"]:
                dup_key = {k: dup[k][0] for k in on}
                raise ValueError(
                    f"Source contains duplicate join key(s) {on}, e.g. {dup_key}. "
                    "distributed_merge_deltalake requires unique join keys in the source "
                    "(multiple source rows cannot match one target row). Pass "
                    "validate_unique_keys=False to skip this check."
                )

        # Pin the target version so the stats pass and the write pass read the
        # same snapshot even if the table is written to concurrently.
        pinned_version = self._resolved_table.version()
        target = daft.read_deltalake(
            self._resolved_table.table_uri, version=pinned_version, io_config=self._io_config
        )
        target_schema = target.schema()
        target_columns = [f.name for f in target_schema]

        # A source column gets the ".__src__" join suffix iff its name also
        # exists on the target side (a collision) and is not a join key.
        suffixed = (set(target_columns) & set(source_columns)) - set(on)

        def _resolve_predicate(pred: Any) -> Any:
            # SQL WHEN-clause semantics: a predicate evaluating to NULL is
            # not-satisfied. Without fill_null, Kleene NULL would propagate
            # through deleted/emitted and the write-pass filter would drop
            # rows whose delete condition was merely unknown.
            if pred is None:
                return None
            if isinstance(pred, Expression):
                return pred.fill_null(lit(False))
            return _parse_predicate(pred, self._source_alias, self._target_alias, on, suffixed).fill_null(
                lit(False)
            )

        def _resolve_update_val(val: Any) -> Any:
            if isinstance(val, Expression):
                return val
            return _resolve_expr(val, self._source_alias, self._target_alias, on, suffixed)

        def _src_ref(name: str) -> Any:
            if name in on:
                return col(name)
            if name in suffixed:
                return col(f"{name}.__src__")
            return col(name)

        # --- Distributed FULL OUTER JOIN ---------------------------------------
        _SRC_MARKER = "__daft_dm_src__"
        _TGT_MARKER = "__daft_dm_tgt__"

        source_tagged = source.with_column(_SRC_MARKER, lit(True))
        target_tagged = target.with_column(_TGT_MARKER, lit(True))

        decompose = _should_decompose_join(
            self._broadcast_join,
            runners.get_or_create_runner().name,
            source_size_bytes,
        )

        if decompose:
            joined = self._decomposed_outer_join(target_tagged, source_tagged)
        else:
            joined = target_tagged.join(source_tagged, on=on, how="outer", suffix=".__src__")

        # Row categories (mutually exclusive).
        is_matched = col(_TGT_MARKER).not_null() & col(_SRC_MARKER).not_null()
        is_source_only = col(_TGT_MARKER).is_null() & col(_SRC_MARKER).not_null()
        is_target_only = col(_TGT_MARKER).not_null() & col(_SRC_MARKER).is_null()

        # --- First-match-wins clause index per row category ---------------------
        # SQL MERGE fires the FIRST clause (in declaration order) whose
        # predicate holds, per row — across kinds, so an UPDATE declared before
        # a DELETE shields matching rows from the delete.
        matched_clauses = [
            (i, kind, updates, pred)
            for i, (kind, updates, pred) in enumerate(self._clauses)
            if kind in ("matched_update", "matched_delete")
        ]
        by_source_clauses = [
            (i, kind, updates, pred)
            for i, (kind, updates, pred) in enumerate(self._clauses)
            if kind in ("by_source_update", "by_source_delete")
        ]
        insert_clauses = [
            (i, kind, updates, pred)
            for i, (kind, updates, pred) in enumerate(self._clauses)
            if kind == "insert"
        ]

        def _first_match_idx(clauses: list, base_cond: Any) -> Any:
            """Global index of the first clause whose predicate holds, else -1."""
            expr = lit(-1)
            for i, _kind, _updates, pred in reversed(clauses):
                cond = base_cond if pred is None else (base_cond & _resolve_predicate(pred))
                expr = when(cond, lit(i)).otherwise(expr)
            return expr

        matched_idx = _first_match_idx(matched_clauses, is_matched)
        by_source_idx = _first_match_idx(by_source_clauses, is_target_only)
        insert_idx = _first_match_idx(insert_clauses, is_source_only)

        def _kind_fires(idx_expr: Any, clauses: list, kinds: "tuple[str, ...]") -> Any:
            fired = lit(False)
            for i, kind, _updates, _pred in clauses:
                if kind in kinds:
                    fired = fired | (idx_expr == lit(i))
            return fired

        deleted = _kind_fires(matched_idx, matched_clauses, ("matched_delete",)) | _kind_fires(
            by_source_idx, by_source_clauses, ("by_source_delete",)
        )
        updated = _kind_fires(matched_idx, matched_clauses, ("matched_update",)) | _kind_fires(
            by_source_idx, by_source_clauses, ("by_source_update",)
        )
        inserted = insert_idx != lit(-1)
        # Source-only rows that no insert clause emits are dropped, not written
        # back as all-NULL rows (#4).
        dropped_source = is_source_only & (insert_idx == lit(-1))
        emitted = ~deleted & ~dropped_source
        copied = emitted & ~inserted & ~updated

        # --- Output value per target column (first-match-wins per row) ---------
        def _clause_value(updates: "Mapping[str, Any] | None", col_name: str) -> Any:
            """Value a clause assigns to col_name, or None if it leaves it alone."""
            if updates is None:  # update_all / insert_all
                return _src_ref(col_name) if col_name in suffixed else None
            if col_name in updates:
                return _resolve_update_val(updates[col_name])
            return None

        output_exprs = []
        for col_name in target_columns:
            dtype = target_schema[col_name].dtype
            default = col(col_name)  # target value (NULL on the source-only side)
            cases: list = []

            for i, kind, updates, _pred in matched_clauses + by_source_clauses:
                if kind not in ("matched_update", "by_source_update"):
                    continue
                idx_expr = matched_idx if kind == "matched_update" else by_source_idx
                val = _clause_value(updates, col_name)
                if val is not None:
                    cases.append((idx_expr == lit(i), val))

            for i, _kind, updates, _pred in insert_clauses:
                val = _clause_value(updates, col_name)
                if val is not None:
                    cases.append((insert_idx == lit(i), val))

            if cases:
                branch = when(cases[0][0], cases[0][1])
                for cond, val in cases[1:]:
                    branch = branch.when(cond, val)
                expr = branch.otherwise(default)
            else:
                expr = default

            output_exprs.append(expr.cast(dtype).alias(col_name))

        # --- Annotated plan (lazy — never materialized) -------------------------
        partition_cols = list(self._resolved_table.metadata().partition_columns)
        _EMIT = "__daft_dm_emit__"
        # Pre-image partition values (raw target-side values, before any update
        # rewrites them) — needed so an update that moves a row across
        # partitions also rewrites the partition it left.
        pre_alias = {pc: f"__daft_dm_pre_{i}__" for i, pc in enumerate(partition_cols)}

        annotated = joined.select(
            *output_exprs,
            emitted.alias(_EMIT),
            inserted.alias("__ins__"),
            updated.alias("__upd__"),
            deleted.alias("__del__"),
            copied.alias("__cop__"),
            is_matched.alias("__m__"),
            is_source_only.alias("__so__"),
            *[col(pc).alias(pre_alias[pc]) for pc in partition_cols],
        )

        # --- Pass 1: narrow stats (metrics + affected partitions) ---------------
        _i64 = DataType.int64()
        agg_exprs = [
            col("__ins__").cast(_i64).sum().alias("inserted"),
            col("__upd__").cast(_i64).sum().alias("updated"),
            col("__del__").cast(_i64).sum().alias("deleted"),
            col("__cop__").cast(_i64).sum().alias("copied"),
            col("__m__").cast(_i64).sum().alias("matched"),
            col("__so__").cast(_i64).sum().alias("source_only"),
            col(_EMIT).cast(_i64).sum().alias("emitted"),
        ]

        affected: "set[tuple] | None" = None
        if partition_cols:
            stats = (
                annotated.with_column("__tp__", ~col("__so__"))
                .groupby(*[pre_alias[pc] for pc in partition_cols], *partition_cols, "__tp__")
                .agg(*agg_exprs)
                .to_pydict()
            )
            groups = range(len(stats["inserted"]))
            num_inserted = sum(stats["inserted"])
            num_updated = sum(stats["updated"])
            num_deleted = sum(stats["deleted"])
            num_matched = sum(stats["matched"])
            num_source_only = sum(stats["source_only"])

            # Affected partition TUPLES: post-image of every modified row, plus
            # the pre-image when the row existed in the target. Tracking whole
            # tuples (not per-column value sets) scopes the rewrite to exactly
            # the touched partitions instead of their cartesian product.
            affected = set()
            for g in groups:
                if stats["inserted"][g] + stats["updated"][g] + stats["deleted"][g] > 0:
                    affected.add(tuple(stats[pc][g] for pc in partition_cols))
                    if stats["__tp__"][g]:
                        affected.add(tuple(stats[pre_alias[pc]][g] for pc in partition_cols))

            if not _partition_values_filterable(affected):
                # NULL or exotic partition values — fall back to full overwrite.
                affected = None

            if affected is not None:
                # delta-rs's commit API accepts only a single conjunction
                # filter, so a non-product tuple set must be expanded to its
                # per-column cartesian closure. Write filter, removal filter,
                # and copied metrics all use the same closure — exact for
                # single-column and product-shaped sets.
                import itertools

                col_values = [
                    sorted({tup[i] for tup in affected}, key=repr) for i in range(len(partition_cols))
                ]
                affected = set(itertools.product(*col_values))

            if affected is not None:
                # Copied/output metrics follow delta-rs semantics: they count
                # only rows in rewritten (affected) partitions.
                num_copied = 0
                num_output = 0
                for g in groups:
                    if tuple(stats[pc][g] for pc in partition_cols) in affected:
                        num_copied += stats["copied"][g]
                        num_output += stats["emitted"][g]
            else:
                num_copied = sum(stats["copied"])
                num_output = sum(stats["emitted"])
        else:
            stats = annotated.agg(*agg_exprs).to_pydict()
            num_inserted = stats["inserted"][0] or 0
            num_updated = stats["updated"][0] or 0
            num_deleted = stats["deleted"][0] or 0
            num_matched = stats["matched"][0] or 0
            num_source_only = stats["source_only"][0] or 0
            num_copied = stats["copied"][0] or 0
            num_output = stats["emitted"][0] or 0

        # --- Pass 2: write ------------------------------------------------------
        num_modified = num_inserted + num_updated + num_deleted
        if num_modified == 0:
            # Nothing changed — skip the write and the commit entirely.
            num_copied = 0
            num_output = 0
        else:
            merge_metadata = {**(self._custom_metadata or {}), "daft.operation": "DISTRIBUTED_MERGE"}
            if partition_cols and affected is not None:
                # Partition-scoped copy-on-write: rewrite only affected partitions.
                # OR-of-AND equality per tuple (not per-column is_in) so exactly
                # the touched partitions are rewritten.
                part_filter = lit(False)
                for tup in affected:
                    tup_match = lit(True)
                    for pc, v in zip(partition_cols, tup):
                        tup_match = tup_match & (col(pc) == lit(v))
                    part_filter = part_filter | tup_match
                write_df = annotated.where(col(_EMIT) & part_filter).select(*[col(c) for c in target_columns])
                self._write_partition_scoped(
                    write_df, partition_cols, affected, pinned_version, merge_metadata
                )
            else:
                # Unpartitioned (or unfilterable partition values): streaming
                # full overwrite via the native distributed writer.
                write_df = annotated.where(col(_EMIT)).select(*[col(c) for c in target_columns])
                write_df.write_deltalake(
                    self._resolved_table.table_uri,
                    mode="overwrite",
                    schema_mode="overwrite",
                    partition_cols=partition_cols if partition_cols else None,
                    custom_metadata=merge_metadata,
                    io_config=self._io_config,
                )

        raw_metrics = {
            "num_source_rows": num_matched + num_source_only,
            "num_target_rows_inserted": num_inserted,
            "num_target_rows_updated": num_updated,
            "num_target_rows_deleted": num_deleted,
            "num_target_rows_copied": num_copied,
            "num_output_rows": num_output,
            "num_target_files_added": 0,
            "num_target_files_removed": 0,
            "execution_time_ms": 0,
            "scan_time_ms": 0,
            "rewrite_time_ms": 0,
        }
        return _format_merge_metrics_as_dataframe(raw_metrics)


# Auto-decompose (broadcast) only for sources at or below this size: broadcast
# copies the source to every worker, so large sources must never take this path.
_BROADCAST_SOURCE_MAX_BYTES = 512 * 1024 * 1024


def _should_decompose_join(
    broadcast_join: "bool | None",
    runner_name: str,
    source_size_bytes: "int | None",
) -> bool:
    """Decide whether to decompose the outer join into broadcast LEFT + ANTI joins.

    Explicit ``broadcast_join`` always wins. Otherwise decompose only on the Ray
    runner (broadcast is unsupported elsewhere) and only when the source is
    *known* to be small enough to broadcast; unknown size is treated as unsafe.
    """
    if broadcast_join is not None:
        return broadcast_join
    if runner_name != "ray":
        return False
    return source_size_bytes is not None and source_size_bytes <= _BROADCAST_SOURCE_MAX_BYTES


def _delta_partition_value_str(value: Any) -> str:
    """Delta-log string form of a partition value, for DNF partition filters."""
    import datetime as _dt

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, _dt.datetime):  # before date: datetime is a date subclass
        # The delta log stores sub-second precision when present
        # ('2024-01-01 12:00:00.123456') and whole seconds otherwise.
        if value.microsecond:
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value)


def _partition_values_filterable(affected: "set[tuple]") -> bool:
    """Whether every affected partition value can be expressed as a DNF filter."""
    import datetime as _dt

    for tup in affected:
        for v in tup:
            if v is None:
                return False
            if not isinstance(v, (str, int, bool, _dt.date)):
                return False
    return True


def _resolve_expr(
    expr_str: str,
    source_alias: str,
    target_alias: str,
    on: list[str],
    suffixed_cols: "set[str] | None" = None,
) -> Any:
    """Resolve a SQL expression string to a Daft expression via ``daft.sql_expr``.

    Alias-qualified references (``source.x`` / ``target.x``) are first rewritten
    to the joined frame's actual column names (see :func:`_rewrite_merge_sql`),
    then the whole string is parsed by Daft's native SQL parser, so the full
    SQL expression grammar is supported (comparisons, arithmetic, CASE, IN,
    LIKE, IS [NOT] NULL, ...). DataFusion-style ``arrow_cast('v', 'Timestamp(...)')``
    literals are translated to ``CAST('v' AS TIMESTAMP)``.

    Raises:
        ValueError: if the expression cannot be parsed. Unparseable input must
            fail loudly — guessing (e.g. treating it as a string literal or an
            always-true predicate) silently corrupts merge results.

    Args:
        suffixed_cols: Source column names that received the ".__src__" join
            suffix (i.e. names that collide with a target column). Names absent
            from this set keep their bare name.
    """
    from daft.sql import sql_expr

    sql = _translate_arrow_cast(expr_str.strip())
    sql = _rewrite_merge_sql(sql, source_alias, target_alias, on, suffixed_cols)
    try:
        return sql_expr(sql)
    except Exception as e:
        raise ValueError(
            f"Could not parse merge expression {expr_str!r} (rewritten: {sql!r}): {e}"
        ) from e


def _parse_predicate(
    pred_str: str,
    source_alias: str,
    target_alias: str,
    on: list[str] | None = None,
    suffixed_cols: "set[str] | None" = None,
) -> Any:
    """Parse a predicate string into a Daft boolean expression via ``daft.sql_expr``.

    Supports the full SQL boolean grammar (AND/OR, comparisons, IS [NOT] NULL,
    IN, LIKE, parentheses, ...). Raises ValueError on unparseable input.
    """
    return _resolve_expr(pred_str, source_alias, target_alias, on or [], suffixed_cols)


def delta_schema_to_pyarrow(schema: "deltalake.Schema") -> "pa.Schema":
    import deltalake
    from packaging.version import parse

    if parse(deltalake.__version__) < parse("1.0.0"):
        return schema.to_pyarrow()
    else:
        import pyarrow as pa

        return pa.schema(schema.to_arrow())
