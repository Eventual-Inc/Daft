# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any, Union

from daft import context, runners
from daft.api_annotations import PublicAPI
from daft.daft import IOConfig, ScanOperatorHandle, StorageConfig
from daft.dataframe import DataFrame
from daft.filesystem import get_protocol_from_path
from daft.io._checkpoint import attach_checkpoint
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from pyiceberg.table import Table as PyIcebergTable

    from daft.checkpoint import CheckpointConfig


logger = logging.getLogger(__name__)


def _convert_iceberg_file_io_properties_to_io_config(
    props: dict[str, Any], location: str | None = None
) -> IOConfig | None:
    """Property keys defined here: https://github.com/apache/iceberg-python/blob/main/pyiceberg/io/__init__.py.

    For an ``oss://`` ``location`` (Alibaba Cloud OSS, S3-compatible), the IOConfig gets
    virtual-hosted addressing and an ``oss``->``s3`` alias so the S3 filesystem resolves
    ``oss://`` paths -- applied even with no IO properties (e.g. env-var credentials).
    """
    from daft.io import AzureConfig, GCSConfig, IOConfig, S3Config

    any_props_set = False

    def get_first_property_value(*property_names: str) -> Any | None:
        for property_name in property_names:
            if property_value := props.get(property_name):
                nonlocal any_props_set
                any_props_set = True
                return property_value
        return None

    is_oss = location is not None and get_protocol_from_path(location) == "oss"

    io_config = IOConfig(
        s3=S3Config(
            endpoint_url=get_first_property_value("s3.endpoint"),
            region_name=get_first_property_value("s3.region", "client.region"),
            key_id=get_first_property_value("s3.access-key-id", "client.access-key-id"),
            access_key=get_first_property_value("s3.secret-access-key", "client.secret-access-key"),
            session_token=get_first_property_value("s3.session-token", "client.session-token"),
            force_virtual_addressing=True if is_oss else None,
        ),
        azure=AzureConfig(
            storage_account=get_first_property_value("adls.account-name", "adlfs.account-name"),
            access_key=get_first_property_value("adls.account-key", "adlfs.account-key"),
            sas_token=get_first_property_value("adls.sas-token", "adlfs.sas-token"),
            tenant_id=get_first_property_value("adls.tenant-id", "adlfs.tenant-id"),
            client_id=get_first_property_value("adls.client-id", "adlfs.client-id"),
            client_secret=get_first_property_value("adls.client-secret", "adlfs.client-secret"),
        ),
        gcs=GCSConfig(
            project_id=get_first_property_value("gcs.project-id"),
            token=get_first_property_value("gcs.oauth2.token"),
        ),
        protocol_aliases={"oss": "s3"} if is_oss else None,
    )

    if is_oss:
        logger.debug("oss:// table detected; applying S3-compatible settings to the IOConfig")
        return io_config
    return io_config if any_props_set else None


def _resolve_ref_snapshot_id(table: "PyIcebergTable", ref_name: str, ref_kind: str) -> int:
    from pyiceberg.table.refs import SnapshotRefType  # pyiceberg is an optional dependency

    expected_ref_type = SnapshotRefType.BRANCH if ref_kind == "branch" else SnapshotRefType.TAG
    ref = table.refs().get(ref_name)

    if ref is None:
        raise ValueError(f"Iceberg {ref_kind} {ref_name!r} does not exist")

    if ref.snapshot_ref_type != expected_ref_type:
        raise ValueError(f"Iceberg {ref_kind} {ref_name!r} is a {ref.snapshot_ref_type.value}")

    return ref.snapshot_id


# Internal helper used by read_iceberg and the Rust SQL scan path; not a public API.
def resolve_snapshot_id(
    table: "PyIcebergTable",
    snapshot_id: int | None,
    branch: str | None,
    tag: str | None,
) -> int | None:
    if sum(value is not None for value in (snapshot_id, branch, tag)) > 1:
        raise ValueError("Only one of snapshot_id, branch, or tag may be provided")

    if branch is not None:
        return _resolve_ref_snapshot_id(table, branch, "branch")
    if tag is not None:
        return _resolve_ref_snapshot_id(table, tag, "tag")

    return snapshot_id


# Internal helper used by read_iceberg and the Rust SQL scan path; not a public API.
def resolve_iceberg_io_config(table: "PyIcebergTable", io_config: IOConfig | None = None) -> IOConfig | None:
    """Resolve the IOConfig for an Iceberg scan using the standard precedence.

    Precedence: an explicitly-provided ``io_config`` wins; otherwise the table's PyIceberg
    FileIO properties are translated (handling S3/Azure/GCS credentials and the ``oss://``
    alias); otherwise the context ``default_io_config`` is used.

    Shared by :func:`read_iceberg` and the Rust SQL ``read_iceberg`` scan path so both honor
    table-embedded credentials and the globally-set default IOConfig.
    """
    if io_config is None:
        io_config = _convert_iceberg_file_io_properties_to_io_config(table.io.properties, table.location())
    if io_config is None:
        io_config = context.get_context().daft_planning_config.default_io_config
    return io_config


@PublicAPI
def read_iceberg(
    table: Union[str, os.PathLike[str], "PyIcebergTable"],
    snapshot_id: int | None = None,
    branch: str | None = None,
    tag: str | None = None,
    io_config: IOConfig | None = None,
    checkpoint: "CheckpointConfig | None" = None,
    ignore_corrupt_files: bool = False,
) -> DataFrame:
    """Create a DataFrame from an Iceberg table.

    Args:
        table (str, os.PathLike, or pyiceberg.table.Table): A path to an Iceberg metadata file (supports remote URLs
            to object stores such as ``s3://`` or ``gs://``) or a
            [PyIceberg Table](https://py.iceberg.apache.org/reference/pyiceberg/table/#pyiceberg.table.Table) created
            using the PyIceberg library.
        snapshot_id (int, optional): Snapshot ID of the table to query
        branch (str, optional): Iceberg branch name to query. Cannot be combined with ``snapshot_id`` or ``tag``.
        tag (str, optional): Iceberg tag name to query. Cannot be combined with ``snapshot_id`` or ``branch``.
        io_config (IOConfig, optional): A custom IOConfig to use when accessing Iceberg object storage data. If provided, configurations set in `table` are ignored.
        checkpoint: Optional :class:`daft.CheckpointConfig` for progress tracking across runs. Bundles the
            checkpoint store, the source key column (``on=``), and optional anti-join tuning. Rows whose key
            already exists in the store are skipped on re-run. Requires the Ray runner.
        ignore_corrupt_files (bool): If True, silently skip corrupt or unreadable data files
            instead of raising an error. Skipped files are recorded in ``df.skipped_corrupt_files``
            after collection. Defaults to False.

    Returns:
        DataFrame: a DataFrame with the schema converted from the specified Iceberg table

    Note:
        This function requires the use of [PyIceberg](https://py.iceberg.apache.org/), which is the Apache Iceberg's
        official project for Python.

    Examples:
        Read an Iceberg table from a PyIceberg table:
        >>> import pyiceberg
        >>>
        >>> table = pyiceberg.Table(...)
        >>> df = daft.read_iceberg(table)
        >>>
        >>> # Filters on this dataframe can now be pushed into the read operation from Iceberg
        >>> df = df.where(df["foo"] > 5)
        >>> df.show()

        Read an Iceberg table from S3 using IOConfig:
        >>> from daft.io import S3Config, IOConfig
        >>> io_config = IOConfig(s3=S3Config(region="us-west-2", anonymous=True))
        >>> df = daft.read_iceberg("s3://bucket/path/to/iceberg/metadata.json", io_config=io_config)
        >>> df.show()
    """
    from pyiceberg.table import StaticTable

    from daft.io.iceberg.iceberg_scan import IcebergDataSource

    # support for read_iceberg('path/to/metadata.json')
    if isinstance(table, (str, os.PathLike)):
        table = StaticTable.from_metadata(metadata_location=os.fspath(table))

    snapshot_id = resolve_snapshot_id(table, snapshot_id, branch, tag)

    io_config = resolve_iceberg_io_config(table, io_config)

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    iceberg_source = IcebergDataSource(
        table, snapshot_id=snapshot_id, storage_config=storage_config, ignore_corrupt_files=ignore_corrupt_files
    )

    handle = ScanOperatorHandle.from_data_source(iceberg_source)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    builder = attach_checkpoint(builder, checkpoint)
    return DataFrame(builder)


@PublicAPI
def read_iceberg_changes(
    table: Union[str, os.PathLike[str], "PyIcebergTable"],
    start_snapshot_id: int | None = None,
    end_snapshot_id: int | None = None,
    start_timestamp_ms: int | None = None,
    end_timestamp_ms: int | None = None,
    io_config: IOConfig | None = None,
    compute_updates: bool = False,
    identifier_columns: list[str] | None = None,
    net_changes: bool = False,
) -> DataFrame:
    """Create a changelog (CDC) DataFrame from a copy-on-write (COW) Iceberg table over a snapshot range.

    Only pure copy-on-write snapshot ranges are supported: if any snapshot in the range has a
    delete manifest (position/equality deletes) in its currently effective manifest list, a
    ``NotImplementedError`` is raised rather than an incorrect result. Tables with schema
    evolution history (added/renamed/type-promoted columns, including nested fields inside
    structs/lists/maps) are supported; each changelog data file's Parquet footer is verified
    against the schema declared by the resolved end snapshot. Requires
    ``pyiceberg>=0.11.0`` (raises ``NotImplementedError`` immediately, before any I/O, if the
    installed version is older); :func:`read_iceberg` (non-CDC) is unaffected and continues to
    support older PyIceberg versions. A baseline field with a declared Iceberg
    ``initial_default`` that's missing from an older data file is rejected rather than
    silently filled with ``None`` -- default-value materialization for historical files is not
    yet implemented.

    Every row in the returned DataFrame carries three additional columns: ``_change_type``
    (``"INSERT"``, ``"DELETE"``, or -- when ``compute_updates=True`` -- ``"UPDATE_BEFORE"``/
    ``"UPDATE_AFTER"``), ``_change_ordinal`` (int64, scan-relative -- not a stable cross-scan
    identifier), and ``_commit_snapshot_id`` (int64, the Iceberg snapshot id that produced the
    change). Carryover rows -- COW file-rewrite noise where a row's content didn't actually
    change but still appears as a (DELETE, INSERT) pair because the whole file was rewritten --
    are removed before the DataFrame is returned. This cannot be disabled, matching Iceberg's own
    ``create_changelog_view`` procedure -- except when ``net_changes=True``, which replaces this
    per-commit carryover removal with net cancellation across the entire scanned range instead.

    Args:
        table (str, os.PathLike, or pyiceberg.table.Table): Same as :func:`read_iceberg`.
        start_snapshot_id (int, optional): Snapshot ID marking the exclusive start of the range.
            If omitted (along with ``start_timestamp_ms``), the range starts from the table's
            first snapshot.
        end_snapshot_id (int, optional): Snapshot ID marking the inclusive end of the range. If
            omitted (along with ``end_timestamp_ms``), defaults to the table's current snapshot.
        start_timestamp_ms (int, optional): UTC millisecond epoch timestamp resolved to the
            snapshot current at or before it, then used as the exclusive start boundary. Cannot
            be combined with ``start_snapshot_id``.
        end_timestamp_ms (int, optional): UTC millisecond epoch timestamp resolved to the
            snapshot current at or before it, then used as the inclusive end boundary. Cannot be
            combined with ``end_snapshot_id``.
        io_config (IOConfig, optional): Same as :func:`read_iceberg`.
        compute_updates (bool): If True, re-mark matched DELETE+INSERT pairs (by
            ``identifier_columns``) as ``UPDATE_BEFORE``/``UPDATE_AFTER`` instead of leaving them
            as independent ``DELETE``/``INSERT`` events. Pairing is cross-ordinal: an identifier
            deleted in one commit and re-inserted in a later one (with no intervening event for
            that identifier) is paired as a single update, matching Iceberg's
            ``ComputeUpdateIterator`` semantics. This only proves the identifier's event sequence
            paired up -- it does not prove any non-identifier column actually changed; some
            writers can split a single logical update into separate DELETE and APPEND commits,
            which would be paired here even though the row's content never changed. Cardinality
            and consistency validation (at most one DELETE and one INSERT per identifier per
            commit) only runs when the resulting ``_change_type`` column is actually consumed --
            if you ``select()`` it away before collecting, validation does not run, but every
            other column's value is unaffected either way. Defaults to False.
        identifier_columns (list[str], optional): Column names that uniquely identify a logical
            row, used to pair DELETE+INSERT events when ``compute_updates=True``. Defaults to the
            table's schema-declared identifier fields (as of ``end_snapshot_id``) when omitted;
            raises ``ValueError`` if explicitly passed as an empty list, or if the table declares
            no identifier fields and none are given. Ignored (and must not be passed) unless
            ``compute_updates=True``; also may not be passed when ``net_changes=True``.
        net_changes (bool): If True, net-cancel changes across the *entire* scanned range
            instead of only within each commit, replacing (not layering on top of) the
            unconditional carryover removal -- matching Iceberg's own procedure, which runs one
            or the other, never both. A row that nets to zero across the range (e.g. inserted
            then later deleted, with no other net change) produces no output at all; a row whose
            net count is nonzero is replayed that many times using its *first* occurrence's
            metadata. Mutually exclusive with ``compute_updates``; ``identifier_columns`` may not
            be passed together with this (net cancellation matches on every non-metadata column,
            not an identifier). Defaults to False.

    Returns:
        DataFrame: A DataFrame with the table's schema plus ``_change_type``, ``_change_ordinal``,
            and ``_commit_snapshot_id`` columns.

    Note:
        This function requires the use of [PyIceberg](https://py.iceberg.apache.org/).

    Examples:
        >>> df = daft.io.iceberg.read_iceberg_changes(table, start_snapshot_id=100, end_snapshot_id=105)
        >>> df = daft.io.iceberg.read_iceberg_changes(table, compute_updates=True, identifier_columns=["id"])
        >>> df = daft.io.iceberg.read_iceberg_changes(table, net_changes=True)
    """
    from pyiceberg.table import StaticTable

    from daft.io.iceberg._changelog_planning import resolve_changelog_range
    from daft.io.iceberg._changelog_schema import require_pyiceberg_version_for_changelog
    from daft.io.iceberg.iceberg_changes_scan import IcebergChangesScanOperator

    # Checked first, before any argument validation or I/O: an unsupported PyIceberg
    # version must fail immediately, not partway through range resolution or planning.
    # Applies regardless of how many schemas
    # the table has had -- the single-schema gate depends on the same PyIceberg signatures.
    require_pyiceberg_version_for_changelog()

    if isinstance(table, (str, os.PathLike)):
        table = StaticTable.from_metadata(metadata_location=os.fspath(table))

    if compute_updates and net_changes:
        raise ValueError("compute_updates and net_changes may not both be True.")
    if net_changes and identifier_columns is not None:
        raise ValueError("identifier_columns may not be passed when net_changes=True.")
    if not compute_updates and not net_changes and identifier_columns is not None:
        raise ValueError("identifier_columns may only be passed when compute_updates=True.")

    # Resolved once, here, at DataFrame-construction time (metadata-only, no I/O) and passed explicitly to every consumer
    # below, rather than each independently re-deriving it or reaching into the operator's
    # internals: the operator itself, and (when compute_updates=True) the identifier
    # resolver, both read `.baseline_schema` off this one shared object.
    resolved_range = resolve_changelog_range(
        table, start_snapshot_id, end_snapshot_id, start_timestamp_ms, end_timestamp_ms
    )

    io_config = (
        _convert_iceberg_file_io_properties_to_io_config(table.io.properties, table.location())
        if io_config is None
        else io_config
    )
    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config

    multithreaded_io = runners.get_or_create_runner().name != "ray"
    storage_config = StorageConfig(multithreaded_io, io_config)

    scan_operator = IcebergChangesScanOperator(
        table,
        resolved_range=resolved_range,
        storage_config=storage_config,
    )

    handle = ScanOperatorHandle.from_python_scan_operator(scan_operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    df = DataFrame(builder)

    if net_changes:
        # Replaces (never layers on top of) carryover removal -- same-commit carryover
        # noise is naturally absorbed by net_changes' own running-total reset.
        from daft.io.iceberg._changelog_net_changes import net_changes as apply_net_changes

        df = apply_net_changes(df)
    else:
        from daft.io.iceberg._changelog_postprocess import remove_carryovers

        df = remove_carryovers(df)

        if compute_updates:
            from daft.io.iceberg._changelog_pairing import pair_updates, resolve_identifier_columns

            resolved_identifier_columns = resolve_identifier_columns(identifier_columns, resolved_range.baseline_schema)
            df = pair_updates(df, resolved_identifier_columns)

    return df
