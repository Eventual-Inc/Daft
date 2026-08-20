"""Planning for Iceberg COW changelog (CDC) reads."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pyiceberg.manifest import ManifestContent, ManifestEntryStatus
from pyiceberg.table.snapshots import Operation, ancestors_between

if TYPE_CHECKING:
    from collections.abc import Iterator

    from pyiceberg.io import FileIO
    from pyiceberg.manifest import DataFile
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.table import Table
    from pyiceberg.table.snapshots import Snapshot

ChangeType = Literal["INSERT", "DELETE"]


@dataclass(frozen=True)
class ResolvedChangelogRange:
    """Metadata-only resolution of a changelog scan's snapshot range and baseline schema.

    Produced once, at DataFrame-construction time, by `resolve_changelog_range` -- the
    single entry point for "what baseline schema does this scan use". Every consumer (the operator itself, the
    `compute_updates` identifier resolver, and the schema gate) reads
    `.baseline_schema` off this one object rather than each independently re-deriving it
    (e.g. by calling `table.schema()`, which reflects the table's *current* schema and can
    diverge from what a historical `end_snapshot_id` actually used).
    """

    from_snapshot_id: int | None
    to_snapshot_id: int
    end_snapshot: Snapshot
    baseline_schema: IcebergSchema


def _resolve_baseline_schema(table: Table, end_snapshot: Snapshot) -> IcebergSchema:
    """Resolve the schema `end_snapshot` was committed under.

    `Snapshot.schema_id` is an optional field in the Iceberg spec and can genuinely be
    `None` (e.g. snapshots produced by older writers). The fallback below only ever
    decides *which schema object to use as baseline* -- it never affects whether a given
    data file still gets a full, unconditional footer-based schema reconciliation; that
    happens unconditionally downstream regardless of how the baseline itself was resolved.
    """
    if end_snapshot.schema_id is not None:
        baseline = table.metadata.schema_by_id(end_snapshot.schema_id)
        if baseline is None:
            raise ValueError(
                f"end snapshot (id={end_snapshot.snapshot_id}) declares "
                f"schema_id={end_snapshot.schema_id}, but no schema with that id exists in this "
                "table's metadata; the table's schema history may be corrupted or incompletely "
                "loaded."
            )
        return baseline

    if len(table.metadata.schemas) == 1:
        return table.metadata.schemas[0]

    raise NotImplementedError(
        f"end snapshot (id={end_snapshot.snapshot_id}) does not declare a schema_id, and this "
        f"table's metadata contains {len(table.metadata.schemas)} schemas; "
        "daft.io.iceberg.read_iceberg_changes() cannot reliably determine which schema this snapshot was "
        "written under. This is only supported when the table's metadata contains exactly one "
        "schema."
    )


def resolve_changelog_range(
    table: Table,
    start_snapshot_id: int | None,
    end_snapshot_id: int | None,
    start_timestamp_ms: int | None,
    end_timestamp_ms: int | None,
) -> ResolvedChangelogRange:
    """Resolve the snapshot range and baseline schema -- metadata-only, no I/O.

    Safe to call eagerly at DataFrame-construction time: `resolve_snapshot_range` and
    `_resolve_baseline_schema` only ever touch `table.metadata`/`table.current_snapshot()`/
    `table.snapshot_by_id()`, structures already loaded in memory once `table` itself was
    constructed. The I/O-triggering steps (`ordered_changelog_snapshots`,
    `plan_changelog_file_tasks`, the footer schema gate) remain deferred to the first
    `to_scan_tasks()` call.
    """
    from_id, to_id = resolve_snapshot_range(
        table, start_snapshot_id, end_snapshot_id, start_timestamp_ms, end_timestamp_ms
    )
    end_snapshot = table.snapshot_by_id(to_id)
    assert end_snapshot is not None, "resolve_snapshot_range guarantees to_id exists in the table's snapshot history"

    baseline_schema = _resolve_baseline_schema(table, end_snapshot)
    return ResolvedChangelogRange(
        from_snapshot_id=from_id,
        to_snapshot_id=to_id,
        end_snapshot=end_snapshot,
        baseline_schema=baseline_schema,
    )


@dataclass(frozen=True)
class ChangelogFileTask:
    """A single data file's contribution to a changelog scan.

    Every row in `data_file` is uniformly `change_type` — this only holds for pure
    copy-on-write ranges, where a data file is wholly added or wholly removed by a single
    snapshot.
    """

    data_file: DataFile
    change_type: ChangeType
    change_ordinal: int
    commit_snapshot_id: int
    spec_id: int
    """The partition-spec id of the *manifest* this entry came from (`ManifestFile.
    partition_spec_id`) -- individual `DataFile` records don't carry their own spec id, so
    this must be threaded through from the enclosing manifest at planning time."""


def _require_int_timestamp_ms(value: int | None, name: str) -> None:
    """Enforce the "UTC millisecond epoch integer" contract for timestamp parameters.

    `bool` is a subclass of `int` in Python, so a plain `isinstance(value, int)` check
    would silently accept `True`/`False`; `type(value) is int` rejects that along with
    `float`, `str`, `datetime`, etc. Table.snapshot_as_of_timestamp does no validation of
    its own -- a float or bool would otherwise be silently compared against snapshot
    timestamps and could resolve to a plausible-looking but wrong historical boundary
    instead of raising.
    """
    if type(value) is not int:
        raise TypeError(
            f"{name} must be an int (UTC millisecond epoch timestamp), got {value!r} ({type(value).__name__})"
        )


def resolve_snapshot_range(
    table: Table,
    start_snapshot_id: int | None,
    end_snapshot_id: int | None,
    start_timestamp_ms: int | None,
    end_timestamp_ms: int | None,
) -> tuple[int | None, int]:
    """Resolve user-provided snapshot/timestamp options into (from_snapshot_id_exclusive, to_snapshot_id_inclusive)."""
    if start_timestamp_ms is not None:
        _require_int_timestamp_ms(start_timestamp_ms, "start_timestamp_ms")
    if end_timestamp_ms is not None:
        _require_int_timestamp_ms(end_timestamp_ms, "end_timestamp_ms")

    if start_snapshot_id is not None and start_timestamp_ms is not None:
        raise ValueError("Only one of start_snapshot_id or start_timestamp_ms may be provided")
    if end_snapshot_id is not None and end_timestamp_ms is not None:
        raise ValueError("Only one of end_snapshot_id or end_timestamp_ms may be provided")

    if start_timestamp_ms is not None:
        start_snapshot = table.snapshot_as_of_timestamp(start_timestamp_ms, inclusive=True)
        if start_snapshot is None:
            raise ValueError(
                f"No snapshot found at or before start_timestamp_ms={start_timestamp_ms}; "
                "the table's earliest known snapshot is later than this timestamp."
            )
        start_snapshot_id = start_snapshot.snapshot_id

    if end_timestamp_ms is not None:
        end_snapshot = table.snapshot_as_of_timestamp(end_timestamp_ms, inclusive=True)
        if end_snapshot is None:
            raise ValueError(
                f"No snapshot found at or before end_timestamp_ms={end_timestamp_ms}; "
                "the table's earliest known snapshot is later than this timestamp."
            )
        end_snapshot_id = end_snapshot.snapshot_id

    if end_snapshot_id is None:
        current = table.current_snapshot()
        if current is None:
            raise ValueError("table has no snapshots to read changes from")
        end_snapshot_id = current.snapshot_id

    if start_snapshot_id is not None and table.snapshot_by_id(start_snapshot_id) is None:
        raise ValueError(f"start_snapshot_id={start_snapshot_id} was not found in this table's snapshot history")
    if table.snapshot_by_id(end_snapshot_id) is None:
        raise ValueError(f"end_snapshot_id={end_snapshot_id} was not found in this table's snapshot history")

    return start_snapshot_id, end_snapshot_id


def _snapshot_has_delete_manifest(snapshot: Snapshot, io: FileIO) -> bool:
    """Whether this snapshot's *currently effective* manifest list contains any delete manifest.

    Deliberately checks `snapshot.manifests(io)` — the full manifest list this snapshot's
    table state references, including manifests inherited unchanged from ancestor commits —
    not just manifests this snapshot itself wrote. A narrower "self-added only" check would
    let a range through even though an older, still-referenced delete manifest continues to
    apply position/equality deletes to some data file in range; since COW changelog reads
    never apply delete files, that would silently misclassify deleted rows as INSERTs. This
    mirrors Iceberg's own `BaseIncrementalChangelogScan` semantics.
    """
    return any(manifest.content == ManifestContent.DELETES for manifest in snapshot.manifests(io))


def ordered_changelog_snapshots(table: Table, from_id: int | None, to_id: int) -> list[Snapshot]:
    """Return the snapshots in (from_id, to_id] ordered oldest-first.

    - Snapshots whose operation is `REPLACE` (compaction / manifest rewrite) are skipped: they
      never represent a logical data change.
    - If any snapshot in range's currently effective manifest list contains a delete manifest,
      raises `NotImplementedError` — daft.io.iceberg.read_iceberg_changes() only supports pure
      copy-on-write (COW) ranges.
    """
    to_snapshot = table.snapshot_by_id(to_id)
    if to_snapshot is None:
        raise ValueError(f"end_snapshot_id={to_id} was not found in this table's snapshot history")

    from_snapshot = None
    if from_id is not None:
        from_snapshot = table.snapshot_by_id(from_id)
        if from_snapshot is None:
            raise ValueError(f"start_snapshot_id={from_id} was not found in this table's snapshot history")

    # ancestors_between walks newest -> oldest and is *inclusive* of from_snapshot; the
    # requested range is exclusive of it, so we validate it was actually reached and drop it.
    chain = list(ancestors_between(from_snapshot, to_snapshot, table.metadata))
    if from_snapshot is not None:
        if not chain or chain[-1].snapshot_id != from_snapshot.snapshot_id:
            raise ValueError(
                f"start_snapshot_id={from_id} is not an ancestor of end_snapshot_id={to_id} "
                "(it may be later than end_snapshot_id, or on a different branch)"
            )
        chain = chain[:-1]

    chain.reverse()  # oldest -> newest

    result = []
    for snapshot in chain:
        operation = snapshot.summary.operation if snapshot.summary is not None else Operation.APPEND
        if operation == Operation.REPLACE:
            continue
        if _snapshot_has_delete_manifest(snapshot, table.io):
            raise NotImplementedError(
                f"Snapshot {snapshot.snapshot_id} (operation={operation.value}) has a delete "
                "manifest in its currently effective manifest list. daft.io.iceberg.read_iceberg_changes() "
                "only supports pure copy-on-write (COW) snapshot ranges; merge-on-read "
                "changelogs (position/equality deletes) are not yet supported."
            )
        result.append(snapshot)
    return result


def compute_snapshot_ordinals(snapshots: list[Snapshot]) -> dict[int, int]:
    """Assign a dense, scan-relative ordinal to each included snapshot (oldest = 0).

    This ordinal is *not* a globally stable identifier — a different snapshot range over the
    same table will reassign ordinals from 0. Use `commit_snapshot_id` for a stable reference.
    """
    return {snapshot.snapshot_id: i for i, snapshot in enumerate(snapshots)}


def plan_changelog_file_tasks(
    table: Table,
    snapshots: list[Snapshot],
    ordinals: dict[int, int],
) -> Iterator[ChangelogFileTask]:
    """Yield one ChangelogFileTask per data file added or removed by the given snapshots.

    For each snapshot, only manifests it *itself* wrote (`added_snapshot_id == snapshot_id`)
    are scanned — manifests merely inherited unchanged from an ancestor commit are not
    reprocessed. This is a different, narrower filter than `_snapshot_has_delete_manifest`'s
    "currently effective manifest list" check above: this one is for precisely collecting
    what a commit actually changed, while that one conservatively decides whether the range
    is safe at all. These filters serve different purposes and must not be combined.
    """
    for snapshot in snapshots:
        ordinal = ordinals[snapshot.snapshot_id]
        for manifest in snapshot.manifests(table.io):
            if manifest.content != ManifestContent.DATA:
                # Guarded against by ordered_changelog_snapshots's delete-manifest check
                # above; skipped defensively rather than assumed.
                continue
            if manifest.added_snapshot_id != snapshot.snapshot_id:
                continue
            # Manifests are immutable, so a manifest newly written by this commit only
            # contains entries this commit itself changed — ADDED or DELETED. Note that for
            # DELETED entries, `entry.snapshot_id` is the deleting commit's id (this
            # snapshot), not the id of whichever earlier snapshot originally added the file —
            # per the Iceberg V2 spec, a manifest entry's snapshot_id field always records
            # "where the file was added, or deleted if status is DELETED".
            for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=False):
                if entry.status == ManifestEntryStatus.ADDED:
                    change_type: ChangeType = "INSERT"
                elif entry.status == ManifestEntryStatus.DELETED:
                    change_type = "DELETE"
                else:
                    continue  # EXISTING: re-filed during manifest merge, no logical change
                yield ChangelogFileTask(
                    data_file=entry.data_file,
                    change_type=change_type,
                    change_ordinal=ordinal,
                    commit_snapshot_id=snapshot.snapshot_id,
                    spec_id=manifest.partition_spec_id,
                )
