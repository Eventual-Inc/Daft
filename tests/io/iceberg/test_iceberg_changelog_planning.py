from __future__ import annotations

from types import SimpleNamespace

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import ManifestContent
from pyiceberg.schema import Schema
from pyiceberg.table.snapshots import Operation
from pyiceberg.types import LongType, NestedField, StringType

from daft.io.iceberg._changelog_planning import (
    ChangelogFileTask,
    _require_int_timestamp_ms,
    _resolve_baseline_schema,
    _snapshot_has_delete_manifest,
    compute_snapshot_ordinals,
    ordered_changelog_snapshots,
    plan_changelog_file_tasks,
    resolve_changelog_range,
    resolve_snapshot_range,
)


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
        warehouse=f"file://{tmpdir}",
    )
    catalog.create_namespace("default")
    yield catalog
    catalog.engine.dispose()


@pytest.fixture(scope="function")
def cow_history_table(local_catalog):
    """A table with a pure copy-on-write history.

    Two appends followed by a partial delete that forces a copy-on-write rewrite of one of
    the appended files.

    Snapshot S1 (APPEND): adds file A with rows (1, 2, 3).
    Snapshot S2 (APPEND): adds file B with rows (4, 5).
    Snapshot S3 (OVERWRITE, COW): deletes file A, adds file A' with rows (2, 3).
    """
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.cow_history", schema=schema)

    batch_a = pa.table({"id": [1, 2, 3], "data": ["a", "b", "c"]})
    table.append(batch_a)

    batch_b = pa.table({"id": [4, 5], "data": ["d", "e"]})
    table.append(batch_b)

    table.delete("id = 1")

    table.refresh()
    return table


def test_resolve_snapshot_range_defaults_to_full_history(cow_history_table):
    snapshots = list(cow_history_table.snapshots())
    assert len(snapshots) == 3

    from_id, to_id = resolve_snapshot_range(cow_history_table, None, None, None, None)
    assert from_id is None
    assert to_id == cow_history_table.current_snapshot().snapshot_id


def test_resolve_snapshot_range_start_equals_end_is_valid(cow_history_table):
    s1 = cow_history_table.snapshots()[0].snapshot_id
    from_id, to_id = resolve_snapshot_range(cow_history_table, s1, s1, None, None)
    assert from_id == s1
    assert to_id == s1
    # An empty (from, to] range: no snapshots in between.
    assert ordered_changelog_snapshots(cow_history_table, from_id, to_id) == []


def test_resolve_snapshot_range_rejects_unknown_snapshot_id(cow_history_table):
    with pytest.raises(ValueError, match="was not found"):
        resolve_snapshot_range(cow_history_table, None, 123456789, None, None)


def test_resolve_snapshot_range_rejects_mutually_exclusive_start_args(cow_history_table):
    with pytest.raises(ValueError, match="mutually exclusive|Only one of"):
        resolve_snapshot_range(cow_history_table, 1, None, 1, None)


@pytest.mark.parametrize("bad_value", [1.0, 1.5, True, False, "1700000000000", None])
def test_require_int_timestamp_ms_rejects_non_int(bad_value):
    if bad_value is None:
        # None means "not provided" and is handled by the caller, not this validator;
        # skip it here since _require_int_timestamp_ms is only ever called when the
        # caller has already established the value is not None.
        pytest.skip("None is a valid 'not provided' sentinel, handled by the caller")
    with pytest.raises(TypeError, match="must be an int"):
        _require_int_timestamp_ms(bad_value, "start_timestamp_ms")


def test_require_int_timestamp_ms_accepts_plain_int():
    _require_int_timestamp_ms(1700000000000, "start_timestamp_ms")  # must not raise


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start_timestamp_ms": 1.5},
        {"start_timestamp_ms": True},
        {"end_timestamp_ms": 1.5},
        {"end_timestamp_ms": False},
    ],
)
def test_resolve_snapshot_range_rejects_non_int_timestamp(cow_history_table, kwargs):
    full_kwargs = {
        "start_snapshot_id": None,
        "end_snapshot_id": None,
        "start_timestamp_ms": None,
        "end_timestamp_ms": None,
    }
    full_kwargs.update(kwargs)
    with pytest.raises(TypeError, match="must be an int"):
        resolve_snapshot_range(cow_history_table, **full_kwargs)


def test_resolve_snapshot_range_timestamp_truth_table(cow_history_table):
    s1, s2, s3 = (s.snapshot_id for s in cow_history_table.snapshots())
    timestamps = [s.timestamp_ms for s in cow_history_table.snapshots()]

    # end_timestamp_ms resolves to the snapshot current at or before it (inclusive).
    from_id, to_id = resolve_snapshot_range(cow_history_table, None, None, None, timestamps[1])
    assert (from_id, to_id) == (None, s2)

    # start_timestamp_ms is the exclusive lower bound, resolved the same way.
    from_id, to_id = resolve_snapshot_range(cow_history_table, None, None, timestamps[0], timestamps[2])
    assert (from_id, to_id) == (s1, s3)

    # A timestamp strictly before the table's first snapshot has no valid resolution.
    with pytest.raises(ValueError, match="No snapshot found at or before"):
        resolve_snapshot_range(cow_history_table, None, None, timestamps[0] - 1_000_000, None)

    # start_timestamp_ms and start_snapshot_id are mutually exclusive.
    with pytest.raises(ValueError, match="Only one of"):
        resolve_snapshot_range(cow_history_table, s1, None, timestamps[0], None)
    with pytest.raises(ValueError, match="Only one of"):
        resolve_snapshot_range(cow_history_table, None, s1, None, timestamps[0])


def test_ordered_changelog_snapshots_full_history_oldest_first(cow_history_table):
    snapshots = list(cow_history_table.snapshots())
    from_id, to_id = resolve_snapshot_range(cow_history_table, None, None, None, None)
    ordered = ordered_changelog_snapshots(cow_history_table, from_id, to_id)

    assert [s.snapshot_id for s in ordered] == [s.snapshot_id for s in snapshots]


def test_ordered_changelog_snapshots_rejects_non_ancestor_start(cow_history_table):
    # A start_snapshot_id that isn't actually an ancestor of end_snapshot_id (here: swapped).
    s1, _, s3 = (s.snapshot_id for s in cow_history_table.snapshots())
    with pytest.raises(ValueError, match="not an ancestor"):
        ordered_changelog_snapshots(cow_history_table, s3, s1)


def test_plan_changelog_file_tasks_cow_insert_delete(cow_history_table):
    snapshots = list(cow_history_table.snapshots())
    s1_id, s2_id, s3_id = (s.snapshot_id for s in snapshots)
    ordinals = compute_snapshot_ordinals(snapshots)
    assert ordinals == {s1_id: 0, s2_id: 1, s3_id: 2}

    tasks = list(plan_changelog_file_tasks(cow_history_table, snapshots, ordinals))

    by_commit: dict[int, list[ChangelogFileTask]] = {}
    for task in tasks:
        by_commit.setdefault(task.commit_snapshot_id, []).append(task)

    # S1: one INSERT task for file A (3 rows).
    assert len(by_commit[s1_id]) == 1
    assert by_commit[s1_id][0].change_type == "INSERT"
    assert by_commit[s1_id][0].change_ordinal == 0
    assert by_commit[s1_id][0].data_file.record_count == 3

    # S2: one INSERT task for file B (2 rows).
    assert len(by_commit[s2_id]) == 1
    assert by_commit[s2_id][0].change_type == "INSERT"
    assert by_commit[s2_id][0].change_ordinal == 1
    assert by_commit[s2_id][0].data_file.record_count == 2

    # S3: COW rewrite of file A -- one DELETE (old file A, 3 rows) and one INSERT
    # (rewritten file A', 2 rows: id=1 was removed).
    s3_tasks = by_commit[s3_id]
    assert len(s3_tasks) == 2
    assert {t.change_type for t in s3_tasks} == {"INSERT", "DELETE"}
    for t in s3_tasks:
        assert t.change_ordinal == 2
        if t.change_type == "DELETE":
            assert t.data_file.record_count == 3
        else:
            assert t.data_file.record_count == 2


def test_snapshot_has_delete_manifest_true_when_any_manifest_is_deletes():
    fake_manifests = [
        SimpleNamespace(content=ManifestContent.DATA),
        SimpleNamespace(content=ManifestContent.DELETES),
    ]
    fake_snapshot = SimpleNamespace(manifests=lambda io: fake_manifests)
    assert _snapshot_has_delete_manifest(fake_snapshot, io=None) is True


def test_snapshot_has_delete_manifest_false_when_all_data():
    fake_manifests = [SimpleNamespace(content=ManifestContent.DATA)]
    fake_snapshot = SimpleNamespace(manifests=lambda io: fake_manifests)
    assert _snapshot_has_delete_manifest(fake_snapshot, io=None) is False


def test_ordered_changelog_snapshots_rejects_range_with_delete_manifest(cow_history_table, monkeypatch):
    # pyiceberg 0.11.1's own table.delete() always falls back to COW (merge-on-read isn't
    # implemented yet) and pyiceberg.table.snapshots.Snapshot is a frozen pydantic model, so
    # there's no way to construct or fake a real MOR delete manifest here. Instead, patch
    # _snapshot_has_delete_manifest itself (already covered in isolation by the two tests
    # above) to simulate one being present for the last snapshot in range, which exercises
    # ordered_changelog_snapshots's wiring: it must call the guard for every snapshot in
    # range and propagate NotImplementedError as soon as any of them trips it. A true
    # end-to-end test against a manifest genuinely written with position deletes is tracked
    # as follow-up coverage once available (e.g. via a Spark-authored MOR table fixture).
    last_snapshot_id = cow_history_table.snapshots()[-1].snapshot_id
    original = _snapshot_has_delete_manifest

    def fake_has_delete_manifest(snapshot, io):
        if snapshot.snapshot_id == last_snapshot_id:
            return True
        return original(snapshot, io)

    monkeypatch.setattr(
        "daft.io.iceberg._changelog_planning._snapshot_has_delete_manifest",
        fake_has_delete_manifest,
    )

    from_id, to_id = resolve_snapshot_range(cow_history_table, None, None, None, None)
    with pytest.raises(NotImplementedError, match="copy-on-write"):
        ordered_changelog_snapshots(cow_history_table, from_id, to_id)


def _fake_snapshot(snapshot_id: int, operation: Operation) -> SimpleNamespace:
    return SimpleNamespace(
        snapshot_id=snapshot_id,
        summary=SimpleNamespace(operation=operation),
        manifests=lambda io: [],
    )


def test_ordered_changelog_snapshots_skips_replace_operations(cow_history_table, monkeypatch):
    # pyiceberg 0.11.1's public write API has no way to produce a real REPLACE (compaction
    # / manifest-rewrite) snapshot -- Table.maintenance only exposes expire_snapshots, and
    # Transaction has no rewrite_manifests/compact_data_files. Fake the ancestors_between
    # result instead, which is legitimate here since REPLACE-skipping only depends on
    # `snapshot.summary.operation` and `snapshot.manifests(io)`, not real ancestry linkage.
    to_id = cow_history_table.snapshots()[-1].snapshot_id
    replace_snap = _fake_snapshot(999001, Operation.REPLACE)
    append_snap = _fake_snapshot(999002, Operation.APPEND)
    # ancestors_between walks newest -> oldest.
    monkeypatch.setattr(
        "daft.io.iceberg._changelog_planning.ancestors_between",
        lambda from_snap, to_snap, metadata: [replace_snap, append_snap],
    )

    result = ordered_changelog_snapshots(cow_history_table, None, to_id)
    assert [s.snapshot_id for s in result] == [append_snap.snapshot_id]


def test_ordered_changelog_snapshots_all_replace_region_is_empty(cow_history_table, monkeypatch):
    to_id = cow_history_table.snapshots()[-1].snapshot_id
    monkeypatch.setattr(
        "daft.io.iceberg._changelog_planning.ancestors_between",
        lambda from_snap, to_snap, metadata: [_fake_snapshot(999003, Operation.REPLACE)],
    )

    assert ordered_changelog_snapshots(cow_history_table, None, to_id) == []


def test_ordered_changelog_snapshots_replace_endpoint_is_skipped(cow_history_table, monkeypatch):
    # The endpoint (newest / first in ancestors_between's newest->oldest order) being a
    # REPLACE must not special-case anything -- it's still just skipped like any other.
    to_id = cow_history_table.snapshots()[-1].snapshot_id
    replace_endpoint = _fake_snapshot(999004, Operation.REPLACE)
    real_append = _fake_snapshot(999005, Operation.APPEND)
    monkeypatch.setattr(
        "daft.io.iceberg._changelog_planning.ancestors_between",
        lambda from_snap, to_snap, metadata: [replace_endpoint, real_append],
    )

    result = ordered_changelog_snapshots(cow_history_table, None, to_id)
    assert [s.snapshot_id for s in result] == [real_append.snapshot_id]


# --- ResolvedChangelogRange / resolve_changelog_range ---


def test_resolve_changelog_range_is_metadata_only_and_resolves_baseline(cow_history_table):
    """End-to-end (real table): a normal call resolves both the range and the baseline schema."""
    to_id = cow_history_table.snapshots()[-1].snapshot_id
    resolved = resolve_changelog_range(cow_history_table, None, None, None, None)
    assert resolved.to_snapshot_id == to_id
    assert resolved.from_snapshot_id is None
    assert resolved.end_snapshot.snapshot_id == to_id
    assert resolved.baseline_schema.schema_id == resolved.end_snapshot.schema_id


def test_resolve_changelog_range_invalid_end_snapshot_id_raises_without_any_io(cow_history_table):
    """A nonexistent end_snapshot_id must fail from this metadata-only call itself.

    No planning/footer I/O has happened by this point because range resolution occurs at
    construction time.
    """
    with pytest.raises(ValueError, match="was not found in this table's snapshot history"):
        resolve_changelog_range(cow_history_table, None, 999999999, None, None)


def test_resolve_changelog_range_historical_end_snapshot_uses_its_own_schema_not_current(local_catalog):
    """A historical end_snapshot_id's baseline must reflect *that* snapshot's schema_id.

    Constructs a table that evolves its schema after the historical snapshot -- something
    `IcebergChangesScanOperator`'s own front gate (still enforcing single-schema-per-table)
    would reject end to end, but `resolve_changelog_range` itself is a standalone function
    and must resolve the *historical* baseline correctly regardless, proving the resolution
    mechanism itself is snapshot-scoped and not just "whatever validate_single_schema_table
    happens to return today."
    """
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    )
    table = local_catalog.create_table("default.historical_baseline", schema=schema)
    table.append(pa.table({"id": [1, 2, 3]}))
    table.refresh()
    historical_snapshot_id = table.current_snapshot().snapshot_id
    historical_schema_id = table.current_snapshot().schema_id

    with table.update_schema() as update:
        update.add_column("extra", LongType())
    table.refresh()
    table.append(pa.table({"id": [4], "extra": [100]}))
    table.refresh()

    resolved = resolve_changelog_range(table, None, historical_snapshot_id, None, None)
    assert resolved.baseline_schema.schema_id == historical_schema_id
    assert "extra" not in {f.name for f in resolved.baseline_schema.fields}


def test_resolve_baseline_schema_uses_schema_id_when_present():
    fake_schema = SimpleNamespace(schema_id=7)
    fake_metadata = SimpleNamespace(schema_by_id=lambda schema_id: fake_schema if schema_id == 7 else None, schemas=[])
    fake_table = SimpleNamespace(metadata=fake_metadata)
    fake_snapshot = SimpleNamespace(snapshot_id=1, schema_id=7)

    assert _resolve_baseline_schema(fake_table, fake_snapshot) is fake_schema


def test_resolve_baseline_schema_schema_id_present_but_missing_raises():
    fake_metadata = SimpleNamespace(schema_by_id=lambda schema_id: None, schemas=[])
    fake_table = SimpleNamespace(metadata=fake_metadata)
    fake_snapshot = SimpleNamespace(snapshot_id=1, schema_id=999)

    with pytest.raises(ValueError, match="no schema with that id exists"):
        _resolve_baseline_schema(fake_table, fake_snapshot)


def test_resolve_baseline_schema_none_schema_id_falls_back_to_unique_schema():
    fake_schema = SimpleNamespace(schema_id=0)
    fake_metadata = SimpleNamespace(schemas=[fake_schema])
    fake_table = SimpleNamespace(metadata=fake_metadata)
    fake_snapshot = SimpleNamespace(snapshot_id=1, schema_id=None)

    assert _resolve_baseline_schema(fake_table, fake_snapshot) is fake_schema


def test_resolve_baseline_schema_none_schema_id_multiple_schemas_fails_closed():
    fake_metadata = SimpleNamespace(schemas=[SimpleNamespace(schema_id=0), SimpleNamespace(schema_id=1)])
    fake_table = SimpleNamespace(metadata=fake_metadata)
    fake_snapshot = SimpleNamespace(snapshot_id=1, schema_id=None)

    with pytest.raises(NotImplementedError, match="cannot reliably determine which schema"):
        _resolve_baseline_schema(fake_table, fake_snapshot)
