from __future__ import annotations

import pyarrow as pa
import pytest

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

import daft
from daft.io.iceberg import iceberg_changes_scan


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
    """Same COW history as tests/io/iceberg/test_iceberg_changelog_planning.py.

    S1 (APPEND): file A = (1,a),(2,b),(3,c).
    S2 (APPEND): file B = (4,d),(5,e).
    S3 (COW delete "id = 1"): DELETE file A, INSERT file A' = (2,b),(3,c).
    """
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.cow_history", schema=schema)
    table.append(pa.table({"id": [1, 2, 3], "data": ["a", "b", "c"]}))
    table.append(pa.table({"id": [4, 5], "data": ["d", "e"]}))
    table.delete("id = 1")
    table.refresh()
    return table


def _rows_by_commit(df: daft.DataFrame) -> dict[int, list[dict]]:
    result: dict[int, list[dict]] = {}
    for row in df.to_pylist():
        result.setdefault(row["_commit_snapshot_id"], []).append(row)
    return result


def test_full_history_changelog_matches_expected_cdc_stream(cow_history_table):
    s1, s2, s3 = (s.snapshot_id for s in cow_history_table.snapshots())

    df = daft.io.iceberg.read_iceberg_changes(cow_history_table)
    assert set(df.column_names) == {"id", "data", "_change_type", "_change_ordinal", "_commit_snapshot_id"}

    by_commit = _rows_by_commit(df)

    # S1: two carryover-free INSERTs.
    assert {(r["id"], r["data"], r["_change_type"]) for r in by_commit[s1]} == {
        (1, "a", "INSERT"),
        (2, "b", "INSERT"),
        (3, "c", "INSERT"),
    }
    assert all(r["_change_ordinal"] == 0 for r in by_commit[s1])

    # S2: two carryover-free INSERTs.
    assert {(r["id"], r["data"], r["_change_type"]) for r in by_commit[s2]} == {
        (4, "d", "INSERT"),
        (5, "e", "INSERT"),
    }
    assert all(r["_change_ordinal"] == 1 for r in by_commit[s2])

    # S3: COW rewrite of file A carried (2,b) and (3,c) over unchanged (same commit,
    # same content) -> cancelled by remove_carryovers. Only the real deletion of (1,a)
    # survives.
    assert by_commit[s3] == [
        {"id": 1, "data": "a", "_change_type": "DELETE", "_change_ordinal": 2, "_commit_snapshot_id": s3}
    ]


def test_partial_range_excludes_earlier_snapshots(cow_history_table):
    s1, s2, s3 = (s.snapshot_id for s in cow_history_table.snapshots())

    # (s1, s3] should only include S2 and S3's changes, not S1's.
    df = daft.io.iceberg.read_iceberg_changes(cow_history_table, start_snapshot_id=s1, end_snapshot_id=s3)
    commits = {row["_commit_snapshot_id"] for row in df.to_pylist()}
    assert commits == {s2, s3}


def test_start_equals_end_returns_empty_changelog(cow_history_table):
    s1 = cow_history_table.snapshots()[0].snapshot_id
    df = daft.io.iceberg.read_iceberg_changes(cow_history_table, start_snapshot_id=s1, end_snapshot_id=s1)
    assert df.to_pylist() == []


def test_read_iceberg_changes_does_not_eagerly_trigger_io(cow_history_table, monkeypatch):
    """Constructing the DataFrame must not run planning/footer I/O.

    Only a later execution action (like to_pylist()/collect()) should trigger planning.
    """
    called = False
    original = iceberg_changes_scan.IcebergChangesScanOperator._get_or_plan_tasks

    def spy(self):
        nonlocal called
        called = True
        return original(self)

    monkeypatch.setattr(iceberg_changes_scan.IcebergChangesScanOperator, "_get_or_plan_tasks", spy)

    df = daft.io.iceberg.read_iceberg_changes(cow_history_table)
    assert called is False, "constructing the DataFrame must not trigger planning"
    df.to_pylist()
    assert called is True


def test_mor_range_is_rejected(cow_history_table, monkeypatch):
    # pyiceberg 0.11.1 can't produce a real MOR delete manifest (see
    # test_iceberg_changelog_planning.py's equivalent test for why); patch the guard
    # function directly to simulate one being present, and verify the NotImplementedError
    # actually propagates through DataFrame execution end-to-end, not just from the
    # planning function in isolation.
    last_snapshot_id = cow_history_table.snapshots()[-1].snapshot_id
    from daft.io.iceberg import _changelog_planning

    original = _changelog_planning._snapshot_has_delete_manifest

    def fake(snapshot, io):
        if snapshot.snapshot_id == last_snapshot_id:
            return True
        return original(snapshot, io)

    monkeypatch.setattr(_changelog_planning, "_snapshot_has_delete_manifest", fake)

    df = daft.io.iceberg.read_iceberg_changes(cow_history_table)
    with pytest.raises(Exception, match="copy-on-write"):
        df.to_pylist()


def test_reserved_column_name_collision_rejected(local_catalog):
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="_change_type", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.name_collision", schema=schema)
    table.append(pa.table({"id": [1], "_change_type": ["x"]}))
    table.refresh()

    with pytest.raises(ValueError, match="reserved changelog metadata column names"):
        daft.io.iceberg.read_iceberg_changes(table)


def test_reserved_partition_field_name_in_range_is_rejected(local_catalog):
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.partition_name_collision", schema=schema)
    with table.update_spec() as update:
        update.add_field("data", "identity", partition_field_name="_change_type")
    table.refresh()
    table.append(pa.table({"id": [1], "data": ["a"]}))
    table.refresh()

    # The requested range includes the commit written under the colliding spec, so this
    # must be rejected.
    with pytest.raises(ValueError, match="reserved changelog metadata column names"):
        daft.io.iceberg.read_iceberg_changes(table).to_pylist()


def test_reserved_partition_field_name_outside_range_is_allowed(local_catalog):
    """A stale colliding spec outside the requested range must not reject the scan.

    A partition spec with a reserved-name collision that predates the requested range
    (and whose data files the range never touches) must not reject an otherwise-valid scan.
    The check is scoped to spec IDs referenced by this scan's planned tasks, not the
    table's entire historical spec set.
    """
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.partition_name_collision_excluded", schema=schema)

    with table.update_spec() as update:
        update.add_field("data", "identity", partition_field_name="_change_type")
    table.refresh()
    table.append(pa.table({"id": [1], "data": ["a"]}))
    table.refresh()
    s_old = table.current_snapshot().snapshot_id

    with table.update_spec() as update:
        update.remove_field("_change_type")
    table.refresh()
    table.append(pa.table({"id": [2], "data": ["b"]}))
    table.refresh()
    s_new = table.current_snapshot().snapshot_id

    # (s_old, s_new] only includes the commit written under the clean (post-evolution)
    # spec; the colliding spec_id is never referenced by any task in this range.
    df = daft.io.iceberg.read_iceberg_changes(table, start_snapshot_id=s_old, end_snapshot_id=s_new)
    rows = df.to_pylist()
    assert [(r["id"], r["data"], r["_change_type"]) for r in rows] == [(2, "b", "INSERT")]


def test_missing_data_file_raises_instead_of_silently_skipping(cow_history_table):
    import os

    # Find the DELETE task's data file (the original file A, no longer referenced by the
    # current table state) and delete it from disk to simulate orphan-file cleanup /
    # expiration having removed a file the changelog range still needs.
    s3 = cow_history_table.snapshots()[-1]
    deleted_path = None
    for manifest in s3.manifests(cow_history_table.io):
        if manifest.added_snapshot_id != s3.snapshot_id:
            continue
        for entry in manifest.fetch_manifest_entry(cow_history_table.io, discard_deleted=False):
            if entry.status.name == "DELETED":
                deleted_path = entry.data_file.file_path
    assert deleted_path is not None

    local_path = deleted_path.removeprefix("file://")
    os.remove(local_path)

    df = daft.io.iceberg.read_iceberg_changes(cow_history_table)
    with pytest.raises(Exception, match="No such file|FileNotFound|not found"):
        df.to_pylist()


# --- Optimizer boundary: limit/count/select must reflect the post-carryover-removal
# stream, not a raw pre-removal scan ---


@pytest.fixture(scope="function")
def heavy_carryover_table(local_catalog):
    """A COW rewrite where most rows are pure carryover noise and only one is a real change.

    If limit/count ever crossed the remove_carryovers window boundary, they would see the
    raw (carryover-inflated) row count/order instead of the resolved one.
    """
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.heavy_carryover", schema=schema)
    ids = list(range(20))
    table.append(pa.table({"id": ids, "data": [f"v{i}" for i in ids]}))
    # Delete a single row -> COW rewrites the file, carrying the other 19 rows over
    # unchanged (19 INSERT+DELETE carryover pairs = 38 raw rows) alongside 1 genuine DELETE.
    table.delete("id = 0")
    table.refresh()
    return table


def test_count_rows_reflects_post_carryover_result(heavy_carryover_table):
    df = daft.io.iceberg.read_iceberg_changes(heavy_carryover_table)
    # 20 genuine INSERTs (first snapshot) + 1 genuine DELETE (second snapshot, after 19
    # carryover pairs cancel) = 21, never the raw 20 + 38 = 58.
    assert df.count_rows() == 21


def test_limit_after_read_returns_real_rows_not_uncancelled_carryover(heavy_carryover_table):
    df = daft.io.iceberg.read_iceberg_changes(heavy_carryover_table)
    deletes = df.where(df["_change_type"] == "DELETE").to_pylist()
    # Exactly one DELETE must exist (id=0); if limit/filter had crossed the carryover
    # window, some of the 19 cancelled carryover DELETEs could have leaked through.
    assert len(deletes) == 1
    assert deletes[0]["id"] == 0

    limited = df.limit(1).to_pylist()
    assert len(limited) == 1


def test_select_after_read_preserves_carryover_correctness(heavy_carryover_table):
    df = daft.io.iceberg.read_iceberg_changes(heavy_carryover_table)
    projected = df.select("id", "_change_type").to_pylist()
    assert len(projected) == 21
    assert sum(1 for r in projected if r["_change_type"] == "DELETE") == 1


def test_select_only_cdc_metadata_columns(cow_history_table):
    df = daft.io.iceberg.read_iceberg_changes(cow_history_table)
    projected = df.select("_change_type", "_change_ordinal", "_commit_snapshot_id")
    assert set(projected.column_names) == {"_change_type", "_change_ordinal", "_commit_snapshot_id"}
    assert len(projected.to_pylist()) == len(df.to_pylist())


# --- Partition spec evolution (non-collision case): end-to-end read spanning two specs ---


def test_changelog_spans_two_partition_specs(local_catalog):
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.spec_evolution_e2e", schema=schema)

    # Spec 0: unpartitioned.
    table.append(pa.table({"id": [1], "data": ["a"]}))
    table.refresh()

    # Evolve to an identity partition on `data`.
    with table.update_spec() as update:
        update.add_field("data", "identity", partition_field_name="data_partition")
    table.refresh()

    # Spec 1: identity-partitioned by data.
    table.append(pa.table({"id": [2], "data": ["b"]}))
    table.refresh()

    df = daft.io.iceberg.read_iceberg_changes(table)
    rows = sorted(df.to_pylist(), key=lambda r: r["id"])
    assert [(r["id"], r["data"], r["_change_type"]) for r in rows] == [
        (1, "a", "INSERT"),
        (2, "b", "INSERT"),
    ]


def test_add_files_imported_data_file_appears_in_changelog(local_catalog, tmp_path):
    """Imported files must be treated as regular ADDED entries subject to the schema gate.

    Files imported via `table.add_files()` (bypassing the normal Iceberg writer) must
    still be picked up as regular ADDED entries and pass the schema gate -- design's
    file-level validation must not special-case or skip externally-imported files.
    """
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    table = local_catalog.create_table("default.add_files", schema=schema)

    pa_schema = schema_to_pyarrow(schema)
    external_path = tmp_path / "external.parquet"
    pq.write_table(pa.table({"id": [9], "data": ["z"]}, schema=pa_schema), str(external_path))

    table.add_files([f"file://{external_path}"])
    table.refresh()

    df = daft.io.iceberg.read_iceberg_changes(table)
    rows = df.to_pylist()
    assert [(r["id"], r["data"], r["_change_type"]) for r in rows] == [(9, "z", "INSERT")]
