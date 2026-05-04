"""Tests for `write_iceberg(checkpoint=...)` — idempotent commits.

The checkpoint-aware source filter (`daft.read_parquet(checkpoint=...)`)
runs only on the Ray runner, so these tests are skipped on the native
runner. See `tests/checkpoint/test_native_runner_gate.py`.
"""

from __future__ import annotations

import os
import re
from unittest.mock import patch

import pytest

import daft

pyiceberg = pytest.importorskip("pyiceberg")

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import Transaction
from pyiceberg.types import LongType, NestedField, StringType

from daft.daft import CheckpointStatus

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="checkpoint+write_iceberg requires Ray runner",
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
def iceberg_table(local_catalog):
    schema = Schema(
        NestedField(field_id=1, name="file_id", type=StringType()),
        NestedField(field_id=2, name="x", type=LongType()),
    )
    return local_catalog.create_table("default.idempotent_test", schema, partition_spec=UNPARTITIONED_PARTITION_SPEC)


@pytest.fixture(scope="function")
def parquet_input(tmpdir):
    """A small parquet input directory we can read with checkpoint=."""
    path = str(tmpdir / "input")
    os.makedirs(path, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}).write_parquet(path)
    return path


@pytest.fixture(scope="function")
def checkpoint_store(tmpdir):
    return daft.CheckpointStore(f"file://{tmpdir}/ckpt")


def test_recovery_after_crash_between_commit_and_mark(iceberg_table, parquet_input, checkpoint_store):
    """Scenario A: crash after commit succeeded but before mark_committed ran.

    Catalog has the snapshot with our markers; store still says `Checkpointed`.
    A fresh call to `write_iceberg(checkpoint=...)` must detect the marker, call
    `mark_committed`, and NOT produce a second snapshot.
    """
    # First call: simulate the crash by patching mark_committed to raise.
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(checkpoint_store, "mark_committed", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # State: catalog has exactly one snapshot with our markers; store entries are
    # still in Checkpointed state because mark_committed never ran.
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    snapshot_id_after_crash = iceberg_table.metadata.snapshots[0].snapshot_id
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"
    crashed_query_id = pending_after_crash[0].query_id

    # Second call: should hit the recovery path. mark_committed is no longer patched.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # Verify: still exactly one snapshot, no double-write.
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1, "recovery must not produce a second snapshot"

    # Verify: it's the *same* snapshot — recovery did not retire the original
    # and replace it with a fresh one. The "exactly 1 snapshot" check above
    # would still pass if recovery had committed a new snapshot whose data
    # superseded the first; the snapshot_id continuity check rules that out.
    assert iceberg_table.metadata.snapshots[0].snapshot_id == snapshot_id_after_crash, (
        "recovery must keep the original snapshot, not replace it with a new one"
    )

    # Verify: previously-Checkpointed entries are now Committed.
    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed, "all entries should be Committed after recovery"

    # Verify: snapshot summary carries the markers in the expected shape. The
    # query_id assertion catches the empty-string failure mode where the field
    # is silently defaulted but the marker has no real identity.
    summary = iceberg_table.metadata.snapshots[0].summary
    assert summary["daft.checkpoint-store"] == checkpoint_store.path
    snapshot_query_id = summary["daft.checkpoint-query"]
    assert snapshot_query_id == crashed_query_id, (
        "recovery must reuse the crashed run's query_id, not a freshly-generated one"
    )
    assert re.match(r"^[a-z]+-[a-z]+-[0-9a-f]{6}$", snapshot_query_id), (
        f"unexpected query_id shape (caught empty/default failure mode?): {snapshot_query_id!r}"
    )

    # The recovered snapshot was committed by the *first* call (this scenario
    # restarts after a successful catalog commit); its summary therefore still
    # records the original 3 added rows / 1 added data file.
    assert summary.get("added-records") == "3"
    assert summary.get("added-data-files") == "1"

    # Verify: the input rows actually round-trip through the table — guards
    # against a snapshot landing with zero data files (which would still
    # satisfy the snapshot-count assertion above).
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_recovery_after_crash_between_stage_and_commit(iceberg_table, parquet_input, checkpoint_store):
    """Scenario B: crash after stage_files+checkpoint, before tx.commit_transaction.

    Files exist on object store, file metadata is in the checkpoint store, status
    is `Checkpointed`. The catalog has no snapshot. A fresh call to
    `write_iceberg(checkpoint=...)` must skip `write_df.collect()`, pull files
    out of the store, and commit exactly one snapshot using the original run's
    query_id (no fresh generation).
    """
    # First call: simulate the crash by patching tx.commit_transaction to raise.
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(Transaction, "commit_transaction", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # State: catalog has zero snapshots (commit didn't land); store has Checkpointed
    # entries with files staged (sink sealed before the commit attempt).
    iceberg_table.refresh()
    assert iceberg_table.metadata.snapshots == [], "no snapshot should land when commit raises"
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"
    crashed_query_id = pending_after_crash[0].query_id
    assert checkpoint_store.get_checkpointed_files(), "expected staged file metadata in store"

    # Second call: should skip the pipeline, pull files from the store, commit once.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # Verify: exactly one snapshot landed.
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1

    # Verify: previously-Checkpointed entries are now Committed.
    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed, "all entries should be Committed after recovery"

    # Verify: the snapshot's query_id matches what was staged before the crash —
    # i.e. the recovery used the existing entries' query_id, not a freshly
    # generated one. This is the load-bearing single-query-id-across-restart
    # invariant.
    summary = iceberg_table.metadata.snapshots[0].summary
    assert summary["daft.checkpoint-store"] == checkpoint_store.path
    assert summary["daft.checkpoint-query"] == crashed_query_id

    # The recovery commit pulled the staged files out of the checkpoint store
    # (no pipeline re-run); a regression that lands a snapshot with markers but
    # zero data would pass everything above. This assertion makes that explicit.
    assert summary.get("added-records") == "3"
    assert summary.get("added-data-files") == "1"

    # Verify: rows round-trip through the table.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_fresh_run_lands_snapshot_with_markers_and_data(iceberg_table, parquet_input, checkpoint_store):
    """Scenario C: fresh run with checkpoint, no crash. The most-traveled path.

    Pipeline runs, store gets populated, snapshot lands with markers,
    `mark_committed` runs. All input rows round-trip through the table.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1

    # Markers present and well-shaped.
    summary = iceberg_table.metadata.snapshots[0].summary
    assert summary["daft.checkpoint-store"] == checkpoint_store.path
    qid = summary["daft.checkpoint-query"]
    assert re.match(r"^[a-z]+-[a-z]+-[0-9a-f]{6}$", qid), f"unexpected query_id shape: {qid!r}"

    # All entries Committed (no leftover Checkpointed state).
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    # Data round-trip.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]
    assert sorted(rows["x"]) == [1, 2, 3]


def test_incremental_writes_dedupe_committed_keys(iceberg_table, tmpdir, checkpoint_store):
    """Two successive write_iceberg calls; second call's input is a superset.

    The source filter drops keys already Committed by the first call, so
    snapshot 2 only contains the *new* rows. Each call lands its own
    snapshot with a distinct query_id; the final table is the input union.
    """
    # Call 1: input {a, b, c}.
    inp1 = str(tmpdir / "in1")
    os.makedirs(inp1, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}).write_parquet(inp1)
    daft.read_parquet(inp1, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    qid_1 = iceberg_table.metadata.snapshots[0].summary["daft.checkpoint-query"]

    # Call 2: input {a, b, c, d, e, f} — superset. Source filter must drop
    # {a, b, c}; only {d, e, f} should flow through to a new snapshot.
    inp2 = str(tmpdir / "in2")
    os.makedirs(inp2, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c", "d", "e", "f"], "x": [1, 2, 3, 4, 5, 6]}).write_parquet(inp2)
    daft.read_parquet(inp2, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 2
    summary_1 = iceberg_table.metadata.snapshots[0].summary
    summary_2 = iceberg_table.metadata.snapshots[-1].summary
    qid_2 = summary_2["daft.checkpoint-query"]
    assert qid_1 != qid_2, "each execution must use its own query_id"

    # Snapshot 1's marker must be unchanged after the second call lands —
    # nothing in the second-call code path should rewrite earlier snapshot
    # summaries.
    assert summary_1["daft.checkpoint-query"] == qid_1

    # Snapshot 2 must contain only the new rows ({d, e, f}). The summary's
    # `added-records` / `added-data-files` fields are populated by pyiceberg's
    # fast_append; if the source filter failed to drop {a, b, c} we would see
    # 6 added records here instead of 3.
    assert summary_2.get("added-records") == "3", (
        f"snapshot 2 must add only the new rows; got summary: {dict(summary_2)}"
    )
    assert summary_2.get("added-data-files") == "1"

    # All entries Committed across both runs.
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    # Final table is the union; no duplicates of {a, b, c}.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_rerun_after_full_commit_is_noop(iceberg_table, parquet_input, checkpoint_store):
    """Re-running write_iceberg with already-Committed input is a no-op.

    Call 1 commits everything. Call 2 with the same input: source filter
    drops every key, the pipeline produces zero pending entries, no second
    snapshot lands, and the helper returns an empty result.
    """
    df1 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df1.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1

    # Re-run with identical input — should be a no-op.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    result = df2.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1, "no second snapshot should land on a no-op re-run"

    # The result DataFrame for an empty short-circuit is empty.
    assert result.to_pydict() == {"operation": [], "rows": [], "file_size": [], "file_name": []}


def test_overwrite_with_checkpoint_raises(iceberg_table, checkpoint_store):
    """`mode='overwrite'` + checkpoint is unsupported; must raise NotImplementedError."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(NotImplementedError, match="overwrite"):
        df.write_iceberg(iceberg_table, mode="overwrite", checkpoint=checkpoint_store)


@pytest.mark.parametrize(
    "reserved_key",
    ["daft.checkpoint-store", "daft.checkpoint-query", "daft.checkpoint-foo"],
)
def test_reserved_snapshot_property_key_raises(iceberg_table, checkpoint_store, reserved_key):
    """Any user-provided `snapshot_properties` key prefixed `daft.checkpoint-` is reserved."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="reserved"):
        df.write_iceberg(
            iceberg_table,
            checkpoint=checkpoint_store,
            snapshot_properties={reserved_key: "spoofed"},
        )


def test_user_snapshot_properties_pass_through(iceberg_table, parquet_input, checkpoint_store):
    """Non-reserved user snapshot_properties must reach the snapshot summary alongside daft markers."""
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(
        iceberg_table,
        checkpoint=checkpoint_store,
        snapshot_properties={"author": "rohit", "release": "v1"},
    )

    iceberg_table.refresh()
    summary = iceberg_table.metadata.snapshots[0].summary
    # User props preserved.
    assert summary["author"] == "rohit"
    assert summary["release"] == "v1"
    # Daft markers also injected — user props don't displace them.
    assert summary["daft.checkpoint-store"] == checkpoint_store.path
    assert summary.get("daft.checkpoint-query") is not None


def test_retry_loop_recovers_from_transient_commit_failure(iceberg_table, parquet_input, checkpoint_store):
    """The defensive retry loop must recover from a transient commit failure.

    Patch `Transaction.commit_transaction` so the first invocation raises
    `CommitFailedException` and the second calls through to the real method.
    The helper's retry loop should swallow attempt 1's failure, loop, and
    commit successfully on attempt 2 — yielding exactly one snapshot.
    """
    original_commit = Transaction.commit_transaction
    call_count = {"n": 0}

    def fail_first_then_succeed(self, *args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise CommitFailedException("simulated transient catalog conflict")
        return original_commit(self, *args, **kwargs)

    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(Transaction, "commit_transaction", fail_first_then_succeed):
        df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # The retry loop must have actually fired — otherwise the test is vacuous.
    assert call_count["n"] >= 2, "expected at least two commit attempts"

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1, "exactly one snapshot must land despite the transient failure"
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending, "all entries should be Committed after a successful retry"

    summary = iceberg_table.metadata.snapshots[0].summary
    assert summary["daft.checkpoint-store"] == checkpoint_store.path
    assert summary.get("added-records") == "3"

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_table_with_pre_existing_legacy_snapshots(iceberg_table, parquet_input, checkpoint_store):
    """Adopting checkpoint=... on a table that already has snapshots without markers.

    The migration path for every existing user. The recovery walk must not
    false-positive on a legacy snapshot (which has no `daft.checkpoint-*` keys),
    and the new write must land as a fresh snapshot on top of the existing one.
    """
    # Pre-populate the table with a plain non-checkpoint write — produces a
    # legacy snapshot whose summary has no `daft.checkpoint-*` markers.
    legacy = daft.from_pydict({"file_id": ["x", "y"], "x": [10, 20]})
    legacy.write_iceberg(iceberg_table)
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    legacy_summary = iceberg_table.metadata.snapshots[0].summary
    assert legacy_summary.get("daft.checkpoint-store") is None, "legacy snapshot must not carry our markers"

    # Now enable checkpoint and write new data. Recovery walks history, sees
    # the legacy snapshot with no matching markers, falls through to commit.
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 2, (
        "checkpoint write must land as a new snapshot on top of the legacy one"
    )

    new_summary = iceberg_table.metadata.snapshots[-1].summary
    assert new_summary["daft.checkpoint-store"] == checkpoint_store.path
    assert new_summary.get("added-records") == "3"

    # Final table is the union: legacy {x, y} + new {a, b, c}.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "x", "y"]


def test_empty_input_on_fresh_store(iceberg_table, tmpdir, checkpoint_store):
    """Writing an empty DataFrame on a fresh store is a clean no-op.

    A scheduled pipeline whose upstream produced zero rows is normal. The
    feature must not crash (e.g. by sealing an empty checkpoint entry that
    was never staged), nor land a polluted empty snapshot.
    """
    import pyarrow as pa

    empty_path = str(tmpdir / "empty_in")
    os.makedirs(empty_path, exist_ok=True)
    # Write a parquet with the right schema but zero rows.
    daft.from_pydict(
        {
            "file_id": pa.array([], type=pa.string()),
            "x": pa.array([], type=pa.int64()),
        }
    ).write_parquet(empty_path)

    df = daft.read_parquet(empty_path, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert iceberg_table.metadata.snapshots == [], "empty input must not land a snapshot"
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending, "no Checkpointed entries should remain after an empty write"


def test_first_write_wins_on_key_collisions(iceberg_table, tmpdir, checkpoint_store):
    """The `on=` column is checkpoint identity, not a primary key.

    Once a key is committed, subsequent rows with the same key are dropped by
    the source filter — even if the new row's other columns differ. This
    codifies first-write-wins semantics so a regression toward upsert-style
    behavior would fail loudly.

    Also covers duplicates within a single input: both occurrences land on
    the first call (no within-input dedup), then both are dropped on re-run.
    """
    # Call 1: input has a duplicate key 'a'. Both rows must land.
    inp1 = str(tmpdir / "in1")
    os.makedirs(inp1, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "a", "b"], "x": [1, 2, 3]}).write_parquet(inp1)
    daft.read_parquet(inp1, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )
    iceberg_table.refresh()
    rows_1 = daft.read_iceberg(iceberg_table).to_pydict()
    pairs_1 = sorted(zip(rows_1["file_id"], rows_1["x"]))
    assert pairs_1 == [("a", 1), ("a", 2), ("b", 3)], "duplicates within one input must both land"

    # Call 2: same keys 'a' and 'b' with DIFFERENT data, plus a new key 'c'.
    # First-write-wins: a's and b's new x values are dropped, only 'c' lands.
    inp2 = str(tmpdir / "in2")
    os.makedirs(inp2, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "a", "b", "c"], "x": [100, 200, 300, 4]}).write_parquet(inp2)
    daft.read_parquet(inp2, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 2

    # Final table: original a, a, b rows preserved with their *first-write*
    # x values; only c is new. A regression to upsert-semantics would replace
    # the a rows with x=100, x=200.
    rows_2 = daft.read_iceberg(iceberg_table).to_pydict()
    pairs_2 = sorted(zip(rows_2["file_id"], rows_2["x"]))
    assert pairs_2 == [("a", 1), ("a", 2), ("b", 3), ("c", 4)], f"first-write-wins violated; got {pairs_2}"


def test_multi_partition_input_aggregates_into_single_snapshot(iceberg_table, tmpdir, checkpoint_store):
    """Multi-partition input must aggregate per-partition entries into one snapshot.

    A multi-partition input produces multiple `Checkpointed` entries — one
    per input partition. A single `write_iceberg` call must aggregate files
    across all of them into one snapshot, mark every entry Committed, and
    preserve the single-query-id invariant across all entries.

    The fixed 3-row / 1-data-file shape every other test uses makes the
    "iterate all entries" code paths trivially correct. This test exercises
    the aggregation explicitly by writing three separate parquet files into
    the input directory; `daft.read_parquet` then sees three parquet files
    and the Ray pipeline distributes them across input partitions.
    """
    inp_dir = str(tmpdir / "multi_in")
    os.makedirs(inp_dir, exist_ok=True)
    parts = [
        (["a", "b"], [1, 2]),
        (["c", "d"], [3, 4]),
        (["e", "f"], [5, 6]),
    ]
    for i, (file_ids, xs) in enumerate(parts):
        daft.from_pydict({"file_id": file_ids, "x": xs}).write_parquet(f"{inp_dir}/part_{i}")

    daft.read_parquet(
        f"{inp_dir}/**", checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")
    ).write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1, "all per-partition entries must aggregate into a single snapshot"

    summary = iceberg_table.metadata.snapshots[0].summary
    # All six rows must land. `added-data-files` should be > 1 — that's the
    # whole point of this test; if the helper only committed files from the
    # first entry it iterated, we'd see fewer files than partitions.
    assert int(summary["added-records"]) == 6, (
        f"all rows from all partitions must be committed; summary={dict(summary)}"
    )
    assert int(summary["added-data-files"]) > 1, (
        f"expected multiple data files for a multi-partition input; summary={dict(summary)}"
    )

    # Every entry the pipeline staged must transition to Committed — a regression
    # that marked only the first id Committed would leave the rest pending.
    all_entries = list(checkpoint_store.list_checkpoints())
    assert len(all_entries) > 1, "expected multiple per-partition checkpoint entries"
    pending = [c for c in all_entries if c.status == CheckpointStatus.Checkpointed]
    assert not pending, "every per-partition entry must be Committed after the write"

    # Single-query-id invariant must hold across all entries — they were all
    # staged by the same execution, so they all share the same query_id.
    query_ids = {c.query_id for c in all_entries}
    assert len(query_ids) == 1, f"all entries from one execution must share one query_id; got {query_ids}"

    # Data round-trip across the union of partitions.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def _write_multi_partition_input(parent_dir: str, parts: list[tuple[list[str], list[int]]]) -> str:
    """Write each (file_ids, xs) pair as its own parquet file under `parent_dir`.

    Returns a glob path suitable for `daft.read_parquet` that picks up every
    written file as a separate input partition.
    """
    os.makedirs(parent_dir, exist_ok=True)
    for i, (file_ids, xs) in enumerate(parts):
        daft.from_pydict({"file_id": file_ids, "x": xs}).write_parquet(f"{parent_dir}/part_{i}")
    return f"{parent_dir}/**"


def test_multi_partition_recovery_after_crash_between_stage_and_commit(iceberg_table, tmpdir, checkpoint_store):
    """Scenario B with multi-partition input.

    The single-partition Scenario B test exercises a single Checkpointed entry
    on the recovery path. With multiple per-partition entries, the helper must
    iterate every blob in `get_checkpointed_files()`, decode each one, and
    aggregate the resulting DataFiles into one commit. A regression that
    decoded only the first blob, or marked only the first id Committed, would
    leak files and pending entries.
    """
    inp_glob = _write_multi_partition_input(
        str(tmpdir / "multi_in"),
        [(["a", "b"], [1, 2]), (["c", "d"], [3, 4]), (["e", "f"], [5, 6])],
    )

    # First call: crash between sink seal and tx.commit_transaction. Multiple
    # per-partition entries get sealed before the patched commit raises.
    df = daft.read_parquet(inp_glob, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(Transaction, "commit_transaction", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert iceberg_table.metadata.snapshots == [], "no snapshot must land when commit raises"
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert len(pending) > 1, (
        f"expected multiple per-partition Checkpointed entries to remain after crash; got {len(pending)}"
    )
    crashed_query_id = pending[0].query_id
    # All staged entries must share the run's query_id (the invariant the
    # recovery path relies on to read it from any single entry).
    assert all(c.query_id == crashed_query_id for c in pending), (
        "single-query-id invariant must hold across all per-partition entries"
    )

    # Second call: recovery aggregates files across all staged entries.
    daft.read_parquet(inp_glob, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    summary = iceberg_table.metadata.snapshots[0].summary
    assert summary["daft.checkpoint-store"] == checkpoint_store.path
    assert summary["daft.checkpoint-query"] == crashed_query_id
    assert int(summary["added-records"]) == 6, f"recovery must aggregate every staged entry; summary={dict(summary)}"
    assert int(summary["added-data-files"]) > 1, "recovery must commit data files from every per-partition entry"

    # Every per-partition entry must transition to Committed.
    still_pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_pending

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_multi_partition_incremental_writes(iceberg_table, tmpdir, checkpoint_store):
    """Incremental write with multi-partition inputs on both calls.

    Realistic large-ingest pattern: each call ingests many parquet files at
    once, and the user re-runs with an overlapping superset. The source
    filter must drop the prior call's keys per-partition, the helper must
    aggregate per-partition entries into call 2's snapshot, and the two calls
    must produce two snapshots with distinct query_ids.
    """
    # Call 1: input split across 2 parquet files → 2 partitions, all 4 rows land.
    inp1_glob = _write_multi_partition_input(
        str(tmpdir / "in1"),
        [(["a", "b"], [1, 2]), (["c", "d"], [3, 4])],
    )
    daft.read_parquet(inp1_glob, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    summary_1 = iceberg_table.metadata.snapshots[0].summary
    qid_1 = summary_1["daft.checkpoint-query"]
    assert int(summary_1["added-records"]) == 4
    assert int(summary_1["added-data-files"]) > 1, "call 1 must produce multi-file output"

    # Call 2: superset across 3 parquet files. Two of the partitions overlap
    # with call 1's keys, one is entirely new — source filter must drop the
    # overlap, only the new rows land.
    inp2_glob = _write_multi_partition_input(
        str(tmpdir / "in2"),
        [
            (["a", "b", "g"], [1, 2, 7]),  # 'a','b' overlap; 'g' new
            (["c", "d", "h"], [3, 4, 8]),  # 'c','d' overlap; 'h' new
            (["i", "j"], [9, 10]),  # entirely new partition
        ],
    )
    daft.read_parquet(inp2_glob, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 2
    summary_2 = iceberg_table.metadata.snapshots[-1].summary
    qid_2 = summary_2["daft.checkpoint-query"]
    assert qid_1 != qid_2, "each call must have its own query_id"

    # Snapshot 2 must contain only the four new rows. Filter dedup must work
    # per-partition: each of call 2's partitions had overlap with call 1, but
    # only the new keys (g, h, i, j) get committed.
    assert int(summary_2["added-records"]) == 4, f"snapshot 2 must add only the new rows; summary={dict(summary_2)}"

    # Final table is the union; no duplicates of {a, b, c, d}.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "g", "h", "i", "j"]


def test_null_keys_are_deduped(iceberg_table, tmpdir, checkpoint_store):
    """NULL values in the `on=` column are deduped on re-run.

    Daft's anti-join uses NULL-equals-NULL semantics (not SQL's NULL != NULL),
    so a NULL key gets recorded in the checkpoint store on the first run and
    matched against on re-runs — same as any other value. This test pins the
    behavior so a regression toward SQL-style NULL handling would fail.
    """
    import pyarrow as pa

    inp = str(tmpdir / "null_in")
    os.makedirs(inp, exist_ok=True)
    # Two non-null keys plus one NULL.
    daft.from_pydict(
        {
            "file_id": pa.array(["a", "b", None], type=pa.string()),
            "x": pa.array([1, 2, 3], type=pa.int64()),
        }
    ).write_parquet(inp)

    # Call 1: all three rows land.
    daft.read_parquet(inp, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )
    iceberg_table.refresh()
    rows_1 = daft.read_iceberg(iceberg_table).to_pydict()
    assert len(rows_1["file_id"]) == 3
    assert sum(1 for fid in rows_1["file_id"] if fid is None) == 1, "first run lands the single NULL-keyed row"

    # Call 2: same input. Filter must drop the NULL row too — same dedup as
    # for non-null keys.
    daft.read_parquet(inp, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=checkpoint_store
    )
    iceberg_table.refresh()
    rows_2 = daft.read_iceberg(iceberg_table).to_pydict()
    assert len(rows_2["file_id"]) == 3, f"re-run with same input must not duplicate NULL-keyed rows; got {rows_2}"
    assert sum(1 for fid in rows_2["file_id"] if fid is None) == 1, "NULL key must be deduped on re-run"
