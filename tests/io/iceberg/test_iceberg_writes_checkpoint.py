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
    df = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
    with patch.object(checkpoint_store, "mark_committed", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # State: catalog has exactly one snapshot with our markers; store entries are
    # still in Checkpointed state because mark_committed never ran.
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"
    crashed_query_id = pending_after_crash[0].query_id

    # Second call: should hit the recovery path. mark_committed is no longer patched.
    df2 = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
    df2.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    # Verify: still exactly one snapshot, no double-write.
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1, "recovery must not produce a second snapshot"

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
    df = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
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
    df2 = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
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
    df = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
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
    daft.read_parquet(inp1, checkpoint=checkpoint_store, on="file_id").write_iceberg(
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
    daft.read_parquet(inp2, checkpoint=checkpoint_store, on="file_id").write_iceberg(
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
    df1 = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
    df1.write_iceberg(iceberg_table, checkpoint=checkpoint_store)

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1

    # Re-run with identical input — should be a no-op.
    df2 = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
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
    df = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
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

    df = daft.read_parquet(parquet_input, checkpoint=checkpoint_store, on="file_id")
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
