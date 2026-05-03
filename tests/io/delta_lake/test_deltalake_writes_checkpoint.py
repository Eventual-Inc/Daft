"""Tests for `write_deltalake(checkpoint=...)` — idempotent commits.

The checkpoint-aware source filter (`daft.read_parquet(checkpoint=...)`)
runs only on the Ray runner, so these tests are skipped on the native
runner.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

import daft

deltalake = pytest.importorskip("deltalake")

from daft.daft import CheckpointStatus

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="checkpoint+write_deltalake requires Ray runner",
)


@pytest.fixture(scope="function")
def delta_table(tmpdir):
    """Create an empty Delta table with schema (file_id: string, x: int64)."""
    import pyarrow as pa

    schema = pa.schema([("file_id", pa.string()), ("x", pa.int64())])
    table_path = str(tmpdir / "delta_table")
    deltalake.write_deltalake(table_path, pa.table({"file_id": [], "x": []}).cast(schema))
    return deltalake.DeltaTable(table_path)


@pytest.fixture(scope="function")
def parquet_input(tmpdir):
    path = str(tmpdir / "input")
    os.makedirs(path, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}).write_parquet(path)
    return path


@pytest.fixture(scope="function")
def checkpoint_store(tmpdir):
    return daft.CheckpointStore(f"file://{tmpdir}/ckpt")


def _read_delta_rows(table_path: str) -> dict:
    return daft.read_deltalake(table_path).to_pydict()


def _find_checkpoint_marker(table: deltalake.DeltaTable, store_path: str, query_id: str) -> bool:
    """Check if any commit in the table's history carries our checkpoint markers."""
    for entry in table.history(limit=50):
        if (
            entry.get("daft.checkpoint-store") == store_path
            and entry.get("daft.checkpoint-query") == query_id
        ):
            return True
    return False


def test_fresh_run_lands_commit_with_markers_and_data(delta_table, parquet_input, checkpoint_store):
    """Happy path: fresh run with checkpoint, no crash."""
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_deltalake(delta_table, checkpoint=checkpoint_store)

    # All entries Committed (no leftover Checkpointed state).
    all_entries = list(checkpoint_store.list_checkpoints())
    assert all_entries, "checkpoint store should have entries after a write"
    pending = [c for c in all_entries if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    # Markers present in Delta history.
    delta_table.update_incremental()
    query_id = all_entries[0].query_id
    assert _find_checkpoint_marker(delta_table, checkpoint_store.path, query_id), (
        "commit info must carry daft.checkpoint-* markers"
    )

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]
    assert sorted(rows["x"]) == [1, 2, 3]


def test_recovery_after_crash_between_commit_and_mark(delta_table, parquet_input, checkpoint_store):
    """Crash after commit succeeded but before mark_committed ran.

    Delta log has the commit with markers; store still says `Checkpointed`.
    A fresh call to `write_deltalake(checkpoint=...)` must detect the marker,
    call `mark_committed`, and NOT produce a second commit.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(checkpoint_store, "mark_committed", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_deltalake(delta_table, checkpoint=checkpoint_store)

    delta_table.update_incremental()
    version_after_crash = delta_table.version()
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"

    # Second call: should hit the recovery path.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_deltalake(delta_table, checkpoint=checkpoint_store)

    delta_table.update_incremental()
    assert delta_table.version() == version_after_crash, "recovery must not produce a second commit"

    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed, "all entries should be Committed after recovery"

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_recovery_after_crash_between_stage_and_commit(delta_table, parquet_input, checkpoint_store):
    """Crash after stage_files+checkpoint, before the catalog commit.

    Files exist on disk, file metadata is in the checkpoint store, status
    is `Checkpointed`. Delta log has no commit from us. A fresh call must
    skip `write_df.collect()`, pull files from the store, and commit.
    """
    def crash_after_stage(write_df, checkpoint, **kwargs):
        from daft.daft import CheckpointStatus as CS

        pending = [c for c in checkpoint.list_checkpoints() if c.status == CS.Checkpointed]
        if not pending:
            write_df.collect()
        raise RuntimeError("simulated crash")

    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch("daft.dataframe._checkpoint_commit.commit_with_checkpoint", crash_after_stage):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_deltalake(delta_table, checkpoint=checkpoint_store)

    delta_table.update_incremental()
    version_after_crash = delta_table.version()
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"
    assert checkpoint_store.get_checkpointed_files(), "expected staged file metadata in store"

    # Second call: should skip the pipeline, pull files from the store, commit once.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_deltalake(delta_table, checkpoint=checkpoint_store)

    delta_table.update_incremental()
    assert delta_table.version() == version_after_crash + 1, "exactly one new commit should land"

    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed, "all entries should be Committed after recovery"

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_rerun_after_full_commit_is_noop(delta_table, parquet_input, checkpoint_store):
    """Re-running write_deltalake with already-Committed input is a no-op."""
    df1 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df1.write_deltalake(delta_table, checkpoint=checkpoint_store)

    delta_table.update_incremental()
    version_after_first = delta_table.version()

    # Re-run with identical input — should be a no-op.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    result = df2.write_deltalake(delta_table, checkpoint=checkpoint_store)

    delta_table.update_incremental()
    assert delta_table.version() == version_after_first, "no second commit should land on a no-op re-run"

    assert result.to_pydict() == {"operation": [], "rows": [], "file_size": [], "file_name": []}


def test_incremental_writes_dedupe_committed_keys(delta_table, tmpdir, checkpoint_store):
    """Two successive write_deltalake calls; second call's input is a superset.

    The source filter drops keys already Committed by the first call, so
    commit 2 only contains the *new* rows.
    """
    # Call 1: input {a, b, c}.
    inp1 = str(tmpdir / "in1")
    os.makedirs(inp1, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}).write_parquet(inp1)
    daft.read_parquet(inp1, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_deltalake(
        delta_table, checkpoint=checkpoint_store
    )

    delta_table.update_incremental()
    version_after_first = delta_table.version()

    # Call 2: input {a, b, c, d, e, f} — superset. Source filter must drop
    # {a, b, c}; only {d, e, f} should flow through.
    inp2 = str(tmpdir / "in2")
    os.makedirs(inp2, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c", "d", "e", "f"], "x": [1, 2, 3, 4, 5, 6]}).write_parquet(inp2)
    daft.read_parquet(inp2, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_deltalake(
        delta_table, checkpoint=checkpoint_store
    )

    delta_table.update_incremental()
    assert delta_table.version() == version_after_first + 1

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_overwrite_with_checkpoint_raises(delta_table, checkpoint_store):
    """`mode='overwrite'` + checkpoint is unsupported; must raise NotImplementedError."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(NotImplementedError, match="overwrite"):
        df.write_deltalake(delta_table, mode="overwrite", checkpoint=checkpoint_store)


@pytest.mark.parametrize(
    "reserved_key",
    ["daft.checkpoint-store", "daft.checkpoint-query", "daft.checkpoint-foo"],
)
def test_reserved_custom_metadata_key_raises(delta_table, checkpoint_store, reserved_key):
    """Any user-provided `custom_metadata` key prefixed `daft.checkpoint-` is reserved."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="reserved"):
        df.write_deltalake(
            delta_table,
            checkpoint=checkpoint_store,
            custom_metadata={reserved_key: "spoofed"},
        )
