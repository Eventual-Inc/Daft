"""Tests for `write_iceberg(checkpoint=daft.IdempotentCommit(...))`.

Idempotence is keyed on the ``idempotence_key`` carried by the
:class:`daft.IdempotentCommit` passed via ``checkpoint=`` on
``write_iceberg``. Each logical commit declares its own key; retries of
the same commit reuse the key. Recovery walks the Iceberg snapshot
history for ``daft.idempotence-key`` matching the call's key.

Single-snapshot invariant — every test asserts that a logical run lands
exactly one new Iceberg snapshot tagged with the call's idempotence key.

The checkpoint-aware source filter (``daft.read_parquet(checkpoint=...)``)
runs only on the Ray runner, so these tests are skipped on the native
runner. See ``tests/checkpoint/test_native_runner_gate.py``.

Optional: set ``CHECKPOINTING_TEST_BUCKET`` (and ensure ``AWS_REGION`` /
AWS auth are exported) to route fixtures through real S3 instead of the
local filesystem. CI defaults to local; S3 is opt-in.
"""

from __future__ import annotations

import os
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
from tests.io._s3_helpers import S3_BUCKET, S3_REGION, s3_io_config, s3_uri

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="checkpoint+write_iceberg requires Ray runner",
)


IDEMPOTENCE_KEY = "test-run-2026-05-04"


@pytest.fixture(scope="function")
def local_catalog(tmpdir):
    if S3_BUCKET:
        catalog = SqlCatalog(
            "default",
            uri=f"sqlite:///{tmpdir}/pyiceberg_catalog.db",
            warehouse=s3_uri("iceberg", "warehouse"),
            **{"s3.region": S3_REGION},
        )
    else:
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
def partitioned_iceberg_table(local_catalog):
    """Identity-partitioned on ``file_id`` — one partition per distinct key."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform

    schema = Schema(
        NestedField(field_id=1, name="file_id", type=StringType()),
        NestedField(field_id=2, name="x", type=LongType()),
    )
    partition_spec = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="file_id")
    )
    return local_catalog.create_table("default.partitioned_idempotent_test", schema, partition_spec=partition_spec)


@pytest.fixture(scope="function")
def parquet_input(tmpdir):
    """A small parquet input directory we can read with checkpoint=."""
    df = daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]})
    if S3_BUCKET:
        path = s3_uri("iceberg", "input")
        df.write_parquet(path, io_config=s3_io_config())
        return path
    path = str(tmpdir / "input")
    os.makedirs(path, exist_ok=True)
    df.write_parquet(path)
    return path


@pytest.fixture(scope="function")
def checkpoint_store(tmpdir):
    if S3_BUCKET:
        return daft.CheckpointStore(s3_uri("iceberg", "ckpt"), io_config=s3_io_config())
    return daft.CheckpointStore(f"file://{tmpdir}/ckpt")


@pytest.fixture(scope="function")
def idempotent_commit(checkpoint_store):
    return daft.IdempotentCommit(store=checkpoint_store, idempotence_key=IDEMPOTENCE_KEY)


def _assert_single_snapshot_with_key(table, key: str) -> dict[str, str]:
    """Assert exactly one snapshot exists tagged with the given idempotence key.

    Returns the snapshot summary so the caller can make further assertions.
    """
    table.refresh()
    snapshots = list(table.metadata.snapshots)
    assert len(snapshots) == 1, f"single-snapshot invariant violated: expected exactly 1 snapshot, got {len(snapshots)}"
    summary = snapshots[0].summary
    assert summary["daft.idempotence-key"] == key, (
        f"snapshot must be tagged with idempotence key {key!r}; got summary={dict(summary)}"
    )
    return summary


def test_fresh_run_lands_single_snapshot_with_key(iceberg_table, parquet_input, checkpoint_store, idempotent_commit):
    """Fresh run, no crash. The most-traveled path.

    Pipeline runs, store gets populated, snapshot lands tagged with the
    idempotence key, ``mark_committed`` runs, all input rows round-trip.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert summary.get("added-records") == "3"

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]
    assert sorted(rows["x"]) == [1, 2, 3]


def test_recovery_after_crash_between_commit_and_mark(
    iceberg_table, parquet_input, checkpoint_store, idempotent_commit
):
    """Scenario A: crash after commit succeeded but before mark_committed ran.

    Iceberg has the snapshot tagged with our key; store still says Checkpointed.
    Second call must walk history, find the key, mark Checkpointed → Committed,
    and NOT produce a second snapshot — and not run the pipeline.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(checkpoint_store, "mark_committed", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    snapshot_id_after_crash = iceberg_table.metadata.snapshots[0].snapshot_id
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"

    # Second call: recovery path. The pipeline must NOT run.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)

    # Same snapshot — recovery did not retire and replace.
    assert iceberg_table.metadata.snapshots[0].snapshot_id == snapshot_id_after_crash, (
        "recovery must keep the original snapshot, not replace it"
    )

    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed, "all entries should be Committed after recovery"

    assert summary.get("added-records") == "3"
    assert summary.get("added-data-files") == "1"

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_recovery_after_crash_between_stage_and_commit(
    iceberg_table, parquet_input, checkpoint_store, idempotent_commit
):
    """Scenario B: crash after stage_files+checkpoint, before tx.commit_transaction.

    Files staged in store, no snapshot in catalog. Second call sees no
    matching snapshot, runs the pipeline (filter skips Checkpointed), pulls
    files from the store, commits exactly once tagged with the idempotence key.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(Transaction, "commit_transaction", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    iceberg_table.refresh()
    assert iceberg_table.metadata.snapshots == [], "no snapshot must land when commit raises"
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"

    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert summary.get("added-records") == "3"
    assert summary.get("added-data-files") == "1"

    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_idempotent_rerun_with_same_key_is_noop(iceberg_table, parquet_input, checkpoint_store, idempotent_commit):
    """Re-running with the same key on an already-committed table is a no-op.

    First call commits. Second call's check-first path finds the snapshot
    with our key and bails — no second snapshot, no pipeline run.
    """
    df1 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df1.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)

    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    result = df2.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert result.to_pydict() == {"operation": [], "rows": [], "file_size": [], "file_name": []}


def test_overwrite_with_checkpoint_raises(iceberg_table, idempotent_commit):
    """`mode='overwrite'` + checkpoint is unsupported."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(NotImplementedError, match="overwrite"):
        df.write_iceberg(iceberg_table, mode="overwrite", checkpoint=idempotent_commit)


@pytest.mark.parametrize(
    "reserved_key",
    ["daft.idempotence-key", "daft.idempotence-foo"],
)
def test_reserved_snapshot_property_key_raises(iceberg_table, idempotent_commit, reserved_key):
    """User-provided `snapshot_properties` keys prefixed `daft.idempotence-` are reserved."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="reserved"):
        df.write_iceberg(
            iceberg_table,
            checkpoint=idempotent_commit,
            snapshot_properties={reserved_key: "spoofed"},
        )


@pytest.mark.parametrize(
    "reserved_key",
    ["daft.idempotence-key", "daft.idempotence-foo"],
)
def test_reserved_snapshot_property_key_raises_without_checkpoint(iceberg_table, reserved_key):
    """The `daft.idempotence-` prefix is reserved regardless of `checkpoint=`.

    A future user who lands a snapshot tagged with `daft.idempotence-key`
    via `snapshot_properties`, then later switches to `checkpoint=...`,
    would otherwise see "recovery silently misses my prior write". Always
    reserving the prefix makes this impossible.
    """
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="reserved"):
        df.write_iceberg(
            iceberg_table,
            snapshot_properties={reserved_key: "spoofed"},
        )


def test_user_snapshot_properties_pass_through(iceberg_table, parquet_input, checkpoint_store, idempotent_commit):
    """Non-reserved user snapshot_properties reach the snapshot summary alongside the marker."""
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(
        iceberg_table,
        checkpoint=idempotent_commit,
        snapshot_properties={"author": "rohit", "release": "v1"},
    )

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert summary["author"] == "rohit"
    assert summary["release"] == "v1"


def test_partitioned_table_result_includes_partitioning_column(
    partitioned_iceberg_table, parquet_input, checkpoint_store, idempotent_commit
):
    """Result includes ``partitioning`` struct column on partitioned tables.

    Same shape as non-checkpoint ``write_iceberg``. Regression for C1 from
    code review: an earlier version of
    ``_write_iceberg_with_checkpoint._build_result()`` omitted partitioning,
    silently dropping the column when users flipped ``checkpoint=`` on a
    partitioned table.

    Pins schema-level presence only. Value-level correctness of partition
    extraction is inherited from the non-checkpoint path's ``getattr``
    against ``data_file.partition`` and is a pyiceberg-internals concern.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    result = df.write_iceberg(partitioned_iceberg_table, checkpoint=idempotent_commit)

    cols = set(result.schema().column_names())
    assert {"operation", "rows", "file_size", "file_name", "partitioning"} <= cols, (
        f"expected partitioned-table result columns to include `partitioning`; got {cols}"
    )


def test_partitioned_table_recovery_branch_result_includes_partitioning_column(
    partitioned_iceberg_table, parquet_input, checkpoint_store, idempotent_commit
):
    """Recovery-branch empty result has the same schema as the populated branch.

    Regression for CC1 from code review: the recovery short-circuit used to
    call the shared ``empty_write_result()`` which always returned 4 columns,
    while the populated branch returns 5 cols (with ``partitioning``) on a
    partitioned table. ``pa.concat`` over runs would break on the mismatch.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    populated = df.write_iceberg(partitioned_iceberg_table, checkpoint=idempotent_commit)

    # Second call with same key → step 1 marker-found short-circuit → empty result.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    empty = df2.write_iceberg(partitioned_iceberg_table, checkpoint=idempotent_commit)

    assert empty.schema().column_names() == populated.schema().column_names(), (
        f"recovery-branch result schema must match populated-branch on partitioned tables; "
        f"got empty={empty.schema().column_names()} vs populated={populated.schema().column_names()}"
    )
    assert empty.count_rows() == 0


def test_retry_loop_recovers_from_transient_commit_failure(
    iceberg_table, parquet_input, checkpoint_store, idempotent_commit
):
    """Defensive retry loop recovers from a transient commit failure."""
    original_commit = Transaction.commit_transaction
    call_count = {"n": 0}

    def fail_first_then_succeed(self, *args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise CommitFailedException("simulated transient catalog conflict")
        return original_commit(self, *args, **kwargs)

    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(Transaction, "commit_transaction", fail_first_then_succeed):
        df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    assert call_count["n"] >= 2

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert summary.get("added-records") == "3"

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_table_with_pre_existing_legacy_snapshots(iceberg_table, parquet_input, checkpoint_store, idempotent_commit):
    """Adopting checkpoint=... on a table with pre-existing snapshots without the marker.

    Recovery walk must not false-positive on legacy snapshots; new write
    lands as a fresh snapshot on top.
    """
    legacy = daft.from_pydict({"file_id": ["x", "y"], "x": [10, 20]})
    legacy.write_iceberg(iceberg_table)
    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    legacy_summary = iceberg_table.metadata.snapshots[0].summary
    assert legacy_summary.get("daft.idempotence-key") is None, "legacy snapshot must not carry our marker"

    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    iceberg_table.refresh()
    snapshots = list(iceberg_table.metadata.snapshots)
    assert len(snapshots) == 2, "checkpoint write must land as a new snapshot on top of the legacy one"

    new_summary = snapshots[-1].summary
    assert new_summary["daft.idempotence-key"] == IDEMPOTENCE_KEY
    assert new_summary.get("added-records") == "3"

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "x", "y"]


def test_empty_input_on_fresh_store(iceberg_table, tmpdir, checkpoint_store, idempotent_commit):
    """Empty input — no Checkpointed entries, no snapshot lands."""
    import pyarrow as pa

    empty_path = str(tmpdir / "empty_in")
    os.makedirs(empty_path, exist_ok=True)
    daft.from_pydict(
        {
            "file_id": pa.array([], type=pa.string()),
            "x": pa.array([], type=pa.int64()),
        }
    ).write_parquet(empty_path)

    df = daft.read_parquet(empty_path, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    iceberg_table.refresh()
    assert iceberg_table.metadata.snapshots == [], "empty input must not land a snapshot"
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending


def test_multi_partition_aggregates_into_single_snapshot(iceberg_table, tmpdir, checkpoint_store, idempotent_commit):
    """Multi-partition input aggregates per-partition entries into one snapshot."""
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
    ).write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert int(summary["added-records"]) == 6
    assert int(summary["added-data-files"]) > 1

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_multi_partition_recovery_after_crash_between_stage_and_commit(
    iceberg_table, tmpdir, checkpoint_store, idempotent_commit
):
    """Scenario B with multi-partition input — recovery aggregates across all entries."""
    inp_dir = str(tmpdir / "multi_in")
    os.makedirs(inp_dir, exist_ok=True)
    parts = [(["a", "b"], [1, 2]), (["c", "d"], [3, 4]), (["e", "f"], [5, 6])]
    for i, (file_ids, xs) in enumerate(parts):
        daft.from_pydict({"file_id": file_ids, "x": xs}).write_parquet(f"{inp_dir}/part_{i}")

    df = daft.read_parquet(f"{inp_dir}/**", checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(Transaction, "commit_transaction", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    iceberg_table.refresh()
    assert iceberg_table.metadata.snapshots == []
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert len(pending) > 1

    daft.read_parquet(
        f"{inp_dir}/**", checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")
    ).write_iceberg(iceberg_table, checkpoint=idempotent_commit)

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert int(summary["added-records"]) == 6
    assert int(summary["added-data-files"]) > 1

    pending_after = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending_after

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_within_input_duplicate_keys_both_land(iceberg_table, tmpdir, checkpoint_store, idempotent_commit):
    """Duplicates within a single input are not deduped — both rows land."""
    inp = str(tmpdir / "dup_in")
    os.makedirs(inp, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "a", "b"], "x": [1, 2, 3]}).write_parquet(inp)
    daft.read_parquet(inp, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table, checkpoint=idempotent_commit
    )

    summary = _assert_single_snapshot_with_key(iceberg_table, IDEMPOTENCE_KEY)
    assert int(summary["added-records"]) == 3

    rows = daft.read_iceberg(iceberg_table).to_pydict()
    pairs = sorted(zip(rows["file_id"], rows["x"]))
    assert pairs == [("a", 1), ("a", 2), ("b", 3)]


def test_idempotent_commit_rejects_empty_key(checkpoint_store):
    """`IdempotentCommit` constructor rejects empty idempotence_key."""
    with pytest.raises(ValueError, match="non-empty"):
        daft.IdempotentCommit(store=checkpoint_store, idempotence_key="")


def test_incremental_writes_dedupe_committed_keys(iceberg_table, tmpdir, checkpoint_store):
    """Two successive write_iceberg calls with distinct keys against the same store.

    Different ``idempotence_key`` per call — each commits its own snapshot —
    but the source-side filter drops keys already Committed by the first
    call, so snapshot 2 only contains the *new* rows. Pins the source filter
    behavior across logical commits (this is *not* same-key dedup; it's
    different-key cross-call dedup via the source anti-join, which is a
    legitimate part of the feature surface).
    """
    # Call 1: input {a, b, c}.
    inp1 = str(tmpdir / "in1")
    os.makedirs(inp1, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}).write_parquet(inp1)
    daft.read_parquet(inp1, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table,
        checkpoint=daft.IdempotentCommit(store=checkpoint_store, idempotence_key="run-1"),
    )

    iceberg_table.refresh()
    assert len(iceberg_table.metadata.snapshots) == 1
    assert iceberg_table.metadata.snapshots[0].summary["daft.idempotence-key"] == "run-1"

    # Call 2: input {a, b, c, d, e, f} — superset.
    inp2 = str(tmpdir / "in2")
    os.makedirs(inp2, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c", "d", "e", "f"], "x": [1, 2, 3, 4, 5, 6]}).write_parquet(inp2)
    daft.read_parquet(inp2, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_iceberg(
        iceberg_table,
        checkpoint=daft.IdempotentCommit(store=checkpoint_store, idempotence_key="run-2"),
    )

    iceberg_table.refresh()
    snapshots = list(iceberg_table.metadata.snapshots)
    assert len(snapshots) == 2

    # Snapshot 1's marker unchanged.
    assert snapshots[0].summary["daft.idempotence-key"] == "run-1"
    # Snapshot 2 carries the new key.
    assert snapshots[-1].summary["daft.idempotence-key"] == "run-2"
    # Snapshot 2 only adds the *new* rows; the source filter drops {a,b,c}.
    assert int(snapshots[-1].summary["added-records"]) == 3
    assert int(snapshots[-1].summary["added-data-files"]) == 1

    # All entries Committed.
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    # Final table is the union; no duplicates.
    rows = daft.read_iceberg(iceberg_table).to_pydict()
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]
