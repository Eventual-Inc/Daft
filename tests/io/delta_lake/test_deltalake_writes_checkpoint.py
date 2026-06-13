"""Tests for ``write_deltalake(checkpoint=daft.IdempotentCommit(...))``.

Idempotence is keyed on the ``idempotence_key`` carried by the
:class:`daft.IdempotentCommit` passed via ``checkpoint=`` on
``write_deltalake``. Each logical commit declares its own key; retries of
the same commit reuse the key. Recovery walks Delta commit history for
``daft.idempotence-key`` matching the call's key.

Single-commit invariant — every test asserts that a logical run lands
exactly one new Delta commit tagged with the call's idempotence key.

The checkpoint-aware source filter (``daft.read_parquet(checkpoint=...)``)
runs only on the Ray runner, so these tests are skipped on the native
runner.

Optional: set ``CHECKPOINTING_TEST_BUCKET`` (and ensure ``AWS_REGION`` /
AWS auth are exported) to route fixtures through real S3 instead of the
local filesystem. CI defaults to local; S3 is opt-in.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

import daft

deltalake = pytest.importorskip("deltalake")

from daft.daft import CheckpointStatus
from tests.io._s3_helpers import S3_BUCKET, delta_storage_options, s3_io_config, s3_uri

pytestmark = pytest.mark.skipif(
    os.environ.get("DAFT_RUNNER") != "ray",
    reason="checkpoint+write_deltalake requires Ray runner",
)


IDEMPOTENCE_KEY = "test-run-2026-05-08"


@pytest.fixture(scope="function")
def delta_table_path(tmpdir):
    """Path for a Delta table that doesn't exist yet (fresh-table path)."""
    if S3_BUCKET:
        return s3_uri("delta", "delta_table")
    return str(tmpdir / "delta_table")


@pytest.fixture(scope="function")
def delta_table(delta_table_path):
    """Create an empty Delta table at delta_table_path with the test schema."""
    import pyarrow as pa

    schema = pa.schema([("file_id", pa.string()), ("x", pa.int64())])
    empty = pa.table({"file_id": [], "x": []}).cast(schema)
    if S3_BUCKET:
        opts = delta_storage_options()
        deltalake.write_deltalake(delta_table_path, empty, storage_options=opts)
        return deltalake.DeltaTable(delta_table_path, storage_options=opts)
    deltalake.write_deltalake(delta_table_path, empty)
    return deltalake.DeltaTable(delta_table_path)


@pytest.fixture(scope="function")
def parquet_input(tmpdir):
    """A small parquet input directory we can read with checkpoint=."""
    df = daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]})
    if S3_BUCKET:
        path = s3_uri("delta", "input")
        df.write_parquet(path, io_config=s3_io_config())
        return path
    path = str(tmpdir / "input")
    os.makedirs(path, exist_ok=True)
    df.write_parquet(path)
    return path


@pytest.fixture(scope="function")
def checkpoint_store(tmpdir):
    if S3_BUCKET:
        return daft.CheckpointStore(s3_uri("delta", "ckpt"), io_config=s3_io_config())
    return daft.CheckpointStore(f"file://{tmpdir}/ckpt")


@pytest.fixture(scope="function")
def idempotent_commit(checkpoint_store):
    return daft.IdempotentCommit(store=checkpoint_store, idempotence_key=IDEMPOTENCE_KEY)


def _read_delta_rows(table_path: str) -> dict:
    if S3_BUCKET:
        return daft.read_deltalake(table_path, io_config=s3_io_config()).to_pydict()
    return daft.read_deltalake(table_path).to_pydict()


def _make_crash_wrapper(real_table):
    """Build a wrapper around a RawDeltaTable that raises on commit attempts.

    PyO3's RawDeltaTable methods are read-only at the instance level, so
    ``patch.object(delta_table._table, "create_write_transaction", ...)``
    fails with ``AttributeError: ... is read-only``. The DeltaTable's
    ``_table`` attribute itself, however, is a plain Python instance
    attribute that we can swap out. This wrapper proxies all attribute
    access to the real table except ``create_write_transaction``, which
    raises — simulating a mid-commit crash.
    """

    class _CrashTableWrapper:
        def create_write_transaction(self, *args, **kwargs):
            raise RuntimeError("simulated crash")

        def __getattr__(self, name):
            return getattr(real_table, name)

    return _CrashTableWrapper()


def _commits_with_key(table: deltalake.DeltaTable, key: str) -> int:
    """Count commits in Delta history tagged with our idempotence key."""
    table.update_incremental()
    return sum(1 for entry in table.history(limit=50) if entry.get("daft.idempotence-key") == key)


def _assert_single_commit_with_key(table: deltalake.DeltaTable, key: str) -> dict:
    """Assert exactly one commit in history tagged with the given key.

    Returns the matching commit entry so callers can make further assertions.
    """
    table.update_incremental()
    matches = [entry for entry in table.history(limit=50) if entry.get("daft.idempotence-key") == key]
    assert len(matches) == 1, (
        f"single-commit invariant violated: expected exactly 1 commit tagged {key!r}, got {len(matches)}"
    )
    return matches[0]


def test_fresh_run_lands_single_commit_with_key(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """Fresh run with an existing (empty) Delta table — the most-traveled path."""
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_deltalake(delta_table, checkpoint=idempotent_commit)

    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]
    assert sorted(rows["x"]) == [1, 2, 3]


def test_fresh_table_path(delta_table_path, parquet_input, checkpoint_store, idempotent_commit):
    """Fresh-table path (table=None) — `create_table_with_add_actions` route.

    Pass a string URI rather than a constructed DeltaTable. The helper takes
    the create-fresh path. Result: one commit with our marker on a brand-new
    table.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_deltalake(delta_table_path, checkpoint=idempotent_commit)

    table = deltalake.DeltaTable(delta_table_path)
    _assert_single_commit_with_key(table, IDEMPOTENCE_KEY)

    rows = _read_delta_rows(delta_table_path)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_recovery_after_crash_between_commit_and_mark(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """Scenario A: crash post-commit, pre-mark.

    Delta log has the commit tagged with our key; store says Checkpointed.
    Second call must walk history, find the key, mark Committed, NOT produce
    a second commit, NOT run the pipeline.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    with patch.object(checkpoint_store, "mark_committed", side_effect=RuntimeError("simulated crash")):
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    version_after_crash = delta_table.version()
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash, "expected Checkpointed entries to remain after crash"

    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_deltalake(delta_table, checkpoint=idempotent_commit)

    # No new commit; the version unchanged.
    delta_table.update_incremental()
    assert delta_table.version() == version_after_crash, "recovery must not produce a second commit"
    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    # All Checkpointed entries graduated to Committed.
    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed


def test_recovery_after_crash_between_stage_and_commit(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """Scenario B: crash post-stage, pre-commit.

    Files staged in store, no commit landed. Second call sees no matching
    commit, runs pipeline (filter skips Checkpointed), pulls AddActions
    from store, commits exactly once tagged with the key.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))

    # PyO3's RawDeltaTable.create_write_transaction is read-only at the
    # instance level, so `patch.object` fails. Swap `_table` (a regular
    # instance attribute on DeltaTable) for a wrapper that raises on
    # create_write_transaction and proxies everything else.
    real_table = delta_table._table
    crash_wrapper = _make_crash_wrapper(real_table)
    delta_table._table = crash_wrapper
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_deltalake(delta_table, checkpoint=idempotent_commit)
    finally:
        delta_table._table = real_table

    # State after crash: no commit landed but pending Checkpointed entries exist.
    delta_table.update_incremental()
    version_pre_crash = delta_table.version()
    pending_after_crash = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert pending_after_crash

    # Second call lands the commit cleanly.
    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df2.write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    assert delta_table.version() == version_pre_crash + 1
    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    still_checkpointed = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not still_checkpointed

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_idempotent_rerun_with_same_key_is_noop(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """Re-running with the same key after a successful commit is a no-op.

    First call commits. Second call's check-first finds the marker and bails;
    no second commit, no pipeline run.
    """
    df1 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df1.write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    version_after_first = delta_table.version()

    df2 = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    result = df2.write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    assert delta_table.version() == version_after_first
    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)
    assert result.to_pydict() == {"operation": [], "rows": [], "file_size": [], "file_name": []}


@pytest.mark.parametrize("mode", ["overwrite", "error", "ignore"])
def test_unsupported_mode_with_checkpoint_raises(delta_table, idempotent_commit, mode):
    """Only ``mode='append'`` is supported with ``checkpoint=``.

    The guard runs before the existing-table dispatch — otherwise
    ``mode='error'`` would assert and ``mode='ignore'`` would silently
    short-circuit, both bypassing the idempotent recovery flow.
    """
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(NotImplementedError, match="mode='append' only"):
        df.write_deltalake(delta_table, mode=mode, checkpoint=idempotent_commit)


def test_non_commit_error_propagates_without_retry(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """A non-``CommitFailedError`` raised during the commit phase propagates immediately.

    The retry loop is for ``deltalake.exceptions.CommitFailedError`` — concurrent
    commits from another writer. Schema mismatches, decode bugs, and other
    surprise exceptions should not be silently retried.
    """
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))

    real_table = delta_table._table

    class _NonCommitFailureWrapper:
        calls = 0

        def create_write_transaction(self, *args, **kwargs):
            type(self).calls += 1
            raise ValueError("definitely not a commit conflict")

        def __getattr__(self, name):
            return getattr(real_table, name)

    wrapper = _NonCommitFailureWrapper()
    delta_table._table = wrapper
    try:
        with pytest.raises(ValueError, match="definitely not a commit conflict"):
            df.write_deltalake(delta_table, checkpoint=idempotent_commit)
    finally:
        delta_table._table = real_table

    # Exactly one commit attempt — the narrow except didn't swallow + retry.
    assert wrapper.calls == 1


@pytest.mark.parametrize(
    "reserved_key",
    ["daft.idempotence-key", "daft.idempotence-foo"],
)
def test_reserved_custom_metadata_key_raises(delta_table, idempotent_commit, reserved_key):
    """User-provided `custom_metadata` keys prefixed `daft.idempotence-` are reserved."""
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="reserved"):
        df.write_deltalake(
            delta_table,
            checkpoint=idempotent_commit,
            custom_metadata={reserved_key: "spoofed"},
        )


@pytest.mark.parametrize(
    "reserved_key",
    ["daft.idempotence-key", "daft.idempotence-foo"],
)
def test_reserved_custom_metadata_key_raises_without_checkpoint(delta_table, reserved_key):
    """The `daft.idempotence-` prefix is reserved regardless of `checkpoint=`.

    Closes the spoofing path: a future user could otherwise land a
    non-idempotent write tagged with the marker, then later switch to
    `checkpoint=...` and have recovery silently miss the prior commit.
    """
    df = daft.from_pydict({"file_id": ["a"], "x": [1]})
    with pytest.raises(ValueError, match="reserved"):
        df.write_deltalake(
            delta_table,
            custom_metadata={reserved_key: "spoofed"},
        )


def test_user_custom_metadata_pass_through(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """Non-reserved user `custom_metadata` flows through to the commit."""
    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_deltalake(
        delta_table,
        checkpoint=idempotent_commit,
        custom_metadata={"author": "rohit", "release": "v1"},
    )

    entry = _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)
    assert entry.get("author") == "rohit"
    assert entry.get("release") == "v1"


def test_retry_loop_recovers_from_transient_commit_failure(
    delta_table, parquet_input, checkpoint_store, idempotent_commit
):
    """Defensive retry loop recovers from a transient ``CommitFailedError``.

    Iter 1 raises (e.g. concurrent writer landed a snapshot between our
    check and our commit); iter 2 succeeds. Asserts exactly one commit
    lands tagged with our key and the second attempt was actually made.
    """
    from deltalake.exceptions import CommitFailedError

    real_table = delta_table._table
    call_count = {"n": 0}

    class _TransientFailureWrapper:
        def create_write_transaction(self, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise CommitFailedError("simulated transient catalog conflict")
            return real_table.create_write_transaction(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(real_table, name)

    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))

    delta_table._table = _TransientFailureWrapper()
    try:
        df.write_deltalake(delta_table, checkpoint=idempotent_commit)
    finally:
        delta_table._table = real_table

    assert call_count["n"] >= 2

    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c"]


def test_table_with_pre_existing_legacy_commits(delta_table, parquet_input, checkpoint_store, idempotent_commit):
    """Adopting `checkpoint=...` on a table with pre-existing untagged commits.

    Recovery walk must not false-positive on legacy commits (no marker);
    new write lands as a fresh commit on top.
    """
    # The fixture's delta_table already has one initial empty commit (version 0).
    # That's our "legacy" commit.
    delta_table.update_incremental()
    legacy_version = delta_table.version()
    legacy_history = list(delta_table.history(limit=50))
    assert all(entry.get("daft.idempotence-key") is None for entry in legacy_history), (
        "legacy commits must not carry our marker"
    )

    df = daft.read_parquet(parquet_input, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    assert delta_table.version() == legacy_version + 1
    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)


def test_empty_input_on_fresh_store(delta_table, tmpdir, checkpoint_store, idempotent_commit):
    """Empty input — no Checkpointed entries are sealed, no commit lands."""
    import pyarrow as pa

    empty_path = str(tmpdir / "empty_in")
    os.makedirs(empty_path, exist_ok=True)
    daft.from_pydict(
        {
            "file_id": pa.array([], type=pa.string()),
            "x": pa.array([], type=pa.int64()),
        }
    ).write_parquet(empty_path)

    delta_table.update_incremental()
    version_before = delta_table.version()

    df = daft.read_parquet(empty_path, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))
    df.write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    assert delta_table.version() == version_before, "empty input must not land a new commit"
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending


def test_multi_partition_aggregates_into_single_commit(delta_table, tmpdir, checkpoint_store, idempotent_commit):
    """Multi-partition input aggregates per-partition entries into one Delta commit."""
    inp_dir = str(tmpdir / "multi_in")
    os.makedirs(inp_dir, exist_ok=True)
    parts = [
        (["a", "b"], [1, 2]),
        (["c", "d"], [3, 4]),
        (["e", "f"], [5, 6]),
    ]
    for i, (file_ids, xs) in enumerate(parts):
        daft.from_pydict({"file_id": file_ids, "x": xs}).write_parquet(f"{inp_dir}/part_{i}")

    delta_table.update_incremental()
    version_before = delta_table.version()

    daft.read_parquet(
        f"{inp_dir}/**", checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")
    ).write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    assert delta_table.version() == version_before + 1, "multi-partition must aggregate into a single commit"
    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_multi_partition_recovery_after_crash_between_stage_and_commit(
    delta_table, tmpdir, checkpoint_store, idempotent_commit
):
    """Scenario B with multi-partition input — recovery aggregates across all entries."""
    inp_dir = str(tmpdir / "multi_in")
    os.makedirs(inp_dir, exist_ok=True)
    parts = [(["a", "b"], [1, 2]), (["c", "d"], [3, 4]), (["e", "f"], [5, 6])]
    for i, (file_ids, xs) in enumerate(parts):
        daft.from_pydict({"file_id": file_ids, "x": xs}).write_parquet(f"{inp_dir}/part_{i}")

    df = daft.read_parquet(f"{inp_dir}/**", checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id"))

    # See test_recovery_after_crash_between_stage_and_commit for why we wrap
    # `_table` rather than patching the method directly.
    real_table = delta_table._table
    delta_table._table = _make_crash_wrapper(real_table)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            df.write_deltalake(delta_table, checkpoint=idempotent_commit)
    finally:
        delta_table._table = real_table

    delta_table.update_incremental()
    version_pre_crash = delta_table.version()
    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert len(pending) > 1

    daft.read_parquet(
        f"{inp_dir}/**", checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")
    ).write_deltalake(delta_table, checkpoint=idempotent_commit)

    delta_table.update_incremental()
    assert delta_table.version() == version_pre_crash + 1
    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    pending_after = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending_after

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]


def test_within_input_duplicate_keys_both_land(delta_table, tmpdir, checkpoint_store, idempotent_commit):
    """Duplicates within a single input are not deduped — both rows land."""
    inp = str(tmpdir / "dup_in")
    os.makedirs(inp, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "a", "b"], "x": [1, 2, 3]}).write_parquet(inp)
    daft.read_parquet(inp, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_deltalake(
        delta_table, checkpoint=idempotent_commit
    )

    _assert_single_commit_with_key(delta_table, IDEMPOTENCE_KEY)

    rows = _read_delta_rows(delta_table.table_uri)
    pairs = sorted(zip(rows["file_id"], rows["x"]))
    assert pairs == [("a", 1), ("a", 2), ("b", 3)]


def test_incremental_writes_dedupe_committed_keys(delta_table, tmpdir, checkpoint_store):
    """Two successive write_deltalake calls with distinct keys against the same store.

    Different ``idempotence_key`` per call — each commits its own version —
    but the source-side filter drops keys already Committed by the first
    call, so commit 2 only contains the *new* rows. Pins the source filter
    behavior across logical commits.
    """
    inp1 = str(tmpdir / "in1")
    os.makedirs(inp1, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c"], "x": [1, 2, 3]}).write_parquet(inp1)
    daft.read_parquet(inp1, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_deltalake(
        delta_table,
        checkpoint=daft.IdempotentCommit(store=checkpoint_store, idempotence_key="run-1"),
    )

    delta_table.update_incremental()
    version_after_first = delta_table.version()
    assert _commits_with_key(delta_table, "run-1") == 1

    inp2 = str(tmpdir / "in2")
    os.makedirs(inp2, exist_ok=True)
    daft.from_pydict({"file_id": ["a", "b", "c", "d", "e", "f"], "x": [1, 2, 3, 4, 5, 6]}).write_parquet(inp2)
    daft.read_parquet(inp2, checkpoint=daft.CheckpointConfig(store=checkpoint_store, on="file_id")).write_deltalake(
        delta_table,
        checkpoint=daft.IdempotentCommit(store=checkpoint_store, idempotence_key="run-2"),
    )

    delta_table.update_incremental()
    assert delta_table.version() == version_after_first + 1
    assert _commits_with_key(delta_table, "run-1") == 1
    assert _commits_with_key(delta_table, "run-2") == 1

    pending = [c for c in checkpoint_store.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    assert not pending

    rows = _read_delta_rows(delta_table.table_uri)
    assert sorted(rows["file_id"]) == ["a", "b", "c", "d", "e", "f"]
