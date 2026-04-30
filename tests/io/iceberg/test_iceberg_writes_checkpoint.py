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
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
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
