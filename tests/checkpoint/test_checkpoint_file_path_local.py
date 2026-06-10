"""Local-filesystem file-path checkpoint tests (native runner).

File-path mode (``on`` omitted) works on the native runner: checkpoint keys
are derived from scan-task metadata instead of a key column. For local files
the key carries a file-identity suffix (mtime / size), so a file that is
*replaced* at the same path is reprocessed on the next run — this is the core
guarantee that distinguishes file-path mode from a plain path-only checkpoint.

These tests use a local ``file://`` checkpoint store, so they need neither
MinIO nor the Ray runner.
"""

from __future__ import annotations

import time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from daft import CheckpointConfig, CheckpointStore


def _run(input_path, ckpt_dir) -> int:
    """Run a file-path-mode checkpoint read and return the number of rows processed.

    Materializes with ``to_pydict()`` rather than ``count_rows()``: the latter
    inserts an aggregate, and checkpointing only supports map-only pipelines
    between source and sink.
    """
    store = CheckpointStore(f"file://{ckpt_dir}")
    df = daft.read_parquet(str(input_path), checkpoint=CheckpointConfig(store=store))
    return len(df.to_pydict()["value"])


def test_file_path_first_run_then_skip(tmp_path):
    """First run processes the file; an unchanged second run skips it entirely."""
    input_file = tmp_path / "data.parquet"
    ckpt_dir = tmp_path / "ckpt"
    pq.write_table(pa.table({"value": [1, 2, 3]}), input_file)

    assert _run(input_file, ckpt_dir) == 3, "run 1 should process all rows"
    assert _run(input_file, ckpt_dir) == 0, "run 2 should skip the unchanged file"


def test_file_path_reprocesses_when_content_changes(tmp_path):
    """A file replaced in place (same path, new content) is reprocessed.

    The replacement changes both the file size and mtime, either of which
    changes the derived atom key, so the file is no longer recognized as
    already-checkpointed.
    """
    input_file = tmp_path / "data.parquet"
    ckpt_dir = tmp_path / "ckpt"
    pq.write_table(pa.table({"value": [1, 2, 3]}), input_file)

    assert _run(input_file, ckpt_dir) == 3, "run 1 processes original content"
    assert _run(input_file, ckpt_dir) == 0, "run 2 skips unchanged file"

    # Replace the file in place with different content (different size).
    pq.write_table(pa.table({"value": [10, 20]}), input_file)

    assert _run(input_file, ckpt_dir) == 2, "run 3 reprocesses the changed file"
    assert _run(input_file, ckpt_dir) == 0, "run 4 skips the now-checkpointed file"


def test_file_path_reprocesses_on_mtime_change_same_size(tmp_path):
    """A same-size content change is still reprocessed via the mtime suffix.

    With identical size, only the modification time distinguishes the new key
    from the old one. The PR records mtime at nanosecond precision; the sleep
    guarantees the timestamp advances even on coarse (1s-resolution) filesystems.

    Reads a single named file (not a directory) to confirm mtime is captured on
    that path too — it goes through ``get_file_metadata``, not a size-only lookup.
    """
    input_file = tmp_path / "data.parquet"
    ckpt_dir = tmp_path / "ckpt"
    pq.write_table(pa.table({"value": [1, 2, 3]}), input_file)

    assert _run(input_file, ckpt_dir) == 3
    assert _run(input_file, ckpt_dir) == 0

    # Same row count / schema => same on-disk size; only mtime differs.
    time.sleep(1.1)
    pq.write_table(pa.table({"value": [4, 5, 6]}), input_file)

    assert _run(input_file, ckpt_dir) == 3, "same-size change must still reprocess (mtime suffix)"


def test_file_path_incremental_new_file(tmp_path):
    """A new file added to the input directory is processed; old files skipped."""
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    ckpt_dir = tmp_path / "ckpt"
    pq.write_table(pa.table({"value": [1, 2, 3]}), input_dir / "a.parquet")

    assert _run(input_dir, ckpt_dir) == 3, "run 1 processes the first file"

    pq.write_table(pa.table({"value": [4, 5]}), input_dir / "b.parquet")
    assert _run(input_dir, ckpt_dir) == 2, "run 2 processes only the new file"
    assert _run(input_dir, ckpt_dir) == 0, "run 3 has nothing new"


def test_file_path_mode_allowed_on_native_runner(tmp_path):
    """File-path mode must not be rejected on the native runner (only row-level is)."""
    input_file = tmp_path / "data.parquet"
    ckpt_dir = tmp_path / "ckpt"
    pq.write_table(pa.table({"value": [1]}), input_file)

    store = CheckpointStore(f"file://{ckpt_dir}")
    # Should not raise.
    daft.read_parquet(str(input_file), checkpoint=CheckpointConfig(store=store)).to_pydict()

    # Row-level mode (on=) remains unsupported on the native runner.
    with pytest.raises(ValueError, match="native"):
        daft.read_parquet(
            str(input_file), checkpoint=CheckpointConfig(store=CheckpointStore(f"file://{ckpt_dir}"), on="value")
        ).to_pydict()
