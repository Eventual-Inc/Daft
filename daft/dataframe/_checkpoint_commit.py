"""Lightweight utilities for checkpoint-based idempotent catalog commits.

Each connector (Iceberg, Delta Lake, etc.) keeps its commit logic inline;
these helpers factor out the repeated boilerplate so connectors don't
duplicate the same bookkeeping patterns.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.daft import CheckpointStatus

if TYPE_CHECKING:
    from daft.checkpoint import CheckpointStore
    from daft.dataframe.dataframe import DataFrame


def get_pending_or_execute(
    write_df: DataFrame,
    checkpoint: CheckpointStore,
) -> list[Any] | None:
    """Return pending checkpoint entries, executing the pipeline if needed.

    If there are no Checkpointed entries, calls ``write_df.collect()`` to run
    the pipeline (which populates the store via SCKO + BlockingSinkNode). If
    still empty after execution, returns ``None`` (meaning everything was
    already committed — the source filter dropped all rows).
    """
    pending = [c for c in checkpoint.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    if not pending:
        write_df.collect()
        pending = [c for c in checkpoint.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]

    if not pending:
        return None
    return pending


def validate_query_id(pending: list[Any]) -> tuple[str, list[str], str]:
    """Validate single-query-id invariant and extract ids/store metadata.

    Returns:
        (query_id, checkpoint_ids, store_path) — but store_path must be
        obtained from the checkpoint object by the caller. This function
        only returns (query_id, checkpoint_ids).

    Raises:
        RuntimeError: if pending entries have mismatched or empty query_ids.
    """
    our_query_id = pending[0].query_id
    if not our_query_id or any(c.query_id != our_query_id for c in pending):
        offending = sorted({c.query_id for c in pending})
        raise RuntimeError(
            "Checkpoint store contains pending entries with mismatched or empty query_ids; "
            f"single-query-id invariant violated (query_ids: {offending}). Possible causes: "
            "concurrent writers against the same path; the store was reused across "
            "different destinations (one store per destination — see CheckpointStore docs); "
            "or pre-feature legacy entries left behind in the store (empty query_id). "
            "Resolve by using a fresh checkpoint path or clearing the store."
        )
    our_ids = [c.id for c in pending]
    return our_query_id, our_ids


def decode_file_metadata(checkpoint: CheckpointStore, column_name: str) -> list[Any]:
    """Decode file objects from staged FileMetadata blobs.

    Each blob is an Arrow IPC stream of a MicroPartition; this extracts
    the given column (e.g. ``"data_file"`` for Iceberg, ``"add_action"``
    for Delta Lake) from each blob and concatenates them.
    """
    from daft.recordbatch.micropartition import MicroPartition

    files: list[Any] = []
    for fm in checkpoint.get_checkpointed_files():
        mp = MicroPartition.from_ipc_stream(fm.data)
        files.extend(mp.to_pydict()[column_name])
    return files


def empty_write_result(write_df: DataFrame) -> DataFrame:
    """Build an empty result DataFrame preserving write_df's metadata."""
    import pyarrow as pa

    from daft import from_pydict

    empty = from_pydict(
        {
            "operation": pa.array([], type=pa.string()),
            "rows": pa.array([], type=pa.int64()),
            "file_size": pa.array([], type=pa.int64()),
            "file_name": pa.array([], type=pa.string()),
        }
    )
    empty._metadata = write_df._metadata
    return empty
