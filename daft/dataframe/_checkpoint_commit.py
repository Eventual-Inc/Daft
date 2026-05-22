"""Lightweight utilities for checkpoint-based idempotent catalog commits.

Each connector (Iceberg, Delta Lake) keeps its commit logic inline;
these helpers factor out the repeated bookkeeping so connectors don't
duplicate the same patterns. Sink-agnostic — connector-specific decoding
of the pickled-object payloads is steered by the ``column_name`` parameter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.checkpoint import CheckpointStore
    from daft.dataframe.dataframe import DataFrame


def decode_file_metadata(store: CheckpointStore, column_name: str) -> list[Any]:
    """Decode pickled Python objects from staged FileMetadata blobs.

    Each FileMetadata entry in the checkpoint store is an Arrow IPC stream
    of a MicroPartition produced by the connector's writer. The MicroPartition
    has a single python-typed column whose cells are pickled objects of the
    connector's choosing (``pyiceberg.DataFile`` for Iceberg,
    ``deltalake.AddAction`` for Delta). The IPC roundtrip preserves them via pickle.

    Args:
        store: the live checkpoint store.
        column_name: name of the python-typed column to extract from each blob.

    Returns:
        Flattened list of decoded Python objects in store iteration order.

    Raises:
        RuntimeError: if any blob fails to decode (writer-version mismatch,
            store corruption, etc.). Wraps the underlying error with context
            naming the expected on-disk shape.
    """
    from daft.recordbatch import MicroPartition

    files: list[Any] = []
    for fm in store.get_checkpointed_files():
        try:
            mp = MicroPartition.from_ipc_stream(fm.data)
            files.extend(mp.to_pydict()[column_name])
        except Exception as e:
            raise RuntimeError(
                f"failed to decode {column_name!r} payloads from a checkpoint store FileMetadata blob; "
                "expected an Arrow IPC stream of a MicroPartition with a python-typed "
                f"`{column_name}` column carrying pickled objects. "
                f"Underlying error: {e}"
            ) from e
    return files


def commit_checkpoints(write_df: DataFrame, checkpoint_ids: list[str], store: Any) -> None:
    """Mark ``checkpoint_ids`` committed and emit one ``CheckpointCommitted`` event.

    Caller wraps in a broader try/except and invokes ``notify_checkpoint_failed``
    on raise; this function emits only the success-side event to avoid
    double-emission across the catalog-commit / mark_committed boundary.
    """
    import time

    from daft.context import get_context

    md = write_df._metadata
    assert md is not None, "commit_checkpoints must run after write_df.collect()"
    start = time.monotonic_ns()
    store.mark_committed(checkpoint_ids)
    duration_us = (time.monotonic_ns() - start) // 1000
    get_context()._notify_checkpoint_committed(md.query_id, checkpoint_ids, duration_us)


def notify_checkpoint_failed(write_df: DataFrame, checkpoint_ids: list[str], error: BaseException) -> None:
    """Emit one ``CheckpointFailed`` for ``checkpoint_ids``. Caller re-raises."""
    from daft.context import get_context

    md = write_df._metadata
    if md is None:
        return
    get_context()._notify_checkpoint_failed(md.query_id, checkpoint_ids, str(error))


def empty_write_result(write_df: DataFrame) -> DataFrame:
    """Build an empty write-result DataFrame preserving ``write_df``'s metadata.

    Used by checkpoint-aware write helpers when nothing needs to be committed
    (recovery short-circuit, fully-committed re-run, source filter dropped
    everything).
    """
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
