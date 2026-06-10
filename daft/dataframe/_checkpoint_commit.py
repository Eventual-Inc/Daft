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
