"""Shared checkpoint-commit orchestration for catalog writers (Iceberg, Delta Lake, etc.).

The high-level flow — pending-entry detection, query_id validation, retry with
recovery check, post-commit bookkeeping — is connector-agnostic. Connector-
specific steps (file decode, recovery check, catalog commit, result building)
are injected via callbacks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from daft.daft import CheckpointStatus

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from daft.checkpoint import CheckpointStore
    from daft.daft import PyFileMetadata
    from daft.dataframe.dataframe import DataFrame


@dataclass
class CommitResult:
    operations: list[str]
    paths: list[str]
    row_counts: list[int]
    sizes: list[int]


def _empty_write_result(write_df: DataFrame) -> DataFrame:
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


def _build_result_df(result: CommitResult, write_df: DataFrame) -> DataFrame:
    from daft import from_pydict

    df = from_pydict(
        {
            "operation": pa.array(result.operations, type=pa.string()),
            "rows": pa.array(result.row_counts, type=pa.int64()),
            "file_size": pa.array(result.sizes, type=pa.int64()),
            "file_name": pa.array(result.paths, type=pa.string()),
        }
    )
    df._metadata = write_df._metadata
    return df


def commit_with_checkpoint(
    write_df: DataFrame,
    checkpoint: CheckpointStore,
    *,
    decode_files: Callable[[Iterable[PyFileMetadata]], list[Any]],
    refresh_and_check_committed: Callable[[str, str], bool],
    commit_files: Callable[[list[Any], str, str], None],
    files_to_result: Callable[[list[Any]], CommitResult],
    retryable_errors: tuple[type[Exception], ...] = (),
    max_retries: int = 2,
) -> DataFrame:
    """Idempotent catalog commit driven by the checkpoint store.

    Args:
        write_df: The write-plan DataFrame (``collect()`` is called only on a fresh run).
        checkpoint: The checkpoint store instance.
        decode_files: Decode ``FileMetadata`` blobs from the store into connector-
            specific file objects (e.g. ``pyiceberg.DataFile``, ``deltalake.AddAction``).
        refresh_and_check_committed: ``(store_path, query_id) -> bool``. Refresh the
            catalog state and return ``True`` if a prior commit with these markers
            already exists.
        commit_files: ``(files, store_path, query_id)``. Commit the files to the
            catalog, embedding ``daft.checkpoint-store`` and ``daft.checkpoint-query``
            markers in the catalog metadata.
        files_to_result: Convert committed file objects into a ``CommitResult``.
        retryable_errors: Exception types to catch and retry on (e.g.
            ``CommitFailedException`` for Iceberg). Empty tuple means no retry.
        max_retries: Maximum number of commit attempts.
    """
    pending = [c for c in checkpoint.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]
    if not pending:
        write_df.collect()
        pending = [c for c in checkpoint.list_checkpoints() if c.status == CheckpointStatus.Checkpointed]

    if not pending:
        return _empty_write_result(write_df)

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
    store_path = checkpoint.path

    files = decode_files(checkpoint.get_checkpointed_files())

    if not files:
        checkpoint.mark_committed(our_ids)
        return _empty_write_result(write_df)

    last_err: Exception | None = None
    for _ in range(max_retries):
        if refresh_and_check_committed(store_path, our_query_id):
            checkpoint.mark_committed(our_ids)
            return _build_result_df(files_to_result(files), write_df)

        try:
            commit_files(files, store_path, our_query_id)
            checkpoint.mark_committed(our_ids)
            return _build_result_df(files_to_result(files), write_df)
        except retryable_errors as e:
            last_err = e
            continue

    if last_err is not None:
        raise last_err
    raise RuntimeError(f"commit_with_checkpoint exhausted {max_retries} retries")
