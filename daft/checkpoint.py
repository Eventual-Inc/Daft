"""Checkpoint store for tracking pipeline progress."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.daft import CheckpointStoreConfig, IOConfig as _IOConfig, build_checkpoint_store

if TYPE_CHECKING:
    from daft.daft import IOConfig


class CheckpointStore:
    """A checkpoint store for tracking which source keys have been processed.

    On re-run, rows with keys already in the store are skipped.

    Args:
        prefix: URI prefix for the store (e.g. ``s3://bucket/checkpoints``).
        io_config: Optional IO configuration for the object store backend.

    Example:
        >>> checkpoint = daft.CheckpointStore("s3://bucket/ckpt", io_config=io)
        >>> df = daft.read_parquet("s3://input/", checkpoint=checkpoint, on="file_id")
    """

    _config: CheckpointStoreConfig
    _store: Any

    def __init__(self, prefix: str, io_config: IOConfig | None = None) -> None:
        if io_config is None:
            io_config = _IOConfig()
        self._config = CheckpointStoreConfig.object_store(prefix, io_config)
        self._store = None

    @property
    def config(self) -> CheckpointStoreConfig:
        return self._config

    def _get_store(self) -> Any:
        if self._store is None:
            self._store = build_checkpoint_store(self._config)
        return self._store

    def list_checkpoints(self) -> list[Any]:
        return self._get_store().list_checkpoints()

    def get_checkpointed_files(self) -> list[Any]:
        return self._get_store().get_checkpointed_files()

    def mark_committed(self, checkpoint_ids: list[str]) -> None:
        self._get_store().mark_committed(checkpoint_ids)

    def __repr__(self) -> str:
        return f"CheckpointStore({self._config})"
