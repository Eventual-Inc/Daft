"""Checkpoint store and per-source configuration for tracking pipeline progress."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.daft import (
    CheckpointConfig as _CheckpointConfig,
)
from daft.daft import (
    CheckpointStoreConfig,
    KeyFilteringSettings,
    build_checkpoint_store,
)
from daft.daft import IOConfig as _IOConfig

if TYPE_CHECKING:
    from daft.daft import IOConfig


__all__ = ["CheckpointConfig", "CheckpointStore", "KeyFilteringSettings"]


class CheckpointStore:
    """A checkpoint store for tracking which source keys have been processed.

    Identifies *where* checkpoint state lives. Pair with
    :class:`CheckpointConfig` to attach the store to a specific source.

    Args:
        path: URI for the store root (e.g. ``s3://bucket/checkpoints``).
        io_config: Optional IO configuration for the object store backend.

    Example:
        >>> store = daft.CheckpointStore("s3://bucket/ckpt", io_config=io)
        >>> config = daft.CheckpointConfig(store=store, on="file_id")
        >>> df = daft.read_parquet("s3://input/", checkpoint=config)
    """

    _path: str
    _config: CheckpointStoreConfig
    _store: Any

    def __init__(self, path: str, io_config: IOConfig | None = None) -> None:
        if io_config is None:
            io_config = _IOConfig()
        self._path = path
        self._config = CheckpointStoreConfig.object_store(path, io_config)
        self._store = None

    @property
    def path(self) -> str:
        """URI for the store root (e.g. ``s3://bucket/checkpoints``)."""
        return self._path

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


class CheckpointConfig:
    """Per-source checkpoint configuration.

    Bundles the store, the key mode, and strategy-specific tuning into a
    single object. Pass via ``checkpoint=`` on source readers (e.g.
    :func:`daft.read_parquet`).

    Two modes are supported:

    - **File-path mode** (default when ``on`` is omitted): keys are derived
      from scan-task metadata (file paths and chunk specs). No user-specified
      key column needed. Any file-based source is checkpointable out of the
      box. Filter and projection pushdown are fully enabled.

    - **Row-level mode** (when ``on`` is specified): keys are values of the
      named column. Requires an anti-join against previously-sealed keys at
      execution time.

    Args:
        store: The :class:`CheckpointStore` holding sealed keys.
        on: Optional name of the source column that uniquely identifies
            inputs. When omitted, file-path mode is used.
        settings: Optional tuning for the key-filtering anti-join (only
            applies to row-level mode). Defaults to engine-chosen values.

    Example:
        >>> store = daft.CheckpointStore("s3://bucket/ckpt", io_config=io)
        >>> # File-path mode (no on=):
        >>> config = daft.CheckpointConfig(store=store)
        >>> df = daft.read_parquet("s3://input/", checkpoint=config)
        >>> # Row-level mode:
        >>> config = daft.CheckpointConfig(store=store, on="file_id")
        >>> df = daft.read_parquet("s3://input/", checkpoint=config)
    """

    _inner: _CheckpointConfig

    def __init__(
        self,
        store: CheckpointStore,
        on: str | None = None,
        settings: KeyFilteringSettings | None = None,
    ) -> None:
        self._inner = _CheckpointConfig(store.config, on, settings)

    def __repr__(self) -> str:
        return f"CheckpointConfig({self._inner!r})"
