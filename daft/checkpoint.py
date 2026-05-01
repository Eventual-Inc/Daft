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
        prefix: URI prefix for the store (e.g. ``s3://bucket/checkpoints``).
        io_config: Optional IO configuration for the object store backend.

    Example:
        >>> store = daft.CheckpointStore("s3://bucket/ckpt", io_config=io)
        >>> config = daft.CheckpointConfig(store=store, on="file_id")
        >>> df = daft.read_parquet("s3://input/", checkpoint=config)
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


class CheckpointConfig:
    """Per-source checkpoint configuration.

    Bundles the store, the key column, and strategy-specific tuning into a
    single object. Pass via ``checkpoint=`` on source readers (e.g.
    :func:`daft.read_parquet`).

    On re-run, rows whose key already exists in the store are skipped.

    Args:
        store: The :class:`CheckpointStore` holding sealed keys.
        on: Name of the source column that uniquely identifies inputs (e.g.
            a file hash or document ID). Must exist in the source schema.
        settings: Optional tuning for the key-filtering anti-join. Defaults
            to engine-chosen values when omitted.

    Example:
        >>> store = daft.CheckpointStore("s3://bucket/ckpt", io_config=io)
        >>> config = daft.CheckpointConfig(
        ...     store=store,
        ...     on="file_id",
        ...     settings=daft.KeyFilteringSettings(num_workers=4, cpus_per_worker=1.0),
        ... )
        >>> df = daft.read_parquet("s3://input/", checkpoint=config)

    Assumptions:
        - **Map-only pipelines.** Only supports pipelines where every operator
          between source and sink is map-only (project, filter, explode, UDF,
          etc.). Shuffle/materialization operators (aggregate, sort, distinct,
          join, pivot, window, repartition) are rejected at plan build time.
        - **Source has a checkpoint key.** The ``on=`` column must exist in
          the source schema and uniquely identify source inputs. The key is
          written to the store on every run, so it should be lightweight.
        - **Strong consistency.** The backing object store must provide
          read-after-write consistency. After ``checkpoint()``, the next
          run's anti-join must be able to see the newly checkpointed keys.
    """

    _inner: _CheckpointConfig

    def __init__(
        self,
        store: CheckpointStore,
        on: str,
        settings: KeyFilteringSettings | None = None,
    ) -> None:
        self._inner = _CheckpointConfig(store.config, on, settings)

    def __repr__(self) -> str:
        return f"CheckpointConfig({self._inner!r})"
