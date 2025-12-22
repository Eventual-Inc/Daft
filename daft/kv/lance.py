"""Lance-based KV Store implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.daft import _LanceKVStore  # type: ignore[attr-defined]
from daft.kv import KVStore

if TYPE_CHECKING:
    from daft.io import IOConfig


class LanceKVStore(KVStore):
    """A KV Store implementation backed by a Lance dataset."""

    def __init__(
        self,
        name: str,
        uri: str,
        key_column: str,
        io_config: IOConfig | None = None,
        batch_size: int = 100,
    ) -> None:
        """Initialize a Lance KV Store.

        Args:
            name: Name of the KV store.
            uri: URI to the Lance dataset.
            key_column: Name of the column to use as the key.
            io_config: Optional IOConfig for accessing the dataset.
            batch_size: Default batch size for scans (default: 100).
        """
        # Type ignore because _LanceKVStore is a dynamic PyO3 class not visible to static analysis
        self._inner = _LanceKVStore(name, uri, key_column, batch_size, io_config)

    @property
    def name(self) -> str:
        """Returns the KV store's name."""
        return self._inner.name

    @property
    def backend_type(self) -> str:
        """Returns the backend type."""
        return "lance"

    def get_config(self) -> Any:
        """Returns the KV store's configuration."""
        return self._inner.to_config()

    def get(self, key: str) -> Any:
        """Get a value by key.

        Args:
            key: The key to look up.

        Returns:
            The value associated with the key, or None if not found.
        """
        return self._inner.get(key)

    def batch_get(self, keys: list[str]) -> list[Any]:
        """Get multiple values by keys.

        This delegates to the Rust implementation which handles batching.
        Currently, the Rust `get` is single-key, so this iterates.
        Future optimization: expose batch_get in Rust.

        Args:
            keys: List of keys to look up.

        Returns:
            List of values corresponding to the keys.
        """
        # Note: In distributed execution (via expressions), batch_get is handled
        # by the Rust executor which calls get() in parallel or uses specific optimizations.
        # This python-side implementation is mainly for local testing/usage.
        return [self.get(k) for k in keys]

    def __repr__(self) -> str:
        return f"LanceKVStore(name='{self.name}')"


__all__ = ["LanceKVStore"]
