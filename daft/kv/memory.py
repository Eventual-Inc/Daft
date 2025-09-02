"""In-memory dictionary-based KV Store implementation for Daft."""

from __future__ import annotations

from typing import Any

from daft.kv import KVConfig, KVStore


class MemoryKVStore(KVStore):
    """In-memory dictionary-based KV Store implementation.

    This class provides an in-memory key-value store for testing and
    development purposes. It stores data in a Python dictionary and
    does not persist data between sessions.

    Examples:
        >>> from daft.kv.memory import MemoryKVStore
        >>> # Create an in-memory KV store
        >>> memory_kv = MemoryKVStore(name="cache")
        >>> # Use with session
        >>> import daft
        >>> daft.attach(memory_kv, alias="cache")
        >>> daft.set_kv("cache")
    """

    def __init__(
        self,
        name: str | None = None,
        initial_data: dict[Any, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Memory KV Store.

        Args:
            name: Optional name for the KV store instance
            initial_data: Optional initial data to populate the store
            **kwargs: Additional options (currently unused)
        """
        self._name = name or "memory_kv_store"
        self._data = initial_data.copy() if initial_data else {}
        self._kwargs = kwargs

        # For memory store, we don't have a specific backend config
        # We'll create a minimal KVConfig for compatibility
        self._kv_config = KVConfig()

    @property
    def name(self) -> str:
        """Returns the KV store's name."""
        return self._name

    @property
    def backend_type(self) -> str:
        """Returns the backend type."""
        return "memory"

    def get_config(self) -> KVConfig:
        """Returns the KV store's configuration."""
        return self._kv_config

    @property
    def data(self) -> dict[Any, Any]:
        """Returns a copy of the stored data."""
        return self._data.copy()

    def get(self, key: Any) -> Any:
        """Get a value by key.

        Args:
            key: The key to retrieve

        Returns:
            The value associated with the key

        Raises:
            KeyError: If the key does not exist
        """
        return self._data[key]

    def set(self, key: Any, value: Any) -> None:
        """Set a key-value pair.

        Args:
            key: The key to set
            value: The value to associate with the key
        """
        self._data[key] = value

    def exists(self, key: Any) -> bool:
        """Check if a key exists.

        Args:
            key: The key to check

        Returns:
            True if the key exists, False otherwise
        """
        return key in self._data

    def delete(self, key: Any) -> bool:
        """Delete a key-value pair.

        Args:
            key: The key to delete

        Returns:
            True if the key was deleted, False if it didn't exist
        """
        if key in self._data:
            del self._data[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all data from the store."""
        self._data.clear()

    def size(self) -> int:
        """Get the number of key-value pairs in the store."""
        return len(self._data)

    def keys(self) -> list[Any]:
        """Get all keys in the store."""
        return list(self._data.keys())

    def values(self) -> list[Any]:
        """Get all values in the store."""
        return list(self._data.values())

    def items(self) -> list[tuple[Any, Any]]:
        """Get all key-value pairs in the store."""
        return list(self._data.items())

    def __repr__(self) -> str:
        """String representation of the Memory KV store."""
        return f"MemoryKVStore(name='{self._name}', size={len(self._data)})"

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"Memory KV Store '{self._name}' with {len(self._data)} items"
