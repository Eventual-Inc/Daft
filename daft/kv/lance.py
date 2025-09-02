"""Lance-based KV Store implementation for Daft."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.io import IOConfig
    from daft.kv import KVConfig, LanceConfig

from daft.kv import KVConfig, KVStore, LanceConfig


class LanceKVStore(KVStore):
    """Lance-based KV Store implementation.

    This class provides a Lance-backed key-value store that integrates
    with Daft's session management system. It wraps the existing Lance
    functionality and provides the standard KVStore interface.

    Examples:
        >>> from daft.kv.lance import LanceKVStore
        >>> # Create a Lance KV store
        >>> lance_kv = LanceKVStore(
        ...     name="my_lance_store", uri="s3://bucket/dataset.lance", columns=["embedding", "metadata"]
        ... )
        >>> # Use with session
        >>> import daft
        >>> daft.attach(lance_kv, alias="embeddings")
        >>> daft.set_kv("embeddings")
    """

    def __init__(
        self,
        name: str | None = None,
        uri: str | None = None,
        io_config: IOConfig | None = None,
        columns: list[str] | None = None,
        batch_size: int = 1000,
        max_connections: int = 32,
        **kwargs: Any,
    ) -> None:
        """Initialize Lance KV Store.

        Args:
            name: Optional name for the KV store instance
            uri: URI path to the Lance dataset
            io_config: IO configuration for accessing the dataset
            columns: List of column names to retrieve
            batch_size: Batch size for processing optimization
            max_connections: Maximum number of connections
            **kwargs: Additional Lance-specific options
        """
        if uri is None:
            raise ValueError("uri is required for Lance KV store")

        self._name = name or "lance_kv_store"
        self._uri = uri
        self._io_config = io_config
        self._columns = columns
        self._batch_size = batch_size
        self._max_connections = max_connections
        self._kwargs = kwargs

        # Create the Lance configuration
        self._lance_config = LanceConfig(
            uri=uri,
            io_config=io_config,
            columns=columns,
            batch_size=batch_size,
            max_connections=max_connections,
        )

        # Create the unified KV configuration
        self._kv_config = KVConfig(lance=self._lance_config)

    @property
    def name(self) -> str:
        """Returns the KV store's name."""
        return self._name

    @property
    def backend_type(self) -> str:
        """Returns the backend type."""
        return "lance"

    def get_config(self) -> KVConfig:
        """Returns the KV store's configuration."""
        return self._kv_config

    @property
    def uri(self) -> str:
        """Returns the Lance dataset URI."""
        return self._uri

    @property
    def columns(self) -> list[str] | None:
        """Returns the columns to retrieve."""
        return self._columns

    @property
    def batch_size(self) -> int:
        """Returns the batch size."""
        return self._batch_size

    @property
    def max_connections(self) -> int:
        """Returns the maximum connections."""
        return self._max_connections

    def with_columns(self, columns: list[str]) -> LanceKVStore:
        """Create a new Lance KV store with different columns.

        Args:
            columns: List of column names to retrieve

        Returns:
            LanceKVStore: New Lance KV store instance with updated columns
        """
        return LanceKVStore(
            name=self._name,
            uri=self._uri,
            io_config=self._io_config,
            columns=columns,
            batch_size=self._batch_size,
            max_connections=self._max_connections,
            **self._kwargs,
        )

    def with_batch_size(self, batch_size: int) -> LanceKVStore:
        """Create a new Lance KV store with different batch size.

        Args:
            batch_size: Batch size for processing optimization

        Returns:
            LanceKVStore: New Lance KV store instance with updated batch size
        """
        return LanceKVStore(
            name=self._name,
            uri=self._uri,
            io_config=self._io_config,
            columns=self._columns,
            batch_size=batch_size,
            max_connections=self._max_connections,
            **self._kwargs,
        )

    def with_max_connections(self, max_connections: int) -> LanceKVStore:
        """Create a new Lance KV store with different max connections.

        Args:
            max_connections: Maximum number of connections

        Returns:
            LanceKVStore: New Lance KV store instance with updated max connections
        """
        return LanceKVStore(
            name=self._name,
            uri=self._uri,
            io_config=self._io_config,
            columns=self._columns,
            batch_size=self._batch_size,
            max_connections=max_connections,
            **self._kwargs,
        )

    def __repr__(self) -> str:
        """String representation of the Lance KV store."""
        return (
            f"LanceKVStore(name='{self._name}', uri='{self._uri}', "
            f"columns={self._columns}, batch_size={self._batch_size}, "
            f"max_connections={self._max_connections})"
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"Lance KV Store '{self._name}' at {self._uri}"
