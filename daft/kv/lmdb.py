"""LMDB-based KV Store implementation for Daft."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.kv import KVConfig, LMDBConfig

from daft.kv import KVConfig, KVStore, LMDBConfig


class LMDBKVStore(KVStore):
    """LMDB-based KV Store implementation.

    This class provides an LMDB-backed key-value store that integrates
    with Daft's session management system. LMDB (Lightning Memory-Mapped Database)
    is a high-performance embedded transactional database.

    Examples:
        >>> from daft.kv.lmdb import LMDBKVStore
        >>> # Create an LMDB KV store
        >>> lmdb_kv = LMDBKVStore(
        ...     name="my_lmdb_store",
        ...     path="/path/to/database.lmdb",
        ...     map_size=2 * 1024 * 1024 * 1024,  # 2GB
        ... )
        >>> # Use with session
        >>> import daft
        >>> daft.attach(lmdb_kv, alias="persistent")
        >>> daft.set_kv("persistent")
    """

    def __init__(
        self,
        name: str | None = None,
        path: str | None = None,
        map_size: int = 1024 * 1024 * 1024,  # 1GB default
        max_dbs: int = 10,
        readonly: bool = False,
        sync: bool = True,
        metasync: bool = True,
        writemap: bool = False,
        max_readers: int = 126,
        **kwargs: Any,
    ) -> None:
        """Initialize LMDB KV Store.

        Args:
            name: Optional name for the KV store instance
            path: File path to the LMDB database
            map_size: Maximum size of the memory map (in bytes)
            max_dbs: Maximum number of named databases
            readonly: Whether to open in read-only mode
            sync: Whether to flush system buffers to disk when committing
            metasync: Whether to flush system buffers to disk only on metapage
            writemap: Use a writeable memory map
            max_readers: Maximum number of concurrent readers
            **kwargs: Additional LMDB-specific options
        """
        if path is None:
            raise ValueError("path is required for LMDB KV store")

        self._name = name or "lmdb_kv_store"
        self._path = path
        self._map_size = map_size
        self._max_dbs = max_dbs
        self._readonly = readonly
        self._sync = sync
        self._metasync = metasync
        self._writemap = writemap
        self._max_readers = max_readers
        self._kwargs = kwargs

        # Create the LMDB configuration
        self._lmdb_config = LMDBConfig(
            path=path,
            map_size=map_size,
            max_dbs=max_dbs,
            readonly=readonly,
            sync=sync,
            metasync=metasync,
            writemap=writemap,
            max_readers=max_readers,
        )

        # Create the unified KV configuration
        self._kv_config = KVConfig(lmdb=self._lmdb_config)

    @property
    def name(self) -> str:
        """Returns the KV store's name."""
        return self._name

    @property
    def backend_type(self) -> str:
        """Returns the backend type."""
        return "lmdb"

    def get_config(self) -> KVConfig:
        """Returns the KV store's configuration."""
        return self._kv_config

    @property
    def path(self) -> str:
        """Returns the LMDB database path."""
        return self._path

    @property
    def map_size(self) -> int:
        """Returns the memory map size."""
        return self._map_size

    @property
    def max_dbs(self) -> int:
        """Returns the maximum number of databases."""
        return self._max_dbs

    @property
    def readonly(self) -> bool:
        """Returns whether the database is read-only."""
        return self._readonly

    @property
    def sync(self) -> bool:
        """Returns the sync setting."""
        return self._sync

    @property
    def metasync(self) -> bool:
        """Returns the metasync setting."""
        return self._metasync

    @property
    def writemap(self) -> bool:
        """Returns the writemap setting."""
        return self._writemap

    @property
    def max_readers(self) -> int:
        """Returns the maximum number of readers."""
        return self._max_readers

    def with_map_size(self, map_size: int) -> LMDBKVStore:
        """Create a new LMDB KV store with different map size.

        Args:
            map_size: Maximum size of the memory map (in bytes)

        Returns:
            LMDBKVStore: New LMDB KV store instance with updated map size
        """
        return LMDBKVStore(
            name=self._name,
            path=self._path,
            map_size=map_size,
            max_dbs=self._max_dbs,
            readonly=self._readonly,
            sync=self._sync,
            metasync=self._metasync,
            writemap=self._writemap,
            max_readers=self._max_readers,
            **self._kwargs,
        )

    def with_readonly(self, readonly: bool = True) -> LMDBKVStore:
        """Create a new LMDB KV store with different readonly setting.

        Args:
            readonly: Whether to open in read-only mode

        Returns:
            LMDBKVStore: New LMDB KV store instance with updated readonly setting
        """
        return LMDBKVStore(
            name=self._name,
            path=self._path,
            map_size=self._map_size,
            max_dbs=self._max_dbs,
            readonly=readonly,
            sync=self._sync,
            metasync=self._metasync,
            writemap=self._writemap,
            max_readers=self._max_readers,
            **self._kwargs,
        )

    def with_sync_options(self, sync: bool = True, metasync: bool = True) -> LMDBKVStore:
        """Create a new LMDB KV store with different sync options.

        Args:
            sync: Whether to flush system buffers to disk when committing
            metasync: Whether to flush system buffers to disk only on metapage

        Returns:
            LMDBKVStore: New LMDB KV store instance with updated sync options
        """
        return LMDBKVStore(
            name=self._name,
            path=self._path,
            map_size=self._map_size,
            max_dbs=self._max_dbs,
            readonly=self._readonly,
            sync=sync,
            metasync=metasync,
            writemap=self._writemap,
            max_readers=self._max_readers,
            **self._kwargs,
        )

    def __repr__(self) -> str:
        """String representation of the LMDB KV store."""
        return (
            f"LMDBKVStore(name='{self._name}', path='{self._path}', "
            f"map_size={self._map_size}, readonly={self._readonly})"
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"LMDB KV Store '{self._name}' at {self._path}"
