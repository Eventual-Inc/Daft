"""KV Store configuration and utilities for Daft."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING, Optional, Any, Callable

if TYPE_CHECKING:
    from daft.io import IOConfig

__all__ = ["KVConfig", "KVStore", "LMDBConfig", "LanceConfig", "load_kv"]


@dataclass
class LanceConfig:
    """Configuration for Lance KV Store backend.

    Args:
        uri: URI path to the Lance dataset
        io_config: IO configuration for accessing the dataset
        columns: List of column names to retrieve
        batch_size: Batch size for processing optimization
        max_connections: Maximum number of connections
    """

    uri: str
    io_config: IOConfig | None = None
    columns: list[str] | None = None
    batch_size: int = 1000
    max_connections: int = 32

    def with_io_config(self, io_config: IOConfig) -> LanceConfig:
        """Set IO configuration."""
        self.io_config = io_config
        return self

    def with_columns(self, columns: list[str]) -> LanceConfig:
        """Set columns to retrieve."""
        self.columns = columns
        return self

    def with_batch_size(self, batch_size: int) -> LanceConfig:
        """Set batch size."""
        self.batch_size = batch_size
        return self

    def with_max_connections(self, max_connections: int) -> LanceConfig:
        """Set maximum connections."""
        self.max_connections = max_connections
        return self


@dataclass
class LMDBConfig:
    """Configuration for LMDB KV Store backend.

    Args:
        path: File path to the LMDB database
        map_size: Maximum size of the memory map (in bytes)
        max_dbs: Maximum number of named databases
        readonly: Whether to open in read-only mode
        sync: Whether to flush system buffers to disk when committing
        metasync: Whether to flush system buffers to disk only on metapage
        writemap: Use a writeable memory map
        max_readers: Maximum number of concurrent readers
    """

    path: str
    map_size: int = 1024 * 1024 * 1024  # 1GB default
    max_dbs: int = 10
    readonly: bool = False
    sync: bool = True
    metasync: bool = True
    writemap: bool = False
    max_readers: int = 126

    def with_map_size(self, map_size: int) -> LMDBConfig:
        """Set map size."""
        self.map_size = map_size
        return self

    def with_max_dbs(self, max_dbs: int) -> LMDBConfig:
        """Set maximum databases."""
        self.max_dbs = max_dbs
        return self

    def with_readonly(self, readonly: bool = True) -> LMDBConfig:
        """Set read-only mode."""
        self.readonly = readonly
        return self

    def with_sync_options(self, sync: bool = True, metasync: bool = True) -> LMDBConfig:
        """Set synchronization options."""
        self.sync = sync
        self.metasync = metasync
        return self


@dataclass
class KVConfig:
    """Unified configuration for KV Store operations.

    This class provides a unified interface for configuring different
    KV Store backends. Supports Lance and LMDB backends.

    Args:
        lance: Lance backend configuration
        lmdb: LMDB backend configuration

    Examples:
        >>> from daft.kv import KVConfig, LanceConfig, LMDBConfig
        >>> from daft.io import IOConfig
        >>> # Create Lance configuration for AI/ML workloads
        >>> lance_config = LanceConfig(
        ...     uri="s3://my-bucket/my-dataset.lance",
        ...     io_config=IOConfig.s3(region="us-west-2"),
        ...     columns=["image", "metadata"],
        ...     batch_size=500,
        ... )
        >>> kv_config = KVConfig(lance=lance_config)
        >>> # Create LMDB configuration for high-performance caching
        >>> lmdb_config = LMDBConfig(
        ...     path="/path/to/cache.lmdb",
        ...     map_size=2 * 1024 * 1024 * 1024,  # 2GB
        ...     readonly=True,
        ... )
        >>> kv_config = KVConfig(lmdb=lmdb_config)
        >>> # Use with kv_get function
        >>> from daft.functions.kv import kv_get
        >>> df = df.with_column("data", kv_get("row_id", kv_config=kv_config))
    """

    lance: LanceConfig | None = None
    lmdb: LMDBConfig | None = None

    @classmethod
    def lance_config(
        cls,
        uri: str,
        io_config: IOConfig | None = None,
        columns: list[str] | None = None,
        batch_size: int = 1000,
        max_connections: int = 32,
    ) -> KVConfig:
        """Create a KVConfig with Lance backend configuration.

        Args:
            uri: URI path to the Lance dataset
            io_config: IO configuration for accessing the dataset
            columns: List of column names to retrieve
            batch_size: Batch size for processing optimization
            max_connections: Maximum number of connections

        Returns:
            KVConfig: Configured KV config with Lance backend
        """
        lance_config = LanceConfig(
            uri=uri,
            io_config=io_config,
            columns=columns,
            batch_size=batch_size,
            max_connections=max_connections,
        )
        return cls(lance=lance_config)

    @classmethod
    def lmdb_config(
        cls,
        path: str,
        map_size: int = 1024 * 1024 * 1024,
        max_dbs: int = 10,
        readonly: bool = False,
        sync: bool = True,
        metasync: bool = True,
        writemap: bool = False,
        max_readers: int = 126,
    ) -> KVConfig:
        """Create a KVConfig with LMDB backend configuration.

        Args:
            path: File path to the LMDB database
            map_size: Maximum size of the memory map (in bytes)
            max_dbs: Maximum number of named databases
            readonly: Whether to open in read-only mode
            sync: Whether to flush system buffers to disk when committing
            metasync: Whether to flush system buffers to disk only on metapage
            writemap: Use a writeable memory map
            max_readers: Maximum number of concurrent readers

        Returns:
            KVConfig: Configured KV config with LMDB backend
        """
        lmdb_config = LMDBConfig(
            path=path,
            map_size=map_size,
            max_dbs=max_dbs,
            readonly=readonly,
            sync=sync,
            metasync=metasync,
            writemap=writemap,
            max_readers=max_readers,
        )
        return cls(lmdb=lmdb_config)

    def get_active_backend(self) -> str:
        """Get the name of the active backend.

        Returns:
            str: Name of the active backend

        Raises:
            ValueError: If no backend is configured or multiple backends are configured
        """
        active_backends = []
        if self.lance is not None:
            active_backends.append("lance")
        if self.lmdb is not None:
            active_backends.append("lmdb")

        if len(active_backends) == 0:
            raise ValueError("No active KV backend configured")
        elif len(active_backends) > 1:
            raise ValueError(
                f"Multiple backends configured: {active_backends}. Only one backend is allowed per KVConfig."
            )
        else:
            return active_backends[0]

    def is_lance_backend(self) -> bool:
        """Check if Lance backend is configured."""
        return self.lance is not None

    def is_lmdb_backend(self) -> bool:
        """Check if LMDB backend is configured."""
        return self.lmdb is not None

    def to_json(self) -> str:
        """Serialize KVConfig to JSON string.

        This method is used to serialize the KVConfig object for transmission
        to the Rust layer through the Expression system.

        Returns:
            str: JSON representation of the KVConfig
        """
        # Convert to dictionary, handling IOConfig serialization
        config_dict = asdict(self)

        # Handle IOConfig serialization if present
        if self.lance and self.lance.io_config:
            # IOConfig needs special handling - for now, we'll skip it
            # TODO: Implement proper IOConfig serialization when needed
            config_dict["lance"]["io_config"] = None

        return json.dumps(config_dict, ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_json(cls, json_str: str) -> KVConfig:
        """Deserialize KVConfig from JSON string.

        Args:
            json_str: JSON string representation of KVConfig

        Returns:
            KVConfig: Deserialized KVConfig object
        """
        config_dict = json.loads(json_str)

        # Reconstruct Lance config if present
        lance_config = None
        if config_dict.get("lance"):
            lance_data = config_dict["lance"]
            lance_config = LanceConfig(
                uri=lance_data["uri"],
                io_config=lance_data.get("io_config"),  # Will be None for now
                columns=lance_data.get("columns"),
                batch_size=lance_data.get("batch_size", 1000),
                max_connections=lance_data.get("max_connections", 32),
            )

        # Reconstruct LMDB config if present
        lmdb_config = None
        if config_dict.get("lmdb"):
            lmdb_data = config_dict["lmdb"]
            lmdb_config = LMDBConfig(
                path=lmdb_data["path"],
                map_size=lmdb_data.get("map_size", 1024 * 1024 * 1024),
                max_dbs=lmdb_data.get("max_dbs", 10),
                readonly=lmdb_data.get("readonly", False),
                sync=lmdb_data.get("sync", True),
                metasync=lmdb_data.get("metasync", True),
                writemap=lmdb_data.get("writemap", False),
                max_readers=lmdb_data.get("max_readers", 126),
            )

        return cls(lance=lance_config, lmdb=lmdb_config)


class KVStoreImportError(ImportError):
    def __init__(self, dependencies: list[str]):
        deps = ", ".join(f"'{d}'" for d in dependencies)
        super().__init__(f"Missing required dependencies: {deps}. " f"Please install {deps} to use this KV store.")


class KVStore(ABC):
    """Base class for KV Store implementations.

    A KV Store provides key-value storage functionality with support for
    batch operations and different backend implementations.

    Note:
        This follows the same pattern as daft.ai.Provider and daft.catalog.Catalog
        for session-based configuration management.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the KV store's name."""
        ...

    @property
    @abstractmethod
    def backend_type(self) -> str:
        """Returns the backend type (e.g., 'lance', 'lmdb', 'redis')."""
        ...

    @abstractmethod
    def get_config(self) -> KVConfig:
        """Returns the KV store's configuration."""
        ...


def load_lance(name: str | None = None, **options: Any) -> KVStore:
    """Load a Lance-based KV store.

    Args:
        name: Optional name for the KV store instance
        **options: Lance-specific configuration options

    Returns:
        KVStore: A Lance-based KV store instance

    Raises:
        KVStoreImportError: If Lance dependencies are not available
    """
    try:
        from daft.kv.lance import LanceKVStore

        return LanceKVStore(name, **options)
    except ImportError as e:
        raise KVStoreImportError(["lance"]) from e


def load_lmdb(name: str | None = None, **options: Any) -> KVStore:
    """Load an LMDB-based KV store.

    Args:
        name: Optional name for the KV store instance
        **options: LMDB-specific configuration options

    Returns:
        KVStore: An LMDB-based KV store instance

    Raises:
        KVStoreImportError: If LMDB dependencies are not available
    """
    try:
        from daft.kv.lmdb import LMDBKVStore

        return LMDBKVStore(name, **options)
    except ImportError as e:
        raise KVStoreImportError(["lmdb"]) from e


def load_redis(name: str | None = None, **options: Any) -> KVStore:
    """Load a Redis-based KV store.

    Args:
        name: Optional name for the KV store instance
        **options: Redis-specific configuration options

    Returns:
        KVStore: A Redis-based KV store instance

    Raises:
        KVStoreImportError: If Redis dependencies are not available
    """
    try:
        from daft.kv.redis import RedisKVStore

        return RedisKVStore(name, **options)
    except ImportError as e:
        raise KVStoreImportError(["redis"]) from e


def load_memory(name: str | None = None, **options: Any) -> KVStore:
    """Load an in-memory dictionary-based KV store.

    Args:
        name: Optional name for the KV store instance
        **options: Memory store configuration options

    Returns:
        KVStore: An in-memory KV store instance
    """
    from daft.kv.memory import MemoryKVStore

    return MemoryKVStore(name, **options)


# Registry of available KV store loaders
KV_STORES: dict[str, Callable[..., KVStore]] = {
    "lance": load_lance,
    "lmdb": load_lmdb,
    "redis": load_redis,
    "memory": load_memory,
}


def load_kv(store_type: str, name: str | None = None, **options: Any) -> KVStore:
    """Load a KV store of the specified type.

    This is the main factory function for creating KV store instances,
    following the same pattern as daft.ai.provider.load_provider.

    Args:
        store_type: Type of KV store to load ('lance', 'lmdb', 'redis', 'memory')
        name: Optional name for the KV store instance
        **options: Store-specific configuration options

    Returns:
        KVStore: A KV store instance of the specified type

    Raises:
        ValueError: If the store type is not supported
        KVStoreImportError: If required dependencies are not available

    Examples:
        >>> from daft.kv import load_kv
        >>> # Load Lance KV store
        >>> lance_kv = load_kv("lance", name="my_lance", uri="s3://bucket/dataset")
        >>> # Load in-memory KV store
        >>> memory_kv = load_kv("memory", name="cache")
        >>> # Load LMDB KV store
        >>> lmdb_kv = load_kv("lmdb", name="persistent", path="/path/to/db.lmdb")
    """
    if store_type not in KV_STORES:
        raise ValueError(
            f"KV store type '{store_type}' is not yet supported. " f"Available types: {list(KV_STORES.keys())}"
        )
    return KV_STORES[store_type](name, **options)
