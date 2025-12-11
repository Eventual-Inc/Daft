"""KV Store implementations and utilities.

This module provides key-value store functionality with support for multiple backends
including Lance, LMDB, and in-memory implementations. It follows the same pattern
as daft.ai.Provider and daft.catalog.Catalog for session-based configuration management.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class LanceConfig:
    """Configuration for Lance-based KV store."""

    uri: str
    mode: str = "append"
    max_rows_per_file: int = 1024 * 1024
    max_rows_per_group: int = 1024


@dataclass
class LMDBConfig:
    """Configuration for LMDB-based KV store."""

    path: str
    map_size: int = 1024 * 1024 * 1024  # 1GB
    max_dbs: int = 10
    readonly: bool = False
    sync: bool = True
    metasync: bool = True
    writemap: bool = False
    max_readers: int = 126


@dataclass
class MemoryConfig:
    """Configuration for in-memory KV store."""

    name: str


class KVConfig:
    """Unified configuration for KV Store operations."""

    def __init__(self) -> None:
        self._lance_config: LanceConfig | None = None
        self._lmdb_config: LMDBConfig | None = None
        self._memory_config: MemoryConfig | None = None

    @classmethod
    def from_lance(
        cls,
        uri: str,
        mode: str = "append",
        max_rows_per_file: int = 1024 * 1024,
        max_rows_per_group: int = 1024,
    ) -> KVConfig:
        """Create a KVConfig with Lance backend configuration.

        Args:
            uri: URI to the Lance dataset
            mode: Write mode ('append', 'overwrite', 'create')
            max_rows_per_file: Maximum number of rows per file
            max_rows_per_group: Maximum number of rows per group

        Returns:
            KVConfig: Configured KV config with Lance backend
        """
        config = cls()
        config._lance_config = LanceConfig(
            uri=uri,
            mode=mode,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
        )
        return config

    @classmethod
    def from_lmdb(
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
        config = cls()
        config._lmdb_config = LMDBConfig(
            path=path,
            map_size=map_size,
            max_dbs=max_dbs,
            readonly=readonly,
            sync=sync,
            metasync=metasync,
            writemap=writemap,
            max_readers=max_readers,
        )
        return config

    @classmethod
    def from_memory(cls, name: str) -> KVConfig:
        """Create a KVConfig with memory backend configuration.

        Args:
            name: Name of the memory store

        Returns:
            KVConfig: Configured KV config with memory backend
        """
        config = cls()
        config._memory_config = MemoryConfig(name=name)
        return config

    def to_json(self) -> str:
        """Convert to JSON string."""
        # Convert to dict, handling nested dataclasses
        config_dict = {}
        if self._lance_config is not None:
            config_dict["lance"] = {
                "uri": self._lance_config.uri,
                "mode": self._lance_config.mode,
                "max_rows_per_file": self._lance_config.max_rows_per_file,
                "max_rows_per_group": self._lance_config.max_rows_per_group,
            }
        if self._lmdb_config is not None:
            config_dict["lmdb"] = {
                "path": self._lmdb_config.path,
                "map_size": self._lmdb_config.map_size,
                "max_dbs": self._lmdb_config.max_dbs,
                "readonly": self._lmdb_config.readonly,
                "sync": self._lmdb_config.sync,
                "metasync": self._lmdb_config.metasync,
                "writemap": self._lmdb_config.writemap,
                "max_readers": self._lmdb_config.max_readers,
            }
        if self._memory_config is not None:
            config_dict["memory"] = {
                "name": self._memory_config.name,
            }
        return json.dumps(config_dict, separators=(",", ":"))


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


# Export functions
__all__ = [
    "KVConfig",
    "KVStore",
    "KVStoreImportError",
    "LMDBConfig",
    "LanceConfig",
    "MemoryConfig",
    "load_kv",
    "load_lance",
    "load_lmdb",
    "load_memory",
]
