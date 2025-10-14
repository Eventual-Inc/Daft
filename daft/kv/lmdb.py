"""LMDB-based KV Store implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.kv import KVConfig

from daft.kv import KVConfig, KVStore, LMDBConfig


class LMDBKVStore(KVStore):
    """LMDB-based KV Store implementation."""

    def __init__(
        self,
        name: str | None = None,
        path: str | None = None,
        map_size: int = 1024 * 1024 * 1024,
        max_dbs: int = 10,
        readonly: bool = False,
        sync: bool = True,
        metasync: bool = True,
        writemap: bool = False,
        max_readers: int = 126,
        **kwargs: Any,
    ) -> None:
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

        self._kv_config = KVConfig.from_lmdb(
            path=path,
            map_size=map_size,
            max_dbs=max_dbs,
            readonly=readonly,
            sync=sync,
            metasync=metasync,
            writemap=writemap,
            max_readers=max_readers,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def backend_type(self) -> str:
        return "lmdb"

    def get_config(self) -> KVConfig:
        return self._kv_config

    @property
    def path(self) -> str:
        return self._path

    @property
    def map_size(self) -> int:
        return self._map_size

    @property
    def max_dbs(self) -> int:
        return self._max_dbs

    @property
    def readonly(self) -> bool:
        return self._readonly

    @property
    def sync(self) -> bool:
        return self._sync

    @property
    def metasync(self) -> bool:
        return self._metasync

    @property
    def writemap(self) -> bool:
        return self._writemap

    @property
    def max_readers(self) -> int:
        return self._max_readers

    def with_map_size(self, map_size: int) -> LMDBKVStore:
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
        return (
            f"LMDBKVStore(name='{self._name}', path='{self._path}', "
            f"map_size={self._map_size}, readonly={self._readonly})"
        )

    def __str__(self) -> str:
        return f"LMDB KV Store '{self._name}' at {self._path}"
