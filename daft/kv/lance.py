"""Lance-based KV Store implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import DataType

if TYPE_CHECKING:
    from types import ModuleType

    class _NativeModule(ModuleType):
        def kv_put(self, kind: str, name: str, key: str, value: Any) -> None: ...
        def kv_get(self, kind: str, name: str, key: str) -> Any: ...

    native: _NativeModule
else:
    pass

if TYPE_CHECKING:
    from daft.kv import KVConfig

from daft.kv import KVConfig, KVStore


class LanceKVStore(KVStore):
    """Lance-based KV Store implementation."""

    def __init__(
        self,
        name: str | None = None,
        uri: str | None = None,
        mode: str = "append",
        max_rows_per_file: int = 1024 * 1024,
        max_rows_per_group: int = 1024,
        **kwargs: Any,
    ) -> None:
        if uri is None:
            raise ValueError("uri is required for Lance KV store")

        self._name = name or "lance_kv_store"
        self._uri = uri
        self._mode = mode
        self._max_rows_per_file = max_rows_per_file
        self._max_rows_per_group = max_rows_per_group
        self._kwargs = kwargs

        self._kv_config = KVConfig.from_lance(
            uri=uri,
            mode=mode,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def backend_type(self) -> str:
        return "lance"

    def get_config(self) -> KVConfig:
        return self._kv_config

    def schema_fields(self) -> list[str]:
        return ["value"]

    def schema(self) -> dict[str, DataType]:
        return {"value": DataType.python()}

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def max_rows_per_file(self) -> int:
        return self._max_rows_per_file

    @property
    def max_rows_per_group(self) -> int:
        return self._max_rows_per_group

    def with_mode(self, mode: str) -> LanceKVStore:
        return LanceKVStore(
            name=self._name,
            uri=self._uri,
            mode=mode,
            max_rows_per_file=self._max_rows_per_file,
            max_rows_per_group=self._max_rows_per_group,
            **self._kwargs,
        )

    def with_max_rows_per_file(self, max_rows_per_file: int) -> LanceKVStore:
        return LanceKVStore(
            name=self._name,
            uri=self._uri,
            mode=self._mode,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=self._max_rows_per_group,
            **self._kwargs,
        )

    def with_max_rows_per_group(self, max_rows_per_group: int) -> LanceKVStore:
        return LanceKVStore(
            name=self._name,
            uri=self._uri,
            mode=self._mode,
            max_rows_per_file=self._max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
            **self._kwargs,
        )

    def __repr__(self) -> str:
        return (
            f"LanceKVStore(name='{self._name}', uri='{self._uri}', "
            f"mode='{self._mode}', max_rows_per_file={self._max_rows_per_file}, "
            f"max_rows_per_group={self._max_rows_per_group})"
        )

    def __str__(self) -> str:
        return f"Lance KV Store '{self._name}' at {self._uri}"

    def put(self, key: str, value: Any) -> None:
        raise NotImplementedError("Use kv_put via KVConfig/ScalarFunction path for Lance")

    def get(self, key: str) -> Any:
        raise NotImplementedError("Use kv_get via KVConfig/ScalarFunction path for Lance")
