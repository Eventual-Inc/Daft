"""Lance-based KV Store implementation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import DataType

if TYPE_CHECKING:
    from types import ModuleType

    from daft.io import IOConfig

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
        io_config: IOConfig | None = None,
        **kwargs: Any,
    ) -> None:
        if uri is None:
            raise ValueError("uri is required for Lance KV store")

        self._name = name or "lance_kv_store"
        self._uri = uri
        self._io_config = io_config
        self._kwargs = kwargs

        self._kv_config = KVConfig.from_lance(
            uri=uri,
            io_config=io_config,
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
    def io_config(self) -> IOConfig | None:
        return self._io_config

    def __repr__(self) -> str:
        return f"LanceKVStore(name='{self._name}', uri='{self._uri}', io_config={self._io_config})"

    def __str__(self) -> str:
        return f"Lance KV Store '{self._name}' at {self._uri}"

    def put(self, key: str, value: Any) -> None:
        raise NotImplementedError("Use kv_put via KVConfig/ScalarFunction path for Lance")

    def get(self, key: str) -> Any:
        raise NotImplementedError("Use kv_get via KVConfig/ScalarFunction path for Lance")
