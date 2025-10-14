from __future__ import annotations

from typing import Any

from daft.datatype import DataType
from daft.kv import KVStore

# No direct native class; memory operations go through Rust ScalarFunctions via daft.functions.kv


class MemoryKVStore(KVStore):
    """Session-attached memory KV store descriptor."""

    def __init__(self, name: str | None = None, **_options: Any) -> None:
        self._name = name or "memory"

    @property
    def name(self) -> str:
        return self._name

    @property
    def backend_type(self) -> str:
        return "memory"

    def get_config(self) -> Any:
        return type("KVConfig", (), {"to_json": lambda self: "{}"})()

    def schema_fields(self) -> list[str]:
        n = self._name.lower()
        if "embedding" in n:
            return ["embedding"]
        if "meta" in n:
            return ["metadata"]
        return ["value"]

    def schema(self) -> dict[str, DataType]:
        fields: dict[str, DataType] = {}
        for f in self.schema_fields():
            if f == "embedding":
                fields[f] = DataType.list(DataType.float64())
            else:
                fields[f] = DataType.python()
        return fields

    def __repr__(self) -> str:
        return f"MemoryKVStore(name={self._name!r})"

    def __str__(self) -> str:
        return self.__repr__()
