from __future__ import annotations

from typing import Callable

from daft.dependencies import pa


class DaftExtension(pa.ExtensionType):  # type: ignore[misc]
    # Never directly import this! Always use the _ensure_registered_super_ext_type function in daft.datatype.
    def __init__(self, dtype: pa.DataType, metadata: bytes = b"") -> None:
        # attributes need to be set first before calling
        # super init (as that calls serialize)
        self._metadata = metadata
        super().__init__(dtype, "daft.super_extension")

    def __reduce__(
        self,
    ) -> tuple[Callable[[pa.DataType, bytes], DaftExtension], tuple[pa.DataType, bytes]]:
        return type(self).__arrow_ext_deserialize__, (self.storage_type, self.__arrow_ext_serialize__())

    def __arrow_ext_serialize__(self) -> bytes:
        return self._metadata

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type: pa.DataType, serialized: bytes) -> DaftExtension:
        return cls(storage_type, serialized)

    def __arrow_ext_equals__(self, other: pa.ExtensionType) -> bool:
        return self.storage_type == other.storage_type and self._metadata == other._metadata
