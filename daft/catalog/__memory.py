from __future__ import annotations

from daft.catalog import Catalog

class MemoryCatalog(Catalog):

    def __init__(self, name: str):
        self._name: str = name

    def __repr__(self) -> str:
        return f"MemoryCatalog('{self._name}')"

    def name(self) -> str:
        return self._name
