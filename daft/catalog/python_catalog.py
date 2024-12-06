from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.dataframe import DataFrame


class PythonCatalog:
    """Wrapper class for various Python implementations of Data Catalogs."""

    @abstractmethod
    def list_tables(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def load_table(self, name: str) -> PythonCatalogTable: ...


class PythonCatalogTable:
    """Wrapper class for various Python implementations of Data Catalog Tables."""

    @abstractmethod
    def to_dataframe(self) -> DataFrame: ...
