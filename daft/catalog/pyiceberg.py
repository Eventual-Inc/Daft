from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog
    from pyiceberg.table import Table as PyIcebergTable

    from daft.dataframe import DataFrame

from daft.catalog.python_catalog import PythonCatalog, PythonCatalogTable


class PyIcebergCatalogAdaptor(PythonCatalog):
    def __init__(self, pyiceberg_catalog: PyIcebergCatalog):
        self._catalog = pyiceberg_catalog

    def list_tables(self, prefix: str) -> list[str]:
        return [".".join(tup) for tup in self._catalog.list_tables(prefix)]

    def load_table(self, name: str) -> PyIcebergTableAdaptor:
        return PyIcebergTableAdaptor(self._catalog.load_table(name))


class PyIcebergTableAdaptor(PythonCatalogTable):
    def __init__(self, pyiceberg_table: PyIcebergTable):
        self._table = pyiceberg_table

    def to_dataframe(self) -> DataFrame:
        import daft

        return daft.read_iceberg(self._table)
