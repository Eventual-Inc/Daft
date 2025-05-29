"""DEPRECATED: Please use `from daft.catalog import Catalog, Table`."""

from __future__ import annotations

from daft.catalog import Catalog, Table

# TODO deprecated catalog APIs #3819
PythonCatalog = Catalog

# TODO deprecated catalog APIs #3819
PythonCatalogTable = Table
