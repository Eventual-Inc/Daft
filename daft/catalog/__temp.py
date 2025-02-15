from __future__ import annotations

from daft.catalog import Catalog, Table, TableSource
from daft.dataframe.dataframe import DataFrame
from daft.logical.schema import Schema
from daft.recordbatch.micropartition import MicroPartition


class TempCatalog(Catalog):
    """A temporary catalog scoped to a given session."""

    def __init__(self, name: str):
        self._name: str = name

    def __repr__(self) -> str:
        return f"TempCatalog('{self._name}')"

    def name(self) -> str:
        return self._name


class TempTable(Table):
    """A temp table holds a reference to an existing dataframe."""

    _inner: DataFrame

    def __init__(self, inner: DataFrame) -> Table:
        self._inner = inner

    def name(self) -> str:
        return self._name

    def schema(self) -> Schema:
        return self._inner.schema()

    def __repr__(self) -> str:
        return f"table('{self._name}')"

    ###
    # DataFrame Methods
    ###

    def read(self) -> DataFrame:
        return self._inner

    def show(self, n: int = 8) -> None:
        return self._inner.show(n)
