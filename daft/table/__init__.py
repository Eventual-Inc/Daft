from __future__ import annotations

from daft import DataFrame, Expression, Schema
from daft.table import MicroPartition

# for future sources, consider https://github.com/Eventual-Inc/Daft/pull/2864
# pandas/arrow/arrow_record_batches/pydict
Source = Schema | DataFrame | str | None

class Table:

    def __init__(self) -> Table:
        raise NotImplementedError("Creating a Table via __init__ is not supported")

    def name(self) -> str:
        return self._name

    def schema(self) -> Schema:
        return self._inner.schema()

    def __repr__(self) -> str:
        return f"table('{self._name}')"

    ###
    # Creation Methods
    ###

    @staticmethod
    def _from_source(name: str, source: Source = None) -> Table:
        if source is None:
            return Table._from_none(name)
        elif isinstance(source, DataFrame):
            return Table._from_df(name, source)
        elif isinstance(source, str):
            return Table._from_path(name, source)
        elif isinstance(source, Schema):
            return Table._from_schema(name, source)
        else:
            raise Exception(f"Unknown table source: {source}")

    @staticmethod
    def _from_df(name: str, df: DataFrame) -> Table:
        t = Table.__new__(Table)
        t._name = name
        t._inner = df
        return t

    @staticmethod
    def _from_none(name: str) -> Table:
        t = Table.__new__(Table)
        t._name = name
        t._inner = DataFrame._from_pylist([])
        return t

    @staticmethod
    def _from_path(self, name: str, path: str) -> Table:
        raise NotImplementedError("Table._from_path")

    @staticmethod
    def _from_schema(name: str, schema: Schema) -> Table:
        t = Table.__new__(Table)
        t._name = name
        t._inner = DataFrame._from_tables(MicroPartition.empty(schema))
        return t

    ###
    # DataFrame Methods
    ###

    def read(self) -> DataFrame:
        return self._inner

    def show(self, n: int = 8) -> None:
        return self._inner.show(n)


__all__ = [
    "Table",
]
