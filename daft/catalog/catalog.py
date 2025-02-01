from __future__ import annotations

from daft.dataframe.dataframe import DataFrame
from daft.logical.schema import Schema

""".
------------------------------------------
BACKLOG
------------------------------------------
- class Identifier
- class Namespace
- class Name
- Table._from_df
- Table._from_path
"""

# for future sources, consider https://github.com/Eventual-Inc/Daft/pull/2864
Source = Schema | DataFrame | str


class Table:
    _name: str
    _schema: Schema

    def __init__(self) -> Table:
        raise NotImplementedError("Creating a Table via __init__ is not supported")

    def __repr__(self) -> str:
        return f"table('{self._name}')"

    @staticmethod
    def _from_source(name: str, source: Source) -> Table:
        if isinstance(source, DataFrame):
            return Table._from_df(name, source)
        elif isinstance(source, str):
            return Table._from_path(name, source)
        elif isinstance(source, Schema):
            return Table._from_schema(name, source)
        else:
            raise Exception(f"Unknown table source: {source}")

    @staticmethod
    def _from_df(self, name: str, df: DataFrame) -> Table:
        raise NotImplementedError("Table._from_df")

    @staticmethod
    def _from_path(self, name: str, path: str) -> Table:
        raise NotImplementedError("Table._from_path")

    @staticmethod
    def _from_schema(name: str, schema: Schema) -> Table:
        t = Table.__new__(Table)
        t._name = name
        t._schema = schema
        return t

    def name(self) -> str:
        return self._name

    def schema(self) -> Schema:
        return self._schema

    # def properties(self):
    #     raise Exception("properties not implemented")

    # def insert(self):
    #     """Insert..."""
    #     raise Exception("not implemented")

    # def update(self):
    #     """??"""
    #     raise Exception("not implemented")

    # def delete(self):
    #     """Delete..."""
    #     raise Exception("not implemented")

    def scan(self):
        raise Exception("read not implemented")

    def show(self):
        raise Exception("show not implemented")


class Catalog:
    def __init__(self, name: str):
        self._name = name
        self._tables = dict()

    def __repr__(self) -> str:
        return f"catalog('{self._name}')"

    def name(self) -> str:
        return self._name

    # ---

    def create_namespace(self):
        raise Exception("not implemented")

    def create_schema(self):
        raise Exception("not implemented")

    def create_table(self, name: str, source: Source) -> Table:
        """Creates a new table scoped to this catalog and returns it"""
        table = Table._from_source(name, source)
        self._tables[name] = table
        return table

    def create_view(self):
        raise Exception("create_view not implemented")

    # ---

    def create_external_table(self):
        raise Exception("create_external_table not implemented")

    # ---

    def get_table(self, name: str) -> Table:
        raise Exception("not implemented")

    # ---

    def list_namespaces(self):
        raise Exception("not implemented")

    def list_schemas(self):
        raise Exception("not implemented")

    def list_tables(self):
        raise Exception("not implemented")

    def list_views(self):
        raise Exception("not implemented")

    # ---

    def drop_namespace(self):
        raise Exception("not implemented")

    def drop_schema(self):
        raise Exception("not implemented")

    def drop_table(self):
        raise Exception("not implemented")

    def drop_view(self):
        raise Exception("not implemented")
