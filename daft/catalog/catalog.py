"""Goals.

- [] use tables without catalogs
- [] use catalogs without a session (implicit)
- [] unity attach / create_external_catalog
- [] iceberg attach / create_external_catalog
"""


class Table:
    def __init__(self, name: str):
        self._name = name

    def __repr__(self) -> str:
        return f"table({self._name})"


class Catalog:

    def __init__(self, name: str = "default"):
        self._name = name
        self._tables = dict()

    def __repr__(self) -> str:
        return f"catalog({self._name})"

    # CREATE ACTIONS

    def create_namespace(self):
        raise Exception("create_namespace not implemented")

    def create_schema(self):
        raise Exception("create_schema not implemented")

    def create_table(self, name: str) -> Table:
        table = Table(name)
        self._tables[name] = table
        return table

    def create_view(self):
        raise Exception("create_view not implemented")

    # CREATE EXTERNAL ACTIONS

    def create_external_table(self):
        raise Exception("create_external_table not implemented")

    # GET ACTIONS

    def get_table(self, name: str) -> Table:
        raise Exception("not implemented")

    # LIST ACTIONS

    def list_namespaces(self):
        raise Exception("not implemented")

    def list_schemas(self):
        raise Exception("not implemented")

    def list_tables(self):
        raise Exception("not implemented")

    def list_views(self):
        raise Exception("not implemented")

    # DROP ACTIONS

    def drop_namespace(self):
        raise Exception("not implemented")

    def drop_schema(self):
        raise Exception("not implemented")

    def drop_table(self):
        raise Exception("not implemented")

    def drop_view(self):
        raise Exception("not implemented")


class Session:
    def __init__(self):
        self._catalogs = dict()

    # verb?
    def attach_catalog(self, catalog: Catalog):
        raise Exception("attach not implemented")

    # verb?
    def detach_catalog(self):
        raise Exception("detach not implemented")
