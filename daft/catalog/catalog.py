"""Goals.

- [] use tables without catalogs
- [] use catalogs without a session (implicit)
- [] unity attach / create_external_catalog
- [] iceberg attach / create_external_catalog
- [] clean read_table with iceberg (impl) specific options
"""

# class Namespace:

#     def __init__(self) -> None:
#         pass

# class Name:
#     def __init__(self) -> None:
#         pass


class Table:
    def __init__(self, name: str):
        self._name = name

    def __repr__(self) -> str:
        return f"table({self._name})"

    def name(self) -> str:
        return self._name

    def read():
        raise Exception("read not implemented")

    def schema():
        raise Exception("schema not implemented")

    def properties():
        raise Exception("properties not implemented")


class Catalog:
    def __init__(self, name: str = "default"):
        self._name = name
        self._tables = dict()

    def __repr__(self) -> str:
        return f"catalog({self._name})"

    def name(self) -> str:
        return self._name

    # ---

    def create_namespace(self):
        raise Exception("create_namespace not implemented")

    def create_schema(self):
        raise Exception("create_schema not implemented")

    def create_table(self, name: str, schema: Schema, if_not_exists=False) -> Table:
        """Create a new table"""
        table = Table(name)
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


class Session:
    def __init__(self):
        self._catalogs = dict()

    # verb?
    def attach_catalog(self, catalog: Catalog):
        raise Exception("attach not implemented")

    # verb?
    def detach_catalog(self):
        raise Exception("detach not implemented")
