"""Goals.

"""


# class Namespace:

#     def __init__(self) -> None:
#         pass

# class Name:
#     def __init__(self) -> None:
#         pass


class Table:
    def __init__(self, name: str):
        raise NotImplementedError("Creating a Table via __init__ is not supported")

    def __repr__(self) -> str:
        return f"table({self._name})"

    def name(self) -> str:
        return self._name

    def scan(self):
        raise Exception("read not implemented")

    def schema(self):
        raise Exception("schema not implemented")

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
    
    def show(self):
        raise Exception("show not implemented")


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
        raise Exception("not implemented")

    def create_schema(self):
        raise Exception("not implemented")

    def create_table(self, name: str) -> Table:
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
