from daft.daft import PyCatalog as _PyCatalog

class SQLCatalog:
    """SQLCatalog is a simple map from table names to dataframes used in query planning."""
    _catalog: _PyCatalog = None # type: ignore

    def __init__(self, tables: dict) -> None:
        catalog = _PyCatalog.new()
        for table_name, table_df in tables.items():
            catalog.register_table(table_name, table_df)
