# isort: dont-add-import: from __future__ import annotations

from daft.api_annotations import PublicAPI
from daft.daft import PyCatalog as _PyCatalog
from daft.daft import sql as _sql
from daft.dataframe import DataFrame
from daft.logical.builder import LogicalPlanBuilder


class SQLCatalog:
    """
    SQLCatalog is a simple map from table names to dataframes used in query planning.

    EXPERIMENTAL: This features is early in development and will change.
    """

    _catalog: _PyCatalog = None  # type: ignore

    def __init__(self, tables: dict) -> None:
        """Create a new SQLCatalog from a dictionary of table names to dataframes."""
        self._catalog = _PyCatalog.new()
        for name, df in tables.items():
            self._catalog.register_table(name, df._get_current_builder()._builder)

    def __str__(self) -> str:
        return str(self._catalog)


@PublicAPI
def sql(sql: str, catalog: SQLCatalog) -> DataFrame:
    """Create a DataFrame from an SQL query.

    EXPERIMENTAL: This features is early in development and will change.

    Args:
        sql (str): SQL query to execute

    Returns:
        DataFrame: Dataframe containing the results of the query
    """
    _py_catalog = catalog._catalog
    _py_logical = _sql(sql, _py_catalog)
    return DataFrame(LogicalPlanBuilder(_py_logical))
