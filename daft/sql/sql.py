# isort: dont-add-import: from __future__ import annotations

import inspect
from typing import Optional, overload

from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import PyCatalog as _PyCatalog
from daft.daft import sql as _sql
from daft.daft import sql_expr as _sql_expr
from daft.dataframe import DataFrame
from daft.exceptions import DaftCoreException
from daft.expressions import Expression
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

    def _copy_from(self, other: "SQLCatalog") -> None:
        self._catalog.copy_from(other._catalog)


@PublicAPI
def sql_expr(sql: str) -> Expression:
    return Expression._from_pyexpr(_sql_expr(sql))


@overload
def sql(sql: str) -> DataFrame: ...


@overload
def sql(sql: str, catalog: SQLCatalog, register_globals: bool = ...) -> DataFrame: ...


@PublicAPI
def sql(sql: str, catalog: Optional[SQLCatalog] = None, register_globals: bool = True) -> DataFrame:
    """Create a DataFrame from an SQL query.

    EXPERIMENTAL: This features is early in development and will change.

    Args:
        sql (str): SQL query to execute
        catalog (SQLCatalog, optional): Catalog of tables to use in the query.
            Defaults to None, in which case a catalog will be built from variables
            in the callers scope.
        register_globals (bool, optional): Whether to incorporate global
            variables into the supplied catalog, in which case a copy of the
            catalog will be made and the original not modified. Defaults to True.

    Returns:
        DataFrame: Dataframe containing the results of the query
    """
    if register_globals:
        try:
            # Caller is back from func, analytics, annotation
            caller_frame = inspect.currentframe().f_back.f_back.f_back  # type: ignore
            caller_vars = {**caller_frame.f_globals, **caller_frame.f_locals}  # type: ignore
        except AttributeError as exc:
            # some interpreters might not implement currentframe; all reasonable
            # errors above should be AttributeError
            raise DaftCoreException("Cannot get caller environment, please provide a catalog") from exc
        catalog_ = SQLCatalog({k: v for k, v in caller_vars.items() if isinstance(v, DataFrame)})
        if catalog is not None:
            catalog_._copy_from(catalog)
        catalog = catalog_
    elif catalog is None:
        raise DaftCoreException("Must supply a catalog if register_globals is False")

    planning_config = get_context().daft_planning_config

    _py_catalog = catalog._catalog
    _py_logical = _sql(sql, _py_catalog, planning_config)
    return DataFrame(LogicalPlanBuilder(_py_logical))
