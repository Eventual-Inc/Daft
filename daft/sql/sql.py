# isort: dont-add-import: from __future__ import annotations

import inspect
from typing import Optional

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


@PublicAPI
def sql(sql: str, catalog: Optional[SQLCatalog] = None, register_globals: bool = True) -> DataFrame:
    """Run a SQL query, returning the results as a DataFrame

    .. WARNING::
        This features is early in development and will likely experience API changes.

    Examples:

        A simple example joining 2 dataframes together using a SQL statement, relying on Daft to detect the names of
        SQL tables using their corresponding Python variable names.

        >>> import daft
        >>>
        >>> df1 = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
        >>> df2 = daft.from_pydict({"a": [1, 2, 3], "c": ["daft", None, None]})
        >>>
        >>> # Daft automatically detects `df1` and `df2` from your Python global namespace
        >>> result_df = daft.sql("SELECT * FROM df1 JOIN df2 ON df1.a = df2.a")
        >>> result_df.show()
        ╭───────┬──────┬──────╮
        │ a     ┆ b    ┆ c    │
        │ ---   ┆ ---  ┆ ---  │
        │ Int64 ┆ Utf8 ┆ Utf8 │
        ╞═══════╪══════╪══════╡
        │ 1     ┆ foo  ┆ daft │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 2     ┆ bar  ┆ None │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 3     ┆ baz  ┆ None │
        ╰───────┴──────┴──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        A more complex example using a SQLCatalog to create a named table called `"my_table"`, which can then be referenced from inside your SQL statement.

        >>> import daft
        >>> from daft.sql import SQLCatalog
        >>>
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
        >>>
        >>> # Register dataframes as tables in SQL explicitly with names
        >>> catalog = SQLCatalog({"my_table": df})
        >>>
        >>> daft.sql("SELECT a FROM my_table", catalog=catalog).show()
        ╭───────╮
        │ a     │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 1     │
        ├╌╌╌╌╌╌╌┤
        │ 2     │
        ├╌╌╌╌╌╌╌┤
        │ 3     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

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
