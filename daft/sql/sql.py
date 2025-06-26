# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import inspect
import warnings
from typing import Optional

import daft
from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import LogicalPlanBuilder as _PyLogicalPlanBuilder
from daft.daft import PySqlCatalog as _PySqlCatalog
from daft.daft import sql_exec as _sql_exec
from daft.daft import sql_expr as _sql_expr
from daft.dataframe import DataFrame
from daft.exceptions import DaftCoreException
from daft.expressions import Expression
from daft.logical.builder import LogicalPlanBuilder


class SQLCatalog:
    """SQLCatalog is a simple map from table names to dataframes used in query planning.

    EXPERIMENTAL: This features is early in development and will change.
    """

    _catalog: _PySqlCatalog = None  # type: ignore

    def __post_init__(self) -> None:
        warnings.warn(
            "This is deprecated and will be removed in daft >= 0.6.0; please use `Catalog.from_pydict()`.",
            DeprecationWarning,
            stacklevel=2,
        )

    def __init__(self, tables: dict[str, DataFrame]) -> None:
        """Create a new SQLCatalog from a dictionary of table names to dataframes."""
        self._catalog = _PySqlCatalog.new()
        for name, df in tables.items():
            self._catalog.register_table(name, df._get_current_builder()._builder)

    def __str__(self) -> str:
        return str(self._catalog)

    def register_table(self, name: str, df: DataFrame) -> None:
        self._catalog.register_table(name, df._get_current_builder()._builder)

    def _copy_from(self, other: "SQLCatalog") -> None:
        self._catalog.copy_from(other._catalog)


@PublicAPI
def sql_expr(sql: str) -> Expression:
    """Parses a SQL string into a Daft Expression.

    This function allows you to create Daft Expressions from SQL snippets, which can then be used
    in Daft operations or combined with other Daft Expressions.

    Args:
        sql (str): A SQL string to be parsed into a Daft Expression.

    Returns:
        Expression: A Daft Expression representing the parsed SQL.

    Examples:
        Create a simple SQL expression:

        >>> import daft
        >>> expr = daft.sql_expr("1 + 2")
        >>> print(expr)
        lit(1) + lit(2)

        Use SQL expression in a Daft DataFrame operation:

        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("c", daft.sql_expr("a + b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ c     │
        │ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╡
        │ 1     ┆ 4     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 7     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 9     │
        ╰───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        `daft.sql_expr` is also called automatically for you in some DataFrame operations such as filters:

        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> result = df.where("x < 3 AND y > 4")
        >>> result.show()
        ╭───────┬───────╮
        │ x     ┆ y     │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 2     ┆ 5     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    return Expression._from_pyexpr(_sql_expr(sql))


@PublicAPI
def sql(
    sql: str,
    catalog: Optional[SQLCatalog] = None,
    register_globals: bool = True,
    **bindings: DataFrame,
) -> DataFrame:
    """Run a SQL query, returning the results as a DataFrame.

    Args:
        sql (str): SQL query to execute
        catalog (SQLCatalog, optional): Catalog of tables to use in the query.
            Defaults to None, in which case a catalog will be built from variables
            in the callers scope.
        register_globals (bool, optional): Whether to incorporate global
            variables into the supplied catalog, in which case a copy of the
            catalog will be made and the original not modified. Defaults to True.
        **bindings: (DataFrame): Additional DataFrame bindings (CTEs) to use for this query.

    Returns:
        DataFrame: Dataframe containing the results of the query

    Warning:
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

        A more complex example using CTE bindings to create a named subquery (DataFrame) called `"my_df"`, which can then be referenced from inside your SQL statement.

        >>> import daft
        >>> from daft.sql import SQLCatalog
        >>>
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
        >>>
        >>> # Register dataframes as table expressions using a python dictionary.
        >>> bindings = {"my_df": df}
        >>>
        >>> daft.sql("SELECT a FROM my_df", **bindings).show()
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
    """
    # This the CTE bindings map which is built in the order globals->catalog->ctes.
    py_ctes: dict[str, _PyLogicalPlanBuilder] = {}

    # 1. Add all python DataFrame variables which are in scope.
    if register_globals:
        try:
            # Caller is back from func, annotation
            caller_frame = inspect.currentframe().f_back.f_back  # type: ignore
            caller_vars = {**caller_frame.f_globals, **caller_frame.f_locals}  # type: ignore
        except AttributeError as exc:
            # some interpreters might not implement currentframe; all reasonable
            # errors above should be AttributeError
            raise DaftCoreException(
                "Cannot get caller environment, please provide CTEs and set `register_globals=False`."
            ) from exc
        for alias, variable in caller_vars.items():
            if isinstance(variable, DataFrame):
                py_ctes[alias] = variable._builder._builder

    # 2. Add all SQLCatalog names for backwards compatibility.
    if catalog:
        warnings.warn(
            "The `catalog` argument is deprecated and will be removed in daft >= 0.6.0. Please use `ctes` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        py_ctes.update(catalog._catalog.to_pydict())

    # 3. Add explicit CTEs last so these can't be shadowed.
    for alias, df in bindings.items():
        py_ctes[alias] = df._builder._builder

    py_sess = daft.current_session()._session
    py_config = get_context().daft_planning_config
    py_object = _sql_exec(sql, py_sess, py_ctes, py_config)

    if py_object is None:
        # for backwards compatibility on the return type i.e. don't introduce nullability
        return DataFrame._from_pydict({})
    elif isinstance(py_object, _PyLogicalPlanBuilder):
        return DataFrame(LogicalPlanBuilder(py_object))
    else:
        raise ValueError(f"Unsupported return type from sql exec: {type(py_object)}")
