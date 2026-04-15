# ruff: noqa: I002
# isort: dont-add-import: from __future__ import annotations

import inspect
import re
from collections.abc import Sequence
from typing import Any

import daft
from daft.api_annotations import PublicAPI
from daft.context import get_context
from daft.daft import LogicalPlanBuilder as _PyLogicalPlanBuilder
from daft.daft import sql_exec as _sql_exec
from daft.daft import sql_expr as _sql_expr
from daft.dataframe import DataFrame
from daft.exceptions import DaftCoreException
from daft.expressions import Expression
from daft.logical.builder import LogicalPlanBuilder


def _apply_parameters(sql: str, params: Sequence[Any] | dict[str, Any]) -> str:
    """Apply parameterized query substitution.

    Supports three parameter styles:
    1. Auto-incremented: ? (sequential substitution from list)
    2. Positional: $1, $2, $3... (positional substitution from list)
    3. Named: :name (named substitution from dict)
    """
    if not params:
        return sql

    # Handle named parameters (:name style)
    if isinstance(params, dict):

        def replace_named(match: re.Match) -> str:
            param_name = match.group(1)
            if param_name not in params:
                raise ValueError(f"Named parameter '{param_name}' not found in parameters")
            return _format_sql_value(params[param_name])

        return re.sub(r"(?<!:):(\w+)", replace_named, sql)

    # Handle positional parameters (? and $n style)
    if isinstance(params, (list, tuple)):
        has_dollar = bool(re.search(r"\$\d+", sql))
        has_question = "?" in sql
        has_named = bool(re.search(r"(?<!:):\w+", sql))

        style_count = sum([has_dollar, has_question, has_named])
        if style_count > 1:
            raise ValueError(
                "Cannot mix parameter styles. Use only one of: "
                "? (auto-incremented), $n (positional), or :name (named)."
            )

        # Handle $1, $2, $3... style
        if has_dollar:

            def replace_indexed(match: re.Match) -> str:
                index = int(match.group(1)) - 1
                if index < 0 or index >= len(params):
                    raise ValueError(f"Positional parameter ${match.group(1)} out of range")
                return _format_sql_value(params[index])

            return re.sub(r"\$(\d+)", replace_indexed, sql)

        # Handle ? style (sequential)
        param_iter = iter(params)

        def replace_question(match: re.Match) -> str:
            try:
                return _format_sql_value(next(param_iter))
            except StopIteration:
                raise ValueError("Not enough parameters provided for '?' placeholders")

        result = re.sub(r"\?", replace_question, sql)

        # Check for unused parameters
        try:
            next(param_iter)
            raise ValueError("Too many parameters provided for '?' placeholders")
        except StopIteration:
            pass

        return result

    raise ValueError(f"Unsupported parameter type: {type(params)}. Expected list, tuple, or dict.")


def _format_sql_value(value: Any) -> str:
    """Format a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    # String and other types - wrap in single quotes, escaping internal quotes
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


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
    register_globals: bool = True,
    params: Sequence[Any] | dict[str, Any] | None = None,
    **bindings: DataFrame,
) -> DataFrame:
    """Run a SQL query, returning the results as a DataFrame.

    Args:
        sql (str): SQL query to execute. Can include parameter placeholders:
            - `?` for auto-incremented parameters (sequential substitution)
            - `$1`, `$2`, etc. for positional parameters
            - `:name` for named parameters
        register_globals (bool, optional): Whether to incorporate global
            variables into the supplied catalog, in which case a copy of the
            catalog will be made and the original not modified. Defaults to True.
        params (Sequence or dict, optional): Parameters to substitute into the SQL query.
            - For `?` and `$n` placeholders: provide a list/tuple of values
            - For `:name` placeholders: provide a dict mapping names to values
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
        ╭───────┬────────┬────────╮
        │ a     ┆ b      ┆ c      │
        │ ---   ┆ ---    ┆ ---    │
        │ Int64 ┆ String ┆ String │
        ╞═══════╪════════╪════════╡
        │ 1     ┆ foo    ┆ daft   │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2     ┆ bar    ┆ None   │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 3     ┆ baz    ┆ None   │
        ╰───────┴────────┴────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        A more complex example using CTE bindings to create a named subquery (DataFrame) called `"my_df"`, which can then be referenced from inside your SQL statement.

        >>> import daft
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

        **Parameterized Queries:**

        Use auto-incremented parameters with `?`:

        >>> import daft
        >>> df = daft.from_pydict({"name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})
        >>> result = daft.sql("SELECT * FROM df WHERE age > ? AND name LIKE ?", params=[28, "C%"])
        >>> result.show()
        ╭─────────┬───────╮
        │ name    ┆ age   │
        │ ---     ┆ ---   │
        │ String  ┆ Int64 │
        ╞═════════╪═══════╡
        │ Charlie ┆ 35    │
        ╰─────────┴───────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

        Use positional parameters with `$1`, `$2`:

        >>> result = daft.sql("SELECT * FROM df WHERE age >= $1 AND name = $2", params=[25, "Alice"])
        >>> result.show()
        ╭───────┬──────╮
        │ name  ┆ age  │
        │ ---   ┆ ---  │
        │ String┆ Int64│
        ╞═══════╪══════╡
        │ Alice ┆ 25   │
        ╰───────┴──────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

        Use named parameters with `:name`:

        >>> result = daft.sql("SELECT * FROM df WHERE age >= :age AND name = :name", params={"age": 25, "name": "Alice"})
        >>> result.show()
        ╭───────┬──────╮
        │ name  ┆ age  │
        │ ---   ┆ ---  │
        │ String┆ Int64│
        ╞═══════╪══════╡
        │ Alice ┆ 25   │
        ╰───────┴──────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    # This the CTE bindings map which is built in the order globals->catalog->ctes.
    py_ctes: dict[str, _PyLogicalPlanBuilder] = {}

    # Apply parameterized query substitution if params are provided
    if params is not None:
        sql = _apply_parameters(sql, params)

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

    # 2. Add explicit CTEs last so these can't be shadowed.
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
