"""Window Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft.daft as native
from daft.expressions import Expression

if TYPE_CHECKING:
    from daft.window import Window


def row_number() -> Expression:
    """Return the row number of the current row (used for window functions).

    Examples:
        >>> import daft
        >>> from daft.window import Window
        >>> from daft.functions import row_number
        >>> df = daft.from_pydict({"category": ["A", "A", "A", "A", "B", "B", "B", "B"], "value": [1, 7, 2, 9, 1, 3, 3, 7]})
        >>>
        >>> # Ascending order
        >>> window = Window().partition_by("category").order_by("value")
        >>> df = df.with_column("row", row_number().over(window))
        >>> df = df.sort("category")
        >>> df.show()
        ╭──────────┬───────┬────────╮
        │ category ┆ value ┆ row    │
        │ ---      ┆ ---   ┆ ---    │
        │ String   ┆ Int64 ┆ UInt64 │
        ╞══════════╪═══════╪════════╡
        │ A        ┆ 1     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 2     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 7     ┆ 3      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 9     ┆ 4      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 1     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 3     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 3     ┆ 3      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 4      │
        ╰──────────┴───────┴────────╯
        <BLANKLINE>
        (Showing first 8 rows)

    Returns:
        Expression: An expression that returns the row number of the current row.
    """
    return Expression._from_pyexpr(native.row_number())


def rank() -> Expression:
    """Return the rank of the current row (used for window functions).

    Examples:
        >>> import daft
        >>> from daft.window import Window
        >>> from daft.functions import rank
        >>> df = daft.from_pydict({"category": ["A", "A", "A", "A", "B", "B", "B", "B"], "value": [1, 3, 3, 7, 7, 7, 4, 4]})
        >>>
        >>> window = Window().partition_by("category").order_by("value", desc=True)
        >>> df = df.with_column("rank", rank().over(window))
        >>> df = df.sort("category")
        >>> df.show()
        ╭──────────┬───────┬────────╮
        │ category ┆ value ┆ rank   │
        │ ---      ┆ ---   ┆ ---    │
        │ String   ┆ Int64 ┆ UInt64 │
        ╞══════════╪═══════╪════════╡
        │ A        ┆ 7     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ A        ┆ 1     ┆ 4      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 3      │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 3      │
        ╰──────────┴───────┴────────╯
        <BLANKLINE>
        (Showing first 8 rows)

    Returns:
        Expression: An expression that returns the rank of the current row.
    """
    return Expression._from_pyexpr(native.rank())


def dense_rank() -> Expression:
    """Return the dense rank of the current row (used for window functions).

    The dense rank is the rank of the current row without gaps.

    Examples:
        >>> import daft
        >>> from daft.window import Window
        >>> from daft.functions import dense_rank
        >>> df = daft.from_pydict({"category": ["A", "A", "A", "A", "B", "B", "B", "B"], "value": [1, 3, 3, 7, 7, 7, 4, 4]})
        >>>
        >>> window = Window().partition_by("category").order_by("value", desc=True)
        >>> df = df.with_column("dense_rank", dense_rank().over(window))
        >>> df = df.sort("category")
        >>> df.show()
        ╭──────────┬───────┬────────────╮
        │ category ┆ value ┆ dense_rank │
        │ ---      ┆ ---   ┆ ---        │
        │ String   ┆ Int64 ┆ UInt64     │
        ╞══════════╪═══════╪════════════╡
        │ A        ┆ 7     ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 2          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 1     ┆ 3          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 7     ┆ 1          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 2          │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 2          │
        ╰──────────┴───────┴────────────╯
        <BLANKLINE>
        (Showing first 8 rows)

    Returns:
        Expression: An expression that returns the dense rank of the current row.
    """
    return Expression._from_pyexpr(native.dense_rank())


def over(expr: Expression, window: Window) -> Expression:
    """Apply the expression as a window function.

    Args:
        expr: The expression to apply as a window function.
        window: The window specification (created using ``daft.Window``)
            defining partitioning, ordering, and framing.

    Examples:
        >>> import daft
        >>> from daft.functions import sum
        >>>
        >>> df = daft.from_pydict(
        ...     {
        ...         "group": ["A", "A", "A", "B", "B", "B"],
        ...         "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06"],
        ...         "value": [1, 2, 3, 4, 5, 6],
        ...     }
        ... )
        >>> window_spec = daft.Window().partition_by("group").order_by("date")
        >>> df = df.with_column("grouped_sum", sum(df["value"]).over(window_spec))
        >>> df.sort(["group", "date"]).show()
        ╭────────┬────────────┬───────┬─────────────╮
        │ group  ┆ date       ┆ value ┆ grouped_sum │
        │ ---    ┆ ---        ┆ ---   ┆ ---         │
        │ String ┆ String     ┆ Int64 ┆ Int64       │
        ╞════════╪════════════╪═══════╪═════════════╡
        │ A      ┆ 2020-01-01 ┆ 1     ┆ 6           │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A      ┆ 2020-01-02 ┆ 2     ┆ 6           │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A      ┆ 2020-01-03 ┆ 3     ┆ 6           │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B      ┆ 2020-01-04 ┆ 4     ┆ 15          │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B      ┆ 2020-01-05 ┆ 5     ┆ 15          │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B      ┆ 2020-01-06 ┆ 6     ┆ 15          │
        ╰────────┴────────────┴───────┴─────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)

    Returns:
        Expression: The result of applying this expression as a window function.
    """
    return Expression._from_pyexpr(expr._expr.over(window._spec))


def lag(expr: Expression, offset: int = 1, default: Expression | None = None) -> Expression:
    """Get the value from a previous row within a window partition.

    Args:
        expr: The expression to get the lagged value of.
        offset: The number of rows to shift backward. Must be >= 0.
        default: Value to use when no previous row exists. Can be a column reference.

    Returns:
        Expression: Value from the row `offset` positions before the current row.

    Examples:
        >>> import daft
        >>> from daft.functions import lag
        >>>
        >>> df = daft.from_pydict(
        ...     {
        ...         "category": ["A", "A", "A", "B", "B", "B"],
        ...         "value": [1, 2, 3, 4, 5, 6],
        ...         "default_val": [10, 20, 30, 40, 50, 60],
        ...     }
        ... )
        >>>
        >>> # Simple lag with null default
        >>> window = daft.Window().partition_by("category").order_by("value")
        >>> df = df.with_column("lagged", lag(df["value"], 1).over(window))
        >>>
        >>> # Lag with column reference as default
        >>> df = df.with_column("lagged_with_default", lag(df["value"], 1, default=df["default_val"]).over(window))
        >>> df.sort(["category", "value"]).show()
        ╭──────────┬───────┬─────────────┬────────┬─────────────────────╮
        │ category ┆ value ┆ default_val ┆ lagged ┆ lagged_with_default │
        │ ---      ┆ ---   ┆ ---         ┆ ---    ┆ ---                 │
        │ String   ┆ Int64 ┆ Int64       ┆ Int64  ┆ Int64               │
        ╞══════════╪═══════╪═════════════╪════════╪═════════════════════╡
        │ A        ┆ 1     ┆ 10          ┆ None   ┆ 10                  │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 2     ┆ 20          ┆ 1      ┆ 1                   │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 30          ┆ 2      ┆ 2                   │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 40          ┆ None   ┆ 40                  │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 5     ┆ 50          ┆ 4      ┆ 4                   │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 6     ┆ 60          ┆ 5      ┆ 5                   │
        ╰──────────┴───────┴─────────────┴────────┴─────────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    if default is not None:
        default = Expression._to_expression(default)
    return Expression._from_pyexpr(expr._expr.offset(-offset, default._expr if default is not None else None))


def lead(expr: Expression, offset: int = 1, default: Expression | None = None) -> Expression:
    """Get the value from a future row within a window partition.

    Args:
        expr: The expression to get the lead value of.
        offset: The number of rows to shift forward. Must be >= 0.
        default: Value to use when no future row exists. Can be a column reference.

    Returns:
        Expression: Value from the row `offset` positions after the current row.

    Examples:
        >>> import daft
        >>> from daft.functions import lead
        >>>
        >>> df = daft.from_pydict(
        ...     {
        ...         "category": ["A", "A", "A", "B", "B", "B"],
        ...         "value": [1, 2, 3, 4, 5, 6],
        ...         "default_val": [10, 20, 30, 40, 50, 60],
        ...     }
        ... )
        >>>
        >>> # Simple lead with null default
        >>> window = daft.Window().partition_by("category").order_by("value")
        >>> df = df.with_column("lead", lead(df["value"], 1).over(window))
        >>>
        >>> # Lead with column reference as default
        >>> df = df.with_column("lead_with_default", lead(df["value"], 1, default=df["default_val"]).over(window))
        >>> df.sort(["category", "value"]).show()
        ╭──────────┬───────┬─────────────┬───────┬───────────────────╮
        │ category ┆ value ┆ default_val ┆ lead  ┆ lead_with_default │
        │ ---      ┆ ---   ┆ ---         ┆ ---   ┆ ---               │
        │ String   ┆ Int64 ┆ Int64       ┆ Int64 ┆ Int64             │
        ╞══════════╪═══════╪═════════════╪═══════╪═══════════════════╡
        │ A        ┆ 1     ┆ 10          ┆ 2     ┆ 2                 │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 2     ┆ 20          ┆ 3     ┆ 3                 │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ A        ┆ 3     ┆ 30          ┆ None  ┆ 30                │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 4     ┆ 40          ┆ 5     ┆ 5                 │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 5     ┆ 50          ┆ 6     ┆ 6                 │
        ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ B        ┆ 6     ┆ 60          ┆ None  ┆ 60                │
        ╰──────────┴───────┴─────────────┴───────┴───────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
    """
    if default is not None:
        default = Expression._to_expression(default)
    return Expression._from_pyexpr(expr._expr.offset(offset, default._expr if default is not None else None))
