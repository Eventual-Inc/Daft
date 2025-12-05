"""Columnar Functions."""

from __future__ import annotations

from daft.expressions import Expression, col
from daft.functions.list import to_list


def columns_sum(*exprs: Expression | str) -> Expression:
    """Sum values across columns.

    Args:
        exprs: The columns to sum.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_sum
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("sum", columns_sum("a", "b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ sum   │
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
    """
    if not exprs:
        raise ValueError("columns_sum requires at least one expression")
    exprs_list = [col(e) if isinstance(e, str) else e for e in exprs]
    return to_list(*exprs_list).list_sum().alias("columns_sum")


def columns_mean(*exprs: Expression | str) -> Expression:
    """Average values across columns. Akin to `columns_avg`.

    Args:
        exprs: The columns to average.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_mean
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("mean", columns_mean("a", "b"))
        >>> df.show()
        ╭───────┬───────┬─────────╮
        │ a     ┆ b     ┆ mean    │
        │ ---   ┆ ---   ┆ ---     │
        │ Int64 ┆ Int64 ┆ Float64 │
        ╞═══════╪═══════╪═════════╡
        │ 1     ┆ 4     ┆ 2.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 3.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 4.5     │
        ╰───────┴───────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_mean requires at least one expression")
    exprs_list = [col(e) if isinstance(e, str) else e for e in exprs]
    return to_list(*exprs_list).list_mean().alias("columns_mean")


def columns_avg(*exprs: Expression | str) -> Expression:
    """Average values across columns. Akin to `columns_mean`.

    Args:
        exprs: The columns to average across.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_avg
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("avg", columns_avg("a", "b"))
        >>> df.show()
        ╭───────┬───────┬─────────╮
        │ a     ┆ b     ┆ avg     │
        │ ---   ┆ ---   ┆ ---     │
        │ Int64 ┆ Int64 ┆ Float64 │
        ╞═══════╪═══════╪═════════╡
        │ 1     ┆ 4     ┆ 2.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 3.5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 4.5     │
        ╰───────┴───────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_avg requires at least one expression")
    exprs_list = [col(e) if isinstance(e, str) else e for e in exprs]
    return to_list(*exprs_list).list_mean().alias("columns_avg")


def columns_min(*exprs: Expression | str) -> Expression:
    """Find the minimum value across columns.

    Args:
        exprs: The columns to find the minimum of.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_min
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("min", columns_min("a", "b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ min   │
        │ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╡
        │ 1     ┆ 4     ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 2     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 3     │
        ╰───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_min requires at least one expression")
    exprs_list = [col(e) if isinstance(e, str) else e for e in exprs]
    return to_list(*exprs_list).list_min().alias("columns_min")


def columns_max(*exprs: Expression | str) -> Expression:
    """Find the maximum value across columns.

    Args:
        exprs: The columns to find the maximum of.

    Examples:
        >>> import daft
        >>> from daft.functions import columns_max
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        >>> df = df.with_column("max", columns_max("a", "b"))
        >>> df.show()
        ╭───────┬───────┬───────╮
        │ a     ┆ b     ┆ max   │
        │ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╡
        │ 1     ┆ 4     ┆ 4     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 5     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 6     ┆ 6     │
        ╰───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if not exprs:
        raise ValueError("columns_max requires at least one expression")
    exprs_list = [col(e) if isinstance(e, str) else e for e in exprs]
    return to_list(*exprs_list).list_max().alias("columns_max")
