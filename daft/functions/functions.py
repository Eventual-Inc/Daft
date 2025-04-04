from __future__ import annotations

import daft.daft as native
from daft.expressions import Expression, list_


def monotonically_increasing_id() -> Expression:
    """Generates a column of monotonically increasing unique ids.

    The implementation puts the partition number in the upper 28 bits, and the row number in each partition
    in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^40 ≈ 68 billion rows per partition.

    Returns:
        Expression: An expression that generates monotonically increasing IDs

    Example:
        ``` py linenums="1"
        import daft
        from daft.functions import monotonically_increasing_id

        daft.context.set_runner_ray()  # doctest: +SKIP
        df = daft.from_pydict({"a": [1, 2, 3, 4]}).into_partitions(2)
        df = df.with_column("id", monotonically_increasing_id())
        df.show()  # doctest: +SKIP
        ```
        ```
        ╭───────┬─────────────╮
        │ a     ┆ id          │
        │ ---   ┆ ---         │
        │ Int64 ┆ UInt64      │
        ╞═══════╪═════════════╡
        │ 1     ┆ 0           │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2     ┆ 1           │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ 68719476736 │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 4     ┆ 68719476737 │
        ╰───────┴─────────────╯

        (Showing first 4 of 4 rows)
        ```
    """
    return Expression._from_pyexpr(native.monotonically_increasing_id())


def columns_sum(*exprs: Expression | str) -> Expression:
    """Sum values across columns.

    Args:
        exprs: The columns to sum.

    Example:
        ``` py linenums="1"
        import daft
        from daft.functions import columns_sum

        df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        df = df.with_column("sum", columns_sum("a", "b"))
        df.show()
        ```
        ```
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

        (Showing first 3 of 3 rows)
        ```
    """
    if not exprs:
        raise ValueError("columns_sum requires at least one expression")
    return list_(*exprs).list.sum().alias("columns_sum")


def columns_mean(*exprs: Expression | str) -> Expression:
    """Average values across columns. Akin to `columns_avg`.

    Args:
        exprs: The columns to average.

    Example:
        ``` py linenums="1"
        import daft
        from daft.functions import columns_mean

        df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        df = df.with_column("mean", columns_mean("a", "b"))
        df.show()
        ```
        ```
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

        (Showing first 3 of 3 rows)
        ```
    """
    if not exprs:
        raise ValueError("columns_mean requires at least one expression")
    return list_(*exprs).list.mean().alias("columns_mean")


def columns_avg(*exprs: Expression | str) -> Expression:
    """Average values across columns. Akin to `columns_mean`.

    Args:
        exprs: The columns to average across.

    Example:
        ``` py linenums="1"
        import daft
        from daft.functions import columns_avg

        df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        df = df.with_column("avg", columns_avg("a", "b"))
        df.show()
        ```
        ```
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

        (Showing first 3 of 3 rows)
        ```
    """
    if not exprs:
        raise ValueError("columns_avg requires at least one expression")
    return list_(*exprs).list.mean().alias("columns_avg")


def columns_min(*exprs: Expression | str) -> Expression:
    """Find the minimum value across columns.

    Args:
        exprs: The columns to find the minimum of.

    Example:
        ``` py linenums="1"
        import daft
        from daft.functions import columns_min

        df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        df = df.with_column("min", columns_min("a", "b"))
        df.show()
        ```
        ```
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

        (Showing first 3 of 3 rows)
        ```
    """
    if not exprs:
        raise ValueError("columns_min requires at least one expression")
    return list_(*exprs).list.min().alias("columns_min")


def columns_max(*exprs: Expression | str) -> Expression:
    """Find the maximum value across columns.

    Args:
        exprs: The columns to find the maximum of.

    Example:
        ``` py linenums="1"
        import daft
        from daft.functions import columns_max

        df = daft.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
        df = df.with_column("max", columns_max("a", "b"))
        df.show()
        ```
        ```
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

        (Showing first 3 of 3 rows)
        ```
    """
    if not exprs:
        raise ValueError("columns_max requires at least one expression")
    return list_(*exprs).list.max().alias("columns_max")
