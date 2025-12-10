"""Aggregate Functions."""

from __future__ import annotations

from typing import Literal

from daft.daft import CountMode
from daft.expressions.expressions import Expression


def count(expr: Expression, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid) -> Expression:
    """Counts the number of values in the expression.

    Args:
        expr (Expression): The input expression to count values from.
        mode: A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".
    """
    if isinstance(mode, str):
        mode = CountMode.from_count_mode_str(mode)
    return Expression._from_pyexpr(expr._expr.count(mode))


def count_distinct(expr: Expression) -> Expression:
    """Counts the number of distinct values in the expression."""
    return Expression._from_pyexpr(expr._expr.count_distinct())


def sum(expr: Expression) -> Expression:
    """Calculates the sum of the values in the expression."""
    return Expression._from_pyexpr(expr._expr.sum())


def product(expr: Expression) -> Expression:
    """Calculates the product of the values in the expression."""
    return Expression._from_pyexpr(expr._expr.product())


def approx_count_distinct(expr: Expression) -> Expression:
    """Calculates the approximate number of non-`NULL` distinct values in the expression.

    Approximation is performed using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

    Examples:
        A global calculation of approximate distinct values in a non-NULL column:

        >>> import daft
        >>> from daft.functions import approx_count_distinct
        >>>
        >>> df = daft.from_pydict({"values": [1, 2, 3, None]})
        >>> df = df.agg(
        ...     approx_count_distinct(df["values"]).alias("distinct_values"),
        ... )
        >>> df.show()
        ╭─────────────────╮
        │ distinct_values │
        │ ---             │
        │ UInt64          │
        ╞═════════════════╡
        │ 3               │
        ╰─────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    return Expression._from_pyexpr(expr._expr.approx_count_distinct())


def approx_percentiles(expr: Expression, percentiles: float | list[float]) -> Expression:
    """Calculates the approximate percentile(s) for a column of numeric values.

    For numeric columns, we use the [sketches_ddsketch crate](https://docs.rs/sketches-ddsketch/latest/sketches_ddsketch/index.html).
    This is a Rust implementation of the paper [DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees (Masson et al.)](https://arxiv.org/pdf/1908.10693)

    1. Null values are ignored in the computation of the percentiles
    2. If all values are Null then the result will also be Null
    3. If ``percentiles`` are supplied as a single float, then the resultant column is a ``Float64`` column
    4. If ``percentiles`` is supplied as a list, then the resultant column is a ``FixedSizeList[Float64; N]`` column, where ``N`` is the length of the supplied list.

    Args:
        percentiles: the percentile(s) at which to find approximate values at. Can be provided as a single
            float or a list of floats.

    Returns:
        A new expression representing the approximate percentile(s). If `percentiles` was a single float, this will be a new `Float64` expression. If `percentiles` was a list of floats, this will be a new expression with type: `FixedSizeList[Float64, len(percentiles)]`.

    Examples:
        A global calculation of approximate percentiles:

        >>> import daft
        >>> from daft.functions import approx_percentiles
        >>>
        >>> df = daft.from_pydict({"scores": [1, 2, 3, 4, 5, None]})
        >>> df = df.agg(
        ...     approx_percentiles(df["scores"], 0.5).alias("approx_median_score"),
        ...     approx_percentiles(df["scores"], [0.25, 0.5, 0.75]).alias("approx_percentiles_scores"),
        ... )
        >>> df.show()
        ╭─────────────────────┬────────────────────────────────╮
        │ approx_median_score ┆ approx_percentiles_scores      │
        │ ---                 ┆ ---                            │
        │ Float64             ┆ List[Float64; 3]               │
        ╞═════════════════════╪════════════════════════════════╡
        │ 2.9742334234767167  ┆ [1.993661701417351, 2.9742334… │
        ╰─────────────────────┴────────────────────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

        A grouped calculation of approximate percentiles:

        >>> df = daft.from_pydict({"class": ["a", "a", "a", "b", "c"], "scores": [1, 2, 3, 1, None]})
        >>> df = (
        ...     df.groupby("class")
        ...     .agg(
        ...         approx_percentiles(df["scores"], 0.5).alias("approx_median_score"),
        ...         approx_percentiles(df["scores"], [0.25, 0.5, 0.75]).alias("approx_percentiles_scores"),
        ...     )
        ...     .sort("class")
        ... )
        >>> df.show()
        ╭────────┬─────────────────────┬────────────────────────────────╮
        │ class  ┆ approx_median_score ┆ approx_percentiles_scores      │
        │ ---    ┆ ---                 ┆ ---                            │
        │ String ┆ Float64             ┆ List[Float64; 3]               │
        ╞════════╪═════════════════════╪════════════════════════════════╡
        │ a      ┆ 1.993661701417351   ┆ [0.9900000000000001, 1.993661… │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b      ┆ 0.9900000000000001  ┆ [0.9900000000000001, 0.990000… │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ c      ┆ None                ┆ None                           │
        ╰────────┴─────────────────────┴────────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._from_pyexpr(expr._expr.approx_percentiles(percentiles))


def mean(expr: Expression) -> Expression:
    """Calculates the mean of the values in the expression."""
    return Expression._from_pyexpr(expr._expr.mean())


def avg(expr: Expression) -> Expression:
    """Calculates the mean of the values in the expression. Alias for mean()."""
    return Expression._from_pyexpr(expr._expr.mean())


def stddev(expr: Expression) -> Expression:
    """Calculates the standard deviation of the values in the expression."""
    return Expression._from_pyexpr(expr._expr.stddev())


def min(expr: Expression) -> Expression:
    """Calculates the minimum of the values in the expression."""
    return Expression._from_pyexpr(expr._expr.min())


def max(expr: Expression) -> Expression:
    """Calculates the maximum of the values in the expression."""
    return Expression._from_pyexpr(expr._expr.max())


def bool_and(expr: Expression) -> Expression:
    """Calculates the boolean AND of all values in the expression.

    For each group:
    - Returns True if all non-null values are True
    - Returns False if any non-null value is False
    - Returns null if the group is empty or contains only null values

    Examples:
        >>> import daft
        >>> from daft.functions import bool_and
        >>> df = daft.from_pydict({"a": [True, None, True], "b": [True, False, None], "c": [None, None, None]})
        >>> df.agg(bool_and(df["a"]), bool_and(df["b"]), bool_and(df["c"])).show()
        ╭──────┬───────┬──────╮
        │ a    ┆ b     ┆ c    │
        │ ---  ┆ ---   ┆ ---  │
        │ Bool ┆ Bool  ┆ Bool │
        ╞══════╪═══════╪══════╡
        │ true ┆ false ┆ None │
        ╰──────┴───────┴──────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    return Expression._from_pyexpr(expr._expr.bool_and())


def bool_or(expr: Expression) -> Expression:
    """Calculates the boolean OR of all values in the expression.

    For each group:
    - Returns True if any non-null value is True
    - Returns False if all non-null values are False
    - Returns null if the group is empty or contains only null values

    Examples:
        >>> import daft
        >>> from daft.functions import bool_or
        >>> df = daft.from_pydict({"a": [False, None, False], "b": [False, True, None], "c": [None, None, None]})
        >>> df.agg(bool_or(df["a"]), bool_or(df["b"]), bool_or(df["c"])).show()
        ╭───────┬──────┬──────╮
        │ a     ┆ b    ┆ c    │
        │ ---   ┆ ---  ┆ ---  │
        │ Bool  ┆ Bool ┆ Bool │
        ╞═══════╪══════╪══════╡
        │ false ┆ true ┆ None │
        ╰───────┴──────┴──────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)
    """
    return Expression._from_pyexpr(expr._expr.bool_or())


def any_value(expr: Expression, ignore_nulls: bool = False) -> Expression:
    """Returns any non-null value from the expression.

    Args:
        expr (Expression): The input expression to select a value from.
        ignore_nulls: whether to ignore null values when selecting the value. Defaults to False.
    """
    return Expression._from_pyexpr(expr._expr.any_value(ignore_nulls))


def skew(expr: Expression) -> Expression:
    """Calculates the skewness of the values from the expression."""
    return Expression._from_pyexpr(expr._expr.skew())


def list_agg(expr: Expression) -> Expression:
    """Aggregates the values in the expression into a list."""
    return Expression._from_pyexpr(expr._expr.agg_list())


def list_agg_distinct(expr: Expression) -> Expression:
    """Aggregates the values in the expression into a list of distinct values (ignoring nulls).

    Returns:
        Expression: A List expression containing the distinct values from the input

    Examples:
        >>> import daft
        >>> from daft.functions import list_agg_distinct
        >>>
        >>> df = daft.from_pydict({"values": [1, 1, None, 2, 2, None]})
        >>> df.agg(list_agg_distinct(df["values"]).alias("distinct_values")).show()
        ╭─────────────────╮
        │ distinct_values │
        │ ---             │
        │ List[Int64]     │
        ╞═════════════════╡
        │ [1, 2]          │
        ╰─────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

        Note that null values are ignored by default:

        >>> df = daft.from_pydict({"values": [None, None, None]})
        >>> df.agg(list_agg_distinct(df["values"]).alias("distinct_values")).show()
        ╭─────────────────╮
        │ distinct_values │
        │ ---             │
        │ List[Null]      │
        ╞═════════════════╡
        │ []              │
        ╰─────────────────╯
        <BLANKLINE>
        (Showing first 1 of 1 rows)

    """
    return Expression._from_pyexpr(expr._expr.agg_set())


def string_agg(expr: Expression) -> Expression:
    """Aggregates the values in the expression into a single string by concatenating them."""
    return Expression._from_pyexpr(expr._expr.agg_concat())
