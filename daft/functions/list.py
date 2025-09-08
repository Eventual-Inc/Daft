"""List Functions."""

from __future__ import annotations

from typing import Literal

from daft.daft import CountMode
from daft.expressions import Expression


def value_counts(expr: Expression) -> Expression:
    """Counts the occurrences of each distinct value in the list.

    Returns:
        Expression: A Map<X, UInt64> expression where the keys are distinct elements from the
                    original list of type X, and the values are UInt64 counts representing
                    the number of times each element appears in the list.

    Note:
        This function does not work for nested types. For example, it will not produce a map
        with lists as keys.

    Examples:
        >>> import daft
        >>> from daft.functions import value_counts
        >>> df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
        >>> df.with_column("value_counts", value_counts(df["letters"])).collect()
        ╭──────────────┬───────────────────╮
        │ letters      ┆ value_counts      │
        │ ---          ┆ ---               │
        │ List[Utf8]   ┆ Map[Utf8: UInt64] │
        ╞══════════════╪═══════════════════╡
        │ [a, b, a]    ┆ [{key: a,         │
        │              ┆ value: 2,         │
        │              ┆ }, {key: …        │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [b, c, b, c] ┆ [{key: b,         │
        │              ┆ value: 2,         │
        │              ┆ }, {key: …        │
        ╰──────────────┴───────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return Expression._call_builtin_scalar_fn("list_value_counts", expr)


def chunk(expr: Expression, size: int) -> Expression:
    """Splits each list into chunks of the given size.

    Args:
        expr: Expression to chunk
        size: size of chunks to split the list into. Must be greater than 0
    Returns:
        Expression: an expression with lists of fixed size lists of the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_chunk", expr, size)


def list_join(expr: Expression, delimiter: str | Expression) -> Expression:
    """Joins every element of a list using the specified string delimiter.

    Args:
        expr: The list expression to join.
        delimiter (str | Expression): the delimiter to use to join lists with

    Returns:
        Expression: a String expression which is every element of the list joined on the delimiter
    """
    return Expression._call_builtin_scalar_fn("list_join", expr, delimiter)


def list_count(expr: Expression, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid) -> Expression:
    """Counts the number of elements in each list.

    Args:
        expr: The list expression to count elements of.
        mode: A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".

    Returns:
        Expression: a UInt64 expression which is the length of each list
    """
    return Expression._call_builtin_scalar_fn("list_count", expr, mode=mode)


def list_sum(expr: Expression) -> Expression:
    """Sums each list. Empty lists and lists with all nulls yield null.

    Returns:
        Expression: an expression with the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_sum", expr)


def list_mean(expr: Expression) -> Expression:
    """Calculates the mean of each list. If no non-null values in a list, the result is null.

    Returns:
        Expression: a Float64 expression with the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_mean", expr)


def list_min(expr: Expression) -> Expression:
    """Calculates the minimum of each list. If no non-null values in a list, the result is null.

    Returns:
        Expression: a Float64 expression with the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_min", expr)


def list_max(expr: Expression) -> Expression:
    """Calculates the maximum of each list. If no non-null values in a list, the result is null.

    Returns:
        Expression: a Float64 expression with the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_max", expr)


def list_bool_and(expr: Expression) -> Expression:
    """Calculates the boolean AND of all values in a list.

    For each list:
    - Returns True if all non-null values are True
    - Returns False if any non-null value is False
    - Returns null if the list is empty or contains only null values

    Examples:
        >>> import daft
        >>> from daft.functions import list_bool_and
        >>> df = daft.from_pydict({"values": [[True, True], [True, False], [None, None], []]})
        >>> df.with_column("result", list_bool_and(df["values"])).collect()
        ╭───────────────┬─────────╮
        │ values        ┆ result  │
        │ ---           ┆ ---     │
        │ List[Boolean] ┆ Boolean │
        ╞═══════════════╪═════════╡
        │ [true, true]  ┆ true    │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ [true, false] ┆ false   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ [None, None]  ┆ None    │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ []            ┆ None    │
        ╰───────────────┴─────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("list_bool_and", expr)


def list_bool_or(expr: Expression) -> Expression:
    """Calculates the boolean OR of all values in a list.

    For each list:
    - Returns True if any non-null value is True
    - Returns False if all non-null values are False
    - Returns null if the list is empty or contains only null values

    Examples:
        >>> import daft
        >>> from daft.functions import list_bool_or
        >>> df = daft.from_pydict({"values": [[True, False], [False, False], [None, None], []]})
        >>> df.with_column("result", list_bool_or(df["values"])).collect()
        ╭────────────────┬─────────╮
        │ values         ┆ result  │
        │ ---            ┆ ---     │
        │ List[Boolean]  ┆ Boolean │
        ╞════════════════╪═════════╡
        │ [true, false]  ┆ true    │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ [false, false] ┆ false   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ [None, None]   ┆ None    │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ []             ┆ None    │
        ╰────────────────┴─────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("list_bool_or", expr)


def list_sort(
    expr: Expression, desc: bool | Expression | None = None, nulls_first: bool | Expression | None = None
) -> Expression:
    """Sorts the inner lists of a list column.

    Args:
        expr: The list expression to sort.
        desc: Whether to sort in descending order. Defaults to false. Pass in a boolean column to control for each row.
        nulls_first: Whether to put nulls first. Defaults to false (nulls last). Pass in a boolean column to control for each row.

    Returns:
        Expression: An expression with the sorted lists

    Examples:
        >>> import daft
        >>> from daft.functions import list_sort
        >>> df = daft.from_pydict({"a": [[1, 3], [4, 2], [6, 7, 1]]})
        >>> df.select(list_sort(df["a"])).show()
        ╭─────────────╮
        │ a           │
        │ ---         │
        │ List[Int64] │
        ╞═════════════╡
        │ [1, 3]      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [2, 4]      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [1, 6, 7]   │
        ╰─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("list_sort", expr, desc=desc, nulls_first=nulls_first)


def list_distinct(expr: Expression) -> Expression:
    """Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.

    Returns:
        Expression: An expression with lists containing only unique elements

    Examples:
        >>> import daft
        >>> from daft.functions import list_distinct
        >>> df = daft.from_pydict({"a": [[1, 2, 2, 3], [4, 4, 6, 2], [6, 7, 1], [None, 1, None, 1]]})
        >>> df.select(list_distinct(df["a"])).show()
        ╭─────────────╮
        │ a           │
        │ ---         │
        │ List[Int64] │
        ╞═════════════╡
        │ [1, 2, 3]   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [4, 6, 2]   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [6, 7, 1]   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [1]         │
        ╰─────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)

        Note that null values are ignored:

        >>> df = daft.from_pydict({"a": [[None, None], [1, None, 1], [None]]})
        >>> df.select(list_distinct(df["a"])).show()
        ╭─────────────╮
        │ a           │
        │ ---         │
        │ List[Int64] │
        ╞═════════════╡
        │ []          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [1]         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ []          │
        ╰─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("list_distinct", expr)


def list_map(expr: Expression, mapper: Expression) -> Expression:
    """Evaluates an expression on all elements in the list.

    Args:
        expr: The list expression to map over.
        mapper: Expression to run. You can select the element with `daft.element()`

    Examples:
        >>> import daft
        >>> from daft.functions import list_map, upper
        >>> df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
        >>> df.with_column("letters_capitalized", list_map(df["letters"], upper(daft.element()))).collect()
        ╭──────────────┬─────────────────────╮
        │ letters      ┆ letters_capitalized │
        │ ---          ┆ ---                 │
        │ List[Utf8]   ┆ List[Utf8]          │
        ╞══════════════╪═════════════════════╡
        │ [a, b, a]    ┆ [A, B, A]           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [b, c, b, c] ┆ [B, C, B, C]        │
        ╰──────────────┴─────────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return Expression._call_builtin_scalar_fn("list_map", expr, mapper)


def explode(expr: Expression) -> Expression:
    """Explode a list expression.

    A row is created for each item in the lists, and the other non-exploded output columns are broadcasted to match.

    If exploding multiple columns at once, all list lengths must match.

    Tip: See also
        [`DataFrame.explode`](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.explain)

    Examples:
        Explode one column, broadcast the rest:

        >>> import daft
        >>> from daft.functions import explode
        >>>
        >>> df = daft.from_pydict({"id": [1, 2, 3], "sentence": ["lorem ipsum", "foo bar baz", "hi"]})
        >>>
        >>> df.with_column("word", explode(df["sentence"].split(" "))).show()
        ╭───────┬─────────────┬───────╮
        │ id    ┆ sentence    ┆ word  │
        │ ---   ┆ ---         ┆ ---   │
        │ Int64 ┆ Utf8        ┆ Utf8  │
        ╞═══════╪═════════════╪═══════╡
        │ 1     ┆ lorem ipsum ┆ lorem │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 1     ┆ lorem ipsum ┆ ipsum │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ foo bar baz ┆ foo   │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ foo bar baz ┆ bar   │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ foo bar baz ┆ baz   │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ hi          ┆ hi    │
        ╰───────┴─────────────┴───────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)

        Explode multiple columns with the same lengths:

        >>> df.select(
        ...     explode(df["sentence"].split(" ")).alias("word"),
        ...     explode(df["sentence"].capitalize().split(" ")).alias("capitalized_word"),
        ... ).show()
        ╭───────┬──────────────────╮
        │ word  ┆ capitalized_word │
        │ ---   ┆ ---              │
        │ Utf8  ┆ Utf8             │
        ╞═══════╪══════════════════╡
        │ lorem ┆ Lorem            │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ ipsum ┆ ipsum            │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ foo   ┆ Foo              │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ bar   ┆ bar              │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ baz   ┆ baz              │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hi    ┆ Hi               │
        ╰───────┴──────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
        >>>

        This will error because exploded lengths are different:

        >>> # df.select(
        >>> #     df["sentence"]
        >>> #             .str.split(" ")
        >>> #             .explode()
        >>> #             .alias("word"),
        >>> #     df["sentence"]
        >>> #             .str.split("a")
        >>> #             .explode()
        >>> #             .alias("split_on_a")
        >>> # ).show()
    """
    return Expression._call_builtin_scalar_fn("explode", expr)
