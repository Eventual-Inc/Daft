"""List Functions."""

from __future__ import annotations

from typing import Literal

from daft.daft import CountMode, list_
from daft.expressions import Expression


def value_counts(list_expr: Expression) -> Expression:
    """Counts the occurrences of each distinct value in the list.

    Args:
        list_expr (List Expression): expression to count the occurrences of each distinct value in.

    Returns:
        Expression (Map Expression):
            A Map<X, UInt64> expression where the keys are distinct elements from the
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
        ╭──────────────┬─────────────────────╮
        │ letters      ┆ value_counts        │
        │ ---          ┆ ---                 │
        │ List[String] ┆ Map[String: UInt64] │
        ╞══════════════╪═════════════════════╡
        │ [a, b, a]    ┆ [{key: a,           │
        │              ┆ value: 2,           │
        │              ┆ }, {key: …          │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [b, c, b, c] ┆ [{key: b,           │
        │              ┆ value: 2,           │
        │              ┆ }, {key: …          │
        ╰──────────────┴─────────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return Expression._call_builtin_scalar_fn("list_value_counts", list_expr)


def chunk(list_expr: Expression, size: int) -> Expression:
    """Splits each list into chunks of the given size.

    Args:
        list_expr (List Expression): expression to chunk
        size (int): size of chunks to split the list into. Must be greater than 0

    Returns:
        Expression (List[FixedSizedList] Expression):
            expression with lists of fixed size lists of the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_chunk", list_expr, size)


def list_join(list_expr: Expression, delimiter: str | Expression) -> Expression:
    """Joins every element of a list using the specified string delimiter.

    Args:
        list_expr (List Expression): expression to join
        delimiter (str | String Expression): the delimiter to use to join lists with

    Returns:
        Expression (String Expression): an expression which is every element of the list joined on the delimiter
    """
    return Expression._call_builtin_scalar_fn("list_join", list_expr, delimiter)


def list_count(
    list_expr: Expression, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid
) -> Expression:
    """Counts the number of elements in each list.

    Args:
        list_expr (List Expression): The list expression to count elements of.
        mode (str | CountMode, default=CountMode.Valid):
            A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".

    Returns:
        Expression (UInt64 Expression): an expression which is the length of each list
    """
    return Expression._call_builtin_scalar_fn("list_count", list_expr, mode=mode)


def list_sum(list_expr: Expression) -> Expression:
    """Sums each list. Empty lists and lists with all nulls yield null.

    Args:
        list_expr (List Expression): expression to sum elements of.

    Returns:
        Expression: an expression with the type of the list values
    """
    return Expression._call_builtin_scalar_fn("list_sum", list_expr)


def list_mean(list_expr: Expression) -> Expression:
    """Calculates the mean of each list. If no non-null values in a list, the result is null.

    Args:
        list_expr (List Expression): expression to calculate the mean of.

    Returns:
        Expression (Float64 Expression): an expression with the calculated mean of the list values
    """
    return Expression._call_builtin_scalar_fn("list_mean", list_expr)


def list_min(list_expr: Expression) -> Expression:
    """Calculates the minimum of each list. If no non-null values in a list, the result is null.

    Args:
        list_expr (List Expression): expression to calculate the minimum of.

    Returns:
        Expression:
            an expression with the type of the list values representing the minimum value in the list
    """
    return Expression._call_builtin_scalar_fn("list_min", list_expr)


def list_max(list_expr: Expression) -> Expression:
    """Calculates the maximum of each list. If no non-null values in a list, the result is null.

    Args:
        list_expr (List Expression): expression to calculate the maximum of.

    Returns:
        Expression:
            an expression with the type of the list values representing the maximum value in the list
    """
    return Expression._call_builtin_scalar_fn("list_max", list_expr)


def list_bool_and(list_expr: Expression) -> Expression:
    """Calculates the boolean AND of all values in a list.

    For each list:
    - Returns True if all non-null values are True
    - Returns False if any non-null value is False
    - Returns null if the list is empty or contains only null values

    Args:
        list_expr (List Expression): expression to calculate the boolean AND of.

    Returns:
        Expression (Boolean Expression): an expression with the result of the boolean AND operation.

    Examples:
        >>> import daft
        >>> from daft.functions import list_bool_and
        >>> df = daft.from_pydict({"values": [[True, True], [True, False], [None, None], []]})
        >>> df.with_column("result", list_bool_and(df["values"])).collect()
        ╭───────────────┬────────╮
        │ values        ┆ result │
        │ ---           ┆ ---    │
        │ List[Bool]    ┆ Bool   │
        ╞═══════════════╪════════╡
        │ [true, true]  ┆ true   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ [true, false] ┆ false  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ [None, None]  ┆ None   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ []            ┆ None   │
        ╰───────────────┴────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("list_bool_and", list_expr)


def list_bool_or(list_expr: Expression) -> Expression:
    """Calculates the boolean OR of all values in a list.

    For each list:
    - Returns True if any non-null value is True
    - Returns False if all non-null values are False
    - Returns null if the list is empty or contains only null values

    Args:
        list_expr (List Expression): expression to calculate the boolean OR of.

    Returns:
         Expression (Boolean Expression): an expression with the result of the boolean OR operation.

    Examples:
        >>> import daft
        >>> from daft.functions import list_bool_or
        >>> df = daft.from_pydict({"values": [[True, False], [False, False], [None, None], []]})
        >>> df.with_column("result", list_bool_or(df["values"])).collect()
        ╭────────────────┬────────╮
        │ values         ┆ result │
        │ ---            ┆ ---    │
        │ List[Bool]     ┆ Bool   │
        ╞════════════════╪════════╡
        │ [true, false]  ┆ true   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ [false, false] ┆ false  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ [None, None]   ┆ None   │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ []             ┆ None   │
        ╰────────────────┴────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)
    """
    return Expression._call_builtin_scalar_fn("list_bool_or", list_expr)


def list_sort(
    list_expr: Expression, desc: bool | Expression | None = None, nulls_first: bool | Expression | None = None
) -> Expression:
    """Sorts the inner lists of a list column.

    Args:
        list_expr (List Expression): expression to sort
        desc: (bool | Boolean Expression) Whether to sort in descending order. Defaults to false. Pass in a boolean column to control for each row.
        nulls_first: (bool | Boolean Expression) Whether to put nulls first. Defaults to false (nulls last). Pass in a boolean column to control for each row.

    Returns:
        Expression (List Expression): an expression with the sorted lists

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
    return Expression._call_builtin_scalar_fn("list_sort", list_expr, desc=desc, nulls_first=nulls_first)


def list_distinct(list_expr: Expression) -> Expression:
    """Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.

    Args:
        list_expr (List Expression): The input list expression

    Returns:
        Expression (List Expression): an expression with lists containing only unique elements

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
    return Expression._call_builtin_scalar_fn("list_distinct", list_expr)


def list_map(list_expr: Expression, mapper: Expression) -> Expression:
    """Evaluates an expression on all elements in the list.

    Args:
        list_expr (List Expression): expression to map over.
        mapper: Expression to run. You can select the element with `daft.element()`

    Returns:
        Expression (List Expression): an expression representing the mapped list.

    Examples:
        >>> import daft
        >>> from daft.functions import list_map, upper
        >>> df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
        >>> df.with_column("letters_capitalized", list_map(df["letters"], upper(daft.element()))).collect()
        ╭──────────────┬─────────────────────╮
        │ letters      ┆ letters_capitalized │
        │ ---          ┆ ---                 │
        │ List[String] ┆ List[String]        │
        ╞══════════════╪═════════════════════╡
        │ [a, b, a]    ┆ [A, B, A]           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [b, c, b, c] ┆ [B, C, B, C]        │
        ╰──────────────┴─────────────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return Expression._call_builtin_scalar_fn("list_map", list_expr, mapper)


def explode(list_expr: Expression) -> Expression:
    """Explode a list expression.

    A row is created for each item in the lists, and the other non-exploded output columns are broadcasted to match.

    If exploding multiple columns at once, all list lengths must match.

    Note:
        Since this changes the cardinality of the dataframe, We only allow a single explode per projection (`select`, `with_columns`)
        If you need to do multiple explodes, each one must be done separately.

    Args:
        list_expr (List Expression): expression to explode.

    Returns:
        Expression: Expression representing the exploded list.

    Tip: See also
        [`DataFrame.explode`](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.explode)

    Examples:
        Explode one column, broadcast the rest:

        >>> import daft
        >>> from daft.functions import explode
        >>>
        >>> df = daft.from_pydict({"id": [1, 2, 3], "sentence": ["lorem ipsum", "foo bar baz", "hi"]})
        >>>
        >>> df.with_column("word", explode(df["sentence"].split(" "))).show()
        ╭───────┬─────────────┬────────╮
        │ id    ┆ sentence    ┆ word   │
        │ ---   ┆ ---         ┆ ---    │
        │ Int64 ┆ String      ┆ String │
        ╞═══════╪═════════════╪════════╡
        │ 1     ┆ lorem ipsum ┆ lorem  │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 1     ┆ lorem ipsum ┆ ipsum  │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2     ┆ foo bar baz ┆ foo    │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2     ┆ foo bar baz ┆ bar    │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 2     ┆ foo bar baz ┆ baz    │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
        │ 3     ┆ hi          ┆ hi     │
        ╰───────┴─────────────┴────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)

        Explode multiple columns with the same lengths:

        >>> df.select(
        ...     explode(df["sentence"].split(" ")).alias("word"),
        ...     explode(df["sentence"].capitalize().split(" ")).alias("capitalized_word"),
        ... ).show()
        ╭────────┬──────────────────╮
        │ word   ┆ capitalized_word │
        │ ---    ┆ ---              │
        │ String ┆ String           │
        ╞════════╪══════════════════╡
        │ lorem  ┆ Lorem            │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ ipsum  ┆ ipsum            │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ foo    ┆ Foo              │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ bar    ┆ bar              │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ baz    ┆ baz              │
        ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hi     ┆ Hi               │
        ╰────────┴──────────────────╯
        <BLANKLINE>
        (Showing first 6 of 6 rows)
        >>>

        This will error because exploded lengths are different:

        >>> # df.select(
        >>> #     df["sentence"]
        >>> #             .split(" ")
        >>> #             .explode()
        >>> #             .alias("word"),
        >>> #     df["sentence"]
        >>> #             .split("a")
        >>> #             .explode()
        >>> #             .alias("split_on_a")
        >>> # ).show()
    """
    return Expression._call_builtin_scalar_fn("explode", list_expr)


def list_append(list_expr: Expression, other: Expression) -> Expression:
    """Appends a value to each list in the column.

    Args:
        list_expr (List Expression): expression to append to
        other (Expression): A value or column of values to append to each list

    Returns:
        Expression (List Expression): an expression with the updated lists

    Examples:
        >>> import daft
        >>> from daft.functions import list_append
        >>>
        >>> df = daft.from_pydict({"a": [[1, 2], [3, 4, 5]], "b": [10, 11]})
        >>> df.with_column("combined", list_append(df["a"], df["b"])).show()
        ╭─────────────┬───────┬───────────────╮
        │ a           ┆ b     ┆ combined      │
        │ ---         ┆ ---   ┆ ---           │
        │ List[Int64] ┆ Int64 ┆ List[Int64]   │
        ╞═════════════╪═══════╪═══════════════╡
        │ [1, 2]      ┆ 10    ┆ [1, 2, 10]    │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [3, 4, 5]   ┆ 11    ┆ [3, 4, 5, 11] │
        ╰─────────────┴───────┴───────────────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    return Expression._call_builtin_scalar_fn("list_append", list_expr, other)


def to_list(*items: Expression) -> Expression:
    """Constructs a list from the item expressions.

    Args:
        *items: item expressions to construct the list

    Returns:
        Expression (List Expression): expression representing the constructed list

    Examples:
        >>> import daft
        >>> from daft.functions import to_list
        >>>
        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> df = df.select(to_list(df["x"], df["y"]).alias("fwd"), to_list(df["y"], df["x"]).alias("rev"))
        >>> df.show()
        ╭─────────────┬─────────────╮
        │ fwd         ┆ rev         │
        │ ---         ┆ ---         │
        │ List[Int64] ┆ List[Int64] │
        ╞═════════════╪═════════════╡
        │ [1, 4]      ┆ [4, 1]      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [2, 5]      ┆ [5, 2]      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [3, 6]      ┆ [6, 3]      │
        ╰─────────────┴─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    assert len(items) > 0, "List constructor requires at least one item"
    return Expression._from_pyexpr(list_([Expression._to_expression(i)._expr for i in items]))
