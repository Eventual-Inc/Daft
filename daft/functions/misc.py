"""Miscellaneous Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

import daft.daft as native
from daft.datatype import DataType, DataTypeLike
from daft.expressions import Expression
from daft.series import item_to_series

if TYPE_CHECKING:
    from daft.io import IOConfig


def monotonically_increasing_id() -> Expression:
    """Generates a column of monotonically increasing unique ids.

    The implementation puts the partition number in the upper 28 bits, and the row number in each partition
    in the lower 36 bits. This allows for 2^28 ≈ 268 million partitions and 2^36 ≈ 68 billion rows per partition.

    Returns:
        Expression: An expression that generates monotonically increasing IDs

    Examples:
        >>> import daft
        >>> from daft.functions import monotonically_increasing_id
        >>> daft.context.set_runner_ray()  # doctest: +SKIP
        >>>
        >>> df = daft.from_pydict({"a": [1, 2, 3, 4]}).into_partitions(2)
        >>> df = df.with_column("id", monotonically_increasing_id())
        >>> df.show()  # doctest: +SKIP
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
        <BLANKLINE>
        (Showing first 4 of 4 rows)

    """
    f = native.get_function_from_registry("monotonically_increasing_id")
    return Expression._from_pyexpr(f())


def file(expr: Expression, io_config: IOConfig | None = None) -> Expression:
    """Converts either a string containing a file reference, or a binary column to a `daft.File` reference.

    If the input is a string, it is assumed to be a file path and is converted to a `daft.File`.
    If the input is a binary column, it is converted to a `daft.File` where the entire contents are buffered in memory.
    """
    return expr._eval_expressions("file", io_config=io_config)


def eq_null_safe(left: Expression, right: Expression) -> Expression:
    """Performs a null-safe equality comparison between two expressions.

    Unlike regular equality (==), null-safe equality (<=> or IS NOT DISTINCT FROM):
    - Returns True when comparing NULL <=> NULL
    - Returns False when comparing NULL <=> any_value
    - Behaves like regular equality for non-NULL values

    Returns:
        Expression: A boolean expression indicating if the values are equal
    """
    left = Expression._to_expression(left)
    right = Expression._to_expression(right)
    return Expression._from_pyexpr(left._expr.eq_null_safe(right._expr))


def cast(expr: Expression, dtype: DataTypeLike) -> Expression:
    """Casts an expression to the given datatype if possible.

    The following combinations of datatype casting is valid:

    | Target →           | Null | Boolean | Integers | Floats | Decimal128 | String | Binary | Fixed-size Binary | Image | Fixed-shape Image | Embedding | Tensor | Fixed-shape Tensor | Python | List | Fixed-size List | Struct | Map | Timestamp | Date | Time | Duration |
    | ------------------ | ---- | ------- | -------- | ------ | ---------- | ------ | ------ | ----------------- | ----- | ----------------- | --------- | ------ | ------------------ | ------ | ---- | --------------- | ------ | --- | --------- | ---- | ---- | -------- |
    | **Source ↓**       |
    | Null               | Y    | Y       | Y        | Y      | Y          | Y      | Y      | Y                 | N     | N                 | Y         | N      | N                  | Y      | Y    | Y               | Y      | Y   | Y         | Y    | Y    | Y        |
    | Boolean            | Y    | Y       | Y        | Y      | N          | Y      | Y      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | N         | N    | N    | N        |
    | Integers           | Y    | Y       | Y        | Y      | Y          | Y      | Y      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | Y         | Y    | Y    | Y        |
    | Floats             | Y    | Y       | Y        | Y      | Y          | Y      | Y      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | M               | N      | N   | Y         | Y    | Y    | Y        |
    | Decimal128         | Y    | N       | Y        | Y      | Y          | N      | N      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | N         | N    | N    | N        |
    | String             | Y    | N       | Y        | Y      | N          | Y      | Y      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | Y         | Y    | N    | N        |
    | Binary             | Y    | N       | Y        | Y      | N          | Y      | Y      | Y                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | N         | N    | N    | N        |
    | Fixed-size Binary  | Y    | N       | N        | N      | N          | N      | Y      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | N         | N    | N    | N        |
    | Image              | N    | N       | N        | N      | N          | N      | N      | N                 | Y     | Y                 | N         | Y      | Y                  | Y      | N    | N               | Y      | N   | N         | N    | N    | N        |
    | Fixed-size Image   | N    | N       | N        | N      | N          | N      | N      | N                 | Y     | Y                 | N         | Y      | Y                  | Y      | Y    | Y               | N      | N   | N         | N    | N    | N        |
    | Embedding          | Y    | N       | N        | N      | N          | N      | N      | N                 | N     | Y                 | N         | Y      | Y                  | Y      | Y    | Y               | N      | N   | N         | N    | N    | N        |
    | Tensor             | Y    | N       | N        | N      | N          | N      | N      | N                 | Y     | Y                 | N         | Y      | Y                  | Y      | N    | N               | Y      | N   | N         | N    | N    | N        |
    | Fixed-shape Tensor | N    | N       | N        | N      | N          | N      | N      | N                 | N     | Y                 | N         | Y      | Y                  | Y      | Y    | Y               | N      | N   | N         | N    | N    | N        |
    | Python             | Y    | Y       | Y        | Y      | N          | Y      | Y      | Y                 | Y     | Y                 | Y         | Y      | Y                  | Y      | Y    | Y               | Y      | N   | N         | N    | N    | N        |
    | List               | N    | N       | N        | N      | N          | N      | N      | N                 | N     | N                 | Y         | N      | N                  | N      | Y    | Y               | N      | Y   | N         | N    | N    | N        |
    | Fixed-size List    | N    | N       | N        | N      | N          | N      | N      | N                 | N     | Y                 | N         | N      | Y                  | N      | Y    | Y               | N      | N   | N         | N    | N    | N        |
    | Struct             | N    | N       | N        | N      | N          | N      | N      | N                 | Y     | N                 | N         | Y      | N                  | N      | N    | N               | Y      | N   | N         | N    | N    | N        |
    | Map                | N    | N       | N        | N      | N          | N      | N      | N                 | N     | N                 | Y         | N      | N                  | N      | Y    | Y               | N      | Y   | N         | N    | N    | N        |
    | Timestamp          | Y    | N       | Y        | Y      | N          | Y      | N      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | Y         | Y    | Y    | N        |
    | Date               | Y    | N       | Y        | Y      | N          | Y      | N      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | Y         | Y    | N    | N        |
    | Time               | Y    | N       | Y        | Y      | N          | Y      | N      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | N         | N    | Y    | N        |
    | Duration           | Y    | N       | Y        | Y      | N          | N      | N      | N                 | N     | N                 | N         | N      | N                  | Y      | N    | N               | N      | N   | N         | N    | N    | N        |

    Returns:
        Expression: Expression with the specified new datatype

    Note:
        - Overflowing values will be wrapped, e.g. 256 will be cast to 0 for an unsigned 8-bit integer.
        - If a string is provided, it will use the sql engine to parse the string into a data type. See the [SQL Reference](https://docs.daft.ai/en/stable/sql/datatypes/) for supported datatypes.
        - a python `type` can also be provided, in which case the corresponding Daft data type will be used.

    Tip: See Also
        [`Expression.cast`](https://docs.daft.ai/en/stable/api/expressions/#daft.expressions.Expression.cast)

    Examples:
        >>> import daft
        >>> df = daft.from_pydict({"float": [1.0, 2.5, None]})
        >>> df = df.select(df["float"].cast(daft.DataType.int64()))
        >>> df.show()
        ╭───────╮
        │ float │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 1     │
        ├╌╌╌╌╌╌╌┤
        │ 2     │
        ├╌╌╌╌╌╌╌┤
        │ None  │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Example with python type and sql types:
        >>> df = daft.from_pydict({"a": [1, 2, 3]})
        >>> df = df.select(
        ...     df["a"].cast(str).alias("str"),
        ...     df["a"].cast(int).alias("int"),
        ...     df["a"].cast(float).alias("float"),
        ...     df["a"].cast("string").alias("sql_string"),
        ...     df["a"].cast("int").alias("sql_int"),
        ...     df["a"].cast("tinyint").alias("sql_tinyint"),
        ... )
        >>> df.show()
        ╭──────┬───────┬─────────┬────────────┬─────────┬─────────────╮
        │ str  ┆ int   ┆ float   ┆ sql_string ┆ sql_int ┆ sql_tinyint │
        │ ---  ┆ ---   ┆ ---     ┆ ---        ┆ ---     ┆ ---         │
        │ Utf8 ┆ Int64 ┆ Float64 ┆ Utf8       ┆ Int32   ┆ Int8        │
        ╞══════╪═══════╪═════════╪════════════╪═════════╪═════════════╡
        │ 1    ┆ 1     ┆ 1       ┆ 1          ┆ 1       ┆ 1           │
        ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 2    ┆ 2     ┆ 2       ┆ 2          ┆ 2       ┆ 2           │
        ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 3    ┆ 3     ┆ 3       ┆ 3          ┆ 3       ┆ 3           │
        ╰──────┴───────┴─────────┴────────────┴─────────┴─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if isinstance(dtype, str):
        dtype = DataType._from_pydatatype(native.sql_datatype(dtype))
    else:
        dtype = DataType._infer_type(dtype)
    expr = Expression._to_expression(expr)
    return Expression._from_pyexpr(expr._expr.cast(dtype._dtype))


def is_null(expr: Expression) -> Expression:
    """Checks if values in the Expression are Null (a special value indicating missing data).

    Returns:
        Expression: Boolean Expression indicating whether values are missing

    Examples:
        >>> import daft
        >>> from daft.functions import is_null
        >>>
        >>> df = daft.from_pydict({"x": [1.0, None, float("nan")]})
        >>> df = df.select(is_null(df["x"]))
        >>> df.collect()
        ╭─────────╮
        │ x       │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    expr = Expression._to_expression(expr)
    return Expression._from_pyexpr(expr._expr.is_null())


def not_null(expr: Expression) -> Expression:
    """Checks if values in the Expression are not Null (a special value indicating missing data).

    Returns:
        Expression: Boolean Expression indicating whether values are not missing

    Examples:
        >>> import daft
        >>> from daft.functions import not_null
        >>>
        >>> df = daft.from_pydict({"x": [1.0, None, float("nan")]})
        >>> df = df.select(not_null(df["x"]))
        >>> df.collect()
        ╭─────────╮
        │ x       │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ true    │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    expr = Expression._to_expression(expr)
    return Expression._from_pyexpr(expr._expr.not_null())


def fill_null(expr: Expression, fill_value: Expression) -> Expression:
    """Fills null values in the Expression with the provided fill_value.

    Returns:
        Expression: Expression with null values filled with the provided fill_value

    Examples:
        >>> import daft
        >>> from daft.functions import fill_null
        >>>
        >>> df = daft.from_pydict({"data": [1, None, 3]})
        >>> df = df.select(fill_null(df["data"], 2))
        >>> df.collect()
        ╭───────╮
        │ data  │
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
    expr = Expression._to_expression(expr)
    fill_value = Expression._to_expression(fill_value)
    return Expression._from_pyexpr(expr._expr.fill_null(fill_value._expr))


def is_in(expr: Expression, other: Any) -> Expression:
    """Checks if values in the Expression are in the provided list.

    Returns:
        Expression: Boolean Expression indicating whether values are in the provided list

    Examples:
        >>> import daft
        >>> from daft.functions import is_in
        >>>
        >>> df = daft.from_pydict({"data": [1, 2, 3]})
        >>> df = df.select(is_in(df["data"], [1, 3]))
        >>> df.collect()
        ╭─────────╮
        │ data    │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ true    │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    if isinstance(other, list):
        other = [Expression._to_expression(item) for item in other]
    elif not isinstance(other, Expression):
        series = item_to_series("items", other)
        other = [Expression._from_pyexpr(native.list_lit(series._series))]
    else:
        other = [other]

    expr = Expression._to_expression(expr)
    return Expression._from_pyexpr(expr._expr.is_in([item._expr for item in other]))


def hash(
    expr: Expression,
    seed: Any | None = None,
    hash_function: Literal["xxhash", "murmurhash3", "sha1"] | None = "xxhash",
) -> Expression:
    """Hashes the values in the Expression.

    Uses the specified hash function to hash the values in the expression. Default to [XXH3_64bits](https://xxhash.com/) non-cryptographic hash function.

    Args:
        expr: The expression to hash.
        seed (optional): Seed used for generating the hash. Defaults to 0.
        hash_function (optional): Hash function to use. One of "xxhash", "murmurhash3", or "sha1". Defaults to "xxhash".

    Note:
        Null values will produce a hash value instead of being propagated as null.

    """
    # Only pass hash_function if explicitly provided to maintain backward compatibility in string representation
    kwargs = {}
    if seed is not None:
        kwargs["seed"] = seed
    if hash_function is not None:
        kwargs["hash_function"] = hash_function
    return Expression._call_builtin_scalar_fn("hash", expr, **kwargs)


def minhash(
    expr: Expression,
    *,
    num_hashes: int,
    ngram_size: int,
    seed: int = 1,
    hash_function: Literal["murmurhash3", "xxhash", "sha1"] = "murmurhash3",
) -> Expression:
    """Runs the MinHash algorithm on the series.

    For a string, calculates the minimum hash over all its ngrams,
    repeating with `num_hashes` permutations. Returns as a list of 32-bit unsigned integers.

    Tokens for the ngrams are delimited by spaces.
    The strings are not normalized or pre-processed, so it is recommended
    to normalize the strings yourself.

    Args:
        expr: The expression to hash.
        num_hashes: The number of hash permutations to compute.
        ngram_size: The number of tokens in each shingle/ngram.
        seed (optional): Seed used for generating permutations and the initial string hashes. Defaults to 1.
        hash_function (optional): Hash function to use for initial string hashing. One of "murmurhash3", "xxhash", or "sha1". Defaults to "murmurhash3".

    """
    return Expression._call_builtin_scalar_fn(
        "minhash", expr, num_hashes=num_hashes, ngram_size=ngram_size, seed=seed, hash_function=hash_function
    )


def length(expr: Expression) -> Expression:
    """Retrieves the length of the given expression.

    The behavior depends on the input type:
    - For strings, returns the number of characters.
    - For binary, returns the number of bytes.
    - For lists, returns the number of elements.

    Returns:
        Expression: an UInt64 expression with the length

    Examples:
        String length:
        >>> import daft
        >>> from daft.functions import length
        >>>
        >>> df = daft.from_pydict({"x": ["foo", "bar", None]})
        >>> df = df.select(length(df["x"]))
        >>> df.show()
        ╭────────╮
        │ x      │
        │ ---    │
        │ UInt64 │
        ╞════════╡
        │ 3      │
        ├╌╌╌╌╌╌╌╌┤
        │ 3      │
        ├╌╌╌╌╌╌╌╌┤
        │ None   │
        ╰────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Binary length:
        >>> df = daft.from_pydict({"x": [b"foo", b"bar", None]})
        >>> df = df.select(length(df["x"]))
        >>> df.show()
        ╭────────╮
        │ x      │
        │ ---    │
        │ UInt64 │
        ╞════════╡
        │ 3      │
        ├╌╌╌╌╌╌╌╌┤
        │ 3      │
        ├╌╌╌╌╌╌╌╌┤
        │ None   │
        ╰────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        List length:
        >>> df = daft.from_pydict({"x": [[1, 2, 3], [4, 5], None]})
        >>> df = df.select(length(df["x"]))
        >>> df.show()
        ╭────────╮
        │ x      │
        │ ---    │
        │ UInt64 │
        ╞════════╡
        │ 3      │
        ├╌╌╌╌╌╌╌╌┤
        │ 2      │
        ├╌╌╌╌╌╌╌╌┤
        │ None   │
        ╰────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("length", expr)


def concat(left: Expression | str | bytes, right: Expression | str | bytes) -> Expression:
    r"""Concatenates two string or binary values.

    Returns:
        Expression: an expression with the same type as the inputs

    Examples:
        String concatenation:

        >>> import daft
        >>> from daft.functions import concat
        >>>
        >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"], "y": ["a", "b", "c"]})
        >>> df.select(concat(df["x"], df["y"])).collect()
        ╭──────╮
        │ x    │
        │ ---  │
        │ Utf8 │
        ╞══════╡
        │ fooa │
        ├╌╌╌╌╌╌┤
        │ barb │
        ├╌╌╌╌╌╌┤
        │ bazc │
        ╰──────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Binary concatenation:

        >>> df = daft.from_pydict(
        ...     {"a": [b"Hello", b"\\xff\\xfe", b"", b"World"], "b": [b" World", b"\\x00", b"empty", b"!"]}
        ... )
        >>> df = df.select(concat(df["a"], df["b"]))
        >>> df.show()
        ╭────────────────────╮
        │ a                  │
        │ ---                │
        │ Binary             │
        ╞════════════════════╡
        │ b"Hello World"     │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b"\\xff\\xfe\\x00" │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b"empty"           │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b"World!"          │
        ╰────────────────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)

    """
    return Expression._to_expression(left) + Expression._to_expression(right)


def coalesce(*args: Expression) -> Expression:
    """Returns the first non-null value in a list of expressions. If all inputs are null, returns null.

    Args:
        *args: Two or more expressions to coalesce

    Returns:
        Expression: Expression containing first non-null value encountered when evaluating arguments in order

    Examples:
        >>> import daft
        >>> from daft.functions import coalesce
        >>> df = daft.from_pydict({"x": [1, None, 3], "y": [None, 2, None]})
        >>> df = df.with_column("first_valid", coalesce(df["x"], df["y"]))
        >>> df.show()
        ╭───────┬───────┬─────────────╮
        │ x     ┆ y     ┆ first_valid │
        │ ---   ┆ ---   ┆ ---         │
        │ Int64 ┆ Int64 ┆ Int64       │
        ╞═══════╪═══════╪═════════════╡
        │ 1     ┆ None  ┆ 1           │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ None  ┆ 2     ┆ 2           │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ 3     ┆ None  ┆ 3           │
        ╰───────┴───────┴─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("coalesce", *args)


def get(expr: Expression, key: int | str | Expression, default: Any = None) -> Expression:
    """Get an index from a list expression or a field from a struct expression.

    Args:
        expr: list or struct expression to get value from
        key: integer index for list or string field for struct. List index can be negative to index from the end of the list.
        default: default value if out of bounds. Only supported for list get

    Returns:
        An expression with the inner type of the input expression.

    Note:
        `expr.get(x)` can also be written as `expr[x]`

    Note:
        `expr.get("*")` is equivalent to `expr.unnest()`

    Examples:
        Getting elements from a list by index:

        >>> import daft
        >>> df = daft.from_pydict({"lists": [[1, 2, 3], [4, 5], [6]]})
        >>> df = df.select(df["lists"].get(0).alias("first"), df["lists"].get(-1).alias("last"))
        >>> df.show()
        ╭───────┬───────╮
        │ first ┆ last  │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 1     ┆ 3     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 4     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 6     ┆ 6     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Getting elements from a list with default value:

        >>> df = daft.from_pydict({"lists": [[1, 2], [3], []]})
        >>> df = df.select(df["lists"].get(2, default=-1))
        >>> df.show()
        ╭───────╮
        │ lists │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ -1    │
        ├╌╌╌╌╌╌╌┤
        │ -1    │
        ├╌╌╌╌╌╌╌┤
        │ -1    │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Getting fields from a struct:

        >>> df = daft.from_pydict({"structs": [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]})
        >>> df = df.select(df["structs"].get("name"), df["structs"].get("age"))
        >>> df.show()
        ╭───────┬───────╮
        │ name  ┆ age   │
        │ ---   ┆ ---   │
        │ Utf8  ┆ Int64 │
        ╞═══════╪═══════╡
        │ Alice ┆ 25    │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ Bob   ┆ 30    │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)

        Using variable indices:

        >>> df = daft.from_pydict({"lists": [[1, 2, 3], [4, 5, 6]], "indices": [0, 2]})
        >>> df = df.select(df["lists"].get(df["indices"]))
        >>> df.show()
        ╭───────╮
        │ lists │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 1     │
        ├╌╌╌╌╌╌╌┤
        │ 6     │
        ╰───────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)

        Unnesting all fields from a struct (equivalent to .unnest()):

        >>> df = daft.from_pydict({"structs": [{"x": 1, "y": 2}, {"x": 3, "y": 4}]})
        >>> df = df.select(df["structs"].get("*"))
        >>> df.show()
        ╭───────┬───────╮
        │ x     ┆ y     │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 1     ┆ 2     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 4     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 2 of 2 rows)
    """
    if isinstance(key, (int, Expression)):
        return Expression._call_builtin_scalar_fn("list_get", expr, key, default)
    elif isinstance(key, str):
        if default is not None:
            raise ValueError("`daft.functions.get` does not support default values for getting a struct field")
        return Expression._from_pyexpr(expr._expr.struct_get(key))
    else:
        raise TypeError(
            f"Argument {key} of type {type(key)} is not supported in `daft.functions.get`. Only int and string types are supported."
        )


def map_get(expr: Expression, key: Expression) -> Expression:
    """Retrieves the value for a key in a map column.

    Args:
        expr: the map expression to get from
        key: the key to retrieve

    Returns:
        Expression: the value expression

    Examples:
        >>> import pyarrow as pa
        >>> import daft
        >>> pa_array = pa.array([[("a", 1)], [], [("b", 2)]], type=pa.map_(pa.string(), pa.int64()))
        >>> df = daft.from_arrow(pa.table({"map_col": pa_array}))
        >>> df = df.with_column("a", df["map_col"].map_get("a"))
        >>> df.show()
        ╭──────────────────┬───────╮
        │ map_col          ┆ a     │
        │ ---              ┆ ---   │
        │ Map[Utf8: Int64] ┆ Int64 │
        ╞══════════════════╪═══════╡
        │ [{key: a,        ┆ 1     │
        │ value: 1,        ┆       │
        │ }]               ┆       │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ []               ┆ None  │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ [{key: b,        ┆ None  │
        │ value: 2,        ┆       │
        │ }]               ┆       │
        ╰──────────────────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    key_expr = Expression._to_expression(key)
    return Expression._from_pyexpr(expr._expr.map_get(key_expr._expr))


def slice(expr: Expression, start: int | Expression, end: int | Expression | None = None) -> Expression:
    r"""Get a subset of each list or binary value.

    Args:
        expr: List or binary expression to slice.
        start: Index or column of indices. The slice will include elements starting from this index. If `start` is negative, it represents an offset from the end
        end: Index or column of indices. The slice will not include elements from this index onwards. If `end` is negative, it represents an offset from the end. If not provided, the slice will include elements up to the end of the list. If start > end, an empty slice is produced.

    Returns:
        Expression: an expression with the same type as the input.

    Note:
        `expr[start:stop]` is also equivalent to `expr.slice(start, stop)`

    Examples:
        Slicing a list expression:
        >>> import daft
        >>> df = daft.from_pydict({"x": [[1, 2, 3], [4, 5, 6, 7], [8]]})
        >>> df = df.select(df["x"].slice(1, -1))
        >>> df.show()
        ╭─────────────╮
        │ x           │
        │ ---         │
        │ List[Int64] │
        ╞═════════════╡
        │ [2]         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ [5, 6]      │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ []          │
        ╰─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

        Slicing a binary expression:
        >>> df = daft.from_pydict({"x": [b"Hello World", b"\xff\xfe\x00", b"empty"]})
        >>> df = df.select(df["x"].slice(1, -2))
        >>> df.show()
        ╭─────────────╮
        │ x           │
        │ ---         │
        │ Binary      │
        ╞═════════════╡
        │ b"ello Wor" │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b""         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ b"mp"       │
        ╰─────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("slice", expr, start, end=end)
