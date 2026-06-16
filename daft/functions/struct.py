"""Struct Functions."""

from __future__ import annotations

from daft.expressions import Expression


def unnest(expr: Expression) -> Expression:
    """Flatten the fields of a struct expression into columns in a DataFrame.

    Examples:
        >>> import daft
        >>> df = daft.from_pydict(
        ...     {
        ...         "struct": [
        ...             {"x": 1, "y": 2},
        ...             {"x": 3, "y": 4},
        ...         ]
        ...     }
        ... )
        >>> unnested_df = df.select(unnest(df["struct"]))
        >>> unnested_df.show()
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
    return expr.get("*")


def to_struct(*fields: Expression, **named_fields: Expression) -> Expression:
    """Constructs a struct from the input expressions.

    Args:
        fields: Expressions to be set as struct fields, using the expression name as the field name.
        named_fields: Expressions to be set as struct fields, using the keyword arg as the field name.

    Returns:
        An expression for a struct column with the input columns as its fields.

    Examples:
        >>> import daft
        >>> from daft.functions import to_struct
        >>>
        >>> df = daft.from_pydict({"a": ["a", "b", "c"], "b": [1, 2, 3]})
        >>> df.select(to_struct(df["a"], b2=df["b"] * 2)).show()
        ╭──────────────────────────────╮
        │ struct                       │
        │ ---                          │
        │ Struct[a: String, b2: Int64] │
        ╞══════════════════════════════╡
        │ {a: a,                       │
        │ b2: 2,                       │
        │ }                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {a: b,                       │
        │ b2: 4,                       │
        │ }                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {a: c,                       │
        │ b2: 6,                       │
        │ }                            │
        ╰──────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    all_fields = list(fields) + [Expression._to_expression(field).alias(name) for name, field in named_fields.items()]
    return Expression._call_builtin_scalar_fn("struct", *all_fields)


def named_struct(*args: str | Expression) -> Expression:
    """Constructs a struct from alternating field name and value pairs.

    This mirrors the SQL ``named_struct`` function and accepts alternating string
    field names and :class:`~daft.expressions.Expression` values as positional arguments.

    Args:
        *args: Alternating ``(field_name, value)`` pairs. Field names must be string
            literals; values must be :class:`~daft.expressions.Expression` objects.

    Returns:
        An expression for a struct column with the given fields.

    Raises:
        ValueError: If an odd number of arguments is supplied, or if a field name is
            not a string.

    Examples:
        >>> import daft
        >>> from daft.functions import named_struct
        >>>
        >>> df = daft.from_pydict({"a": ["a", "b", "c"], "b": [1, 2, 3]})
        >>> df.select(named_struct("x", df["a"], "y", df["b"])).show()
        ╭──────────────────────────────╮
        │ struct                       │
        │ ---                          │
        │ Struct[x: String, y: Int64]  │
        ╞══════════════════════════════╡
        │ {x: a,                       │
        │ y: 1,                        │
        │ }                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {x: b,                       │
        │ y: 2,                        │
        │ }                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {x: c,                       │
        │ y: 3,                        │
        │ }                            │
        ╰──────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    if len(args) == 0 or len(args) % 2 != 0:
        raise ValueError("named_struct requires an even number of arguments (alternating field names and values)")

    all_fields = []
    for i in range(0, len(args), 2):
        name = args[i]
        value = args[i + 1]
        if not isinstance(name, str):
            raise ValueError(f"named_struct field names must be strings, got {type(name)} at position {i}")
        all_fields.append(Expression._to_expression(value).alias(name))

    return Expression._call_builtin_scalar_fn("struct", *all_fields)
