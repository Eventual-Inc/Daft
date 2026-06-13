"""Numeric Functions."""

from __future__ import annotations

import math

from daft.expressions import Expression


def abs(expr: Expression) -> Expression:
    """Absolute of a numeric expression."""
    return Expression._call_builtin_scalar_fn("abs", expr)


def e() -> Expression:
    """Returns Euler's number (e = 2.71828...)."""
    return Expression._call_builtin_scalar_fn("e")


def ceil(expr: Expression) -> Expression:
    """The ceiling of a numeric expression."""
    return Expression._call_builtin_scalar_fn("ceil", expr)


def floor(expr: Expression) -> Expression:
    """The floor of a numeric expression."""
    return Expression._call_builtin_scalar_fn("floor", expr)


def clip(
    expr: Expression,
    min: Expression | None = None,
    max: Expression | None = None,
) -> Expression:
    """Clips an expression to the given minimum and maximum values.

    Args:
        expr: The expression to clip
        min: Minimum value to clip to. If None (or column value is Null), no lower clipping is applied.
        max: Maximum value to clip to. If None (or column value is Null), no upper clipping is applied.
    """
    return Expression._call_builtin_scalar_fn("clip", expr, min, max)


def sign(expr: Expression) -> Expression:
    """The sign of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sign", expr)


def negate(expr: Expression) -> Expression:
    """The negative of a numeric expression."""
    return Expression._call_builtin_scalar_fn("negate", expr)


def round(expr: Expression, decimals: Expression | int = 0) -> Expression:
    """The round of a numeric expression.

    Args:
        expr: The expression to round
        decimals: number of decimal places to round to. Defaults to 0.
    """
    return Expression._call_builtin_scalar_fn("round", expr, decimals)


def sqrt(expr: Expression) -> Expression:
    """The square root of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sqrt", expr)


def cbrt(expr: Expression) -> Expression:
    """The cube root of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cbrt", expr)


def sin(expr: Expression) -> Expression:
    """The elementwise sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sin", expr)


def cos(expr: Expression) -> Expression:
    """The elementwise cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cos", expr)


def tan(expr: Expression) -> Expression:
    """The elementwise tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("tan", expr)


def csc(expr: Expression) -> Expression:
    """The elementwise cosecant of a numeric expression."""
    return Expression._call_builtin_scalar_fn("csc", expr)


def sec(expr: Expression) -> Expression:
    """The elementwise secant of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sec", expr)


def cot(expr: Expression) -> Expression:
    """The elementwise cotangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cot", expr)


def sinh(expr: Expression) -> Expression:
    """The elementwise hyperbolic sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sinh", expr)


def cosh(expr: Expression) -> Expression:
    """The elementwise hyperbolic cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cosh", expr)


def tanh(expr: Expression) -> Expression:
    """The elementwise hyperbolic tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("tanh", expr)


def arcsin(expr: Expression) -> Expression:
    """The elementwise arc sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arcsin", expr)


def arccos(expr: Expression) -> Expression:
    """The elementwise arc cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arccos", expr)


def arctan(expr: Expression) -> Expression:
    """The elementwise arc tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arctan", expr)


def arctan2(y: Expression, x: Expression) -> Expression:
    """Calculates the four quadrant arctangent of coordinates (y, x), in radians.

    * ``x = 0``, ``y = 0``: ``0``
    * ``x >= 0``: ``[-pi/2, pi/2]``
    * ``y >= 0``: ``(pi/2, pi]``
    * ``y < 0``: ``(-pi, -pi/2)``
    """
    return Expression._call_builtin_scalar_fn("arctan2", y, x)


def arctanh(expr: Expression) -> Expression:
    """The elementwise inverse hyperbolic tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arctanh", expr)


def arccosh(expr: Expression) -> Expression:
    """The elementwise inverse hyperbolic cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arccosh", expr)


def arcsinh(expr: Expression) -> Expression:
    """The elementwise inverse hyperbolic sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arcsinh", expr)


def radians(expr: Expression) -> Expression:
    """The elementwise radians of a numeric expression."""
    return Expression._call_builtin_scalar_fn("radians", expr)


def degrees(expr: Expression) -> Expression:
    """The elementwise degrees of a numeric expression."""
    return Expression._call_builtin_scalar_fn("degrees", expr)


def log2(expr: Expression) -> Expression:
    """The elementwise log base 2 of a numeric expression."""
    return Expression._call_builtin_scalar_fn("log2", expr)


def log10(expr: Expression) -> Expression:
    """The elementwise log base 10 of a numeric expression."""
    return Expression._call_builtin_scalar_fn("log10", expr)


def log(expr: Expression, base: int | float = math.e) -> Expression:
    """The elementwise log with given base, of a numeric expression.

    Args:
        expr: The expression to take the logarithm of
        base: The base of the logarithm. Defaults to e.
    """
    return Expression._call_builtin_scalar_fn("log", expr, base)


def ln(expr: Expression) -> Expression:
    """The elementwise natural log of a numeric expression."""
    return Expression._call_builtin_scalar_fn("ln", expr)


def log1p(expr: Expression) -> Expression:
    """The ln(expr + 1) of a numeric expression."""
    return Expression._call_builtin_scalar_fn("log1p", expr)


def factorial(expr: Expression) -> Expression:
    """Returns the factorial of a non-negative integer."""
    return Expression._call_builtin_scalar_fn("factorial", expr)


def hypot(a: Expression, b: Expression) -> Expression:
    """Returns sqrt(a^2 + b^2), the Euclidean norm."""
    return Expression._call_builtin_scalar_fn("hypot", a, b)


def pi() -> Expression:
    """Returns the mathematical constant pi (3.14159...)."""
    return Expression._call_builtin_scalar_fn("pi")


def pow(base: Expression, expr: Expression) -> Expression:
    """The base^expr of a numeric expression."""
    return Expression._call_builtin_scalar_fn("pow", base, expr)


def power(base: Expression, expr: Expression) -> Expression:
    """The base^expr of a numeric expression."""
    return Expression._call_builtin_scalar_fn("power", base, expr)


def pmod(a: Expression, b: Expression) -> Expression:
    """Returns the positive modulo of ``a`` by ``b``.

    Computes ``r = a % b``; returns ``r`` when ``r >= 0`` and ``(r + b) % b`` otherwise.
    Examples: ``pmod(-7, 3) == 2``, ``pmod(7, -3) == 1``, ``pmod(-7, -3) == -1``.
    Returns NULL when ``b`` is 0.
    """
    return Expression._call_builtin_scalar_fn("pmod", a, b)


def exp(expr: Expression) -> Expression:
    """The e^expr of a numeric expression."""
    return Expression._call_builtin_scalar_fn("exp", expr)


def expm1(expr: Expression) -> Expression:
    """The e^expr - 1 of a numeric expression."""
    return Expression._call_builtin_scalar_fn("expm1", expr)


def between(expr: Expression, lower: Expression | int | float, upper: Expression | int | float) -> Expression:
    """Checks if values in the Expression are between lower and upper, inclusive.

    Args:
        expr: The expression to check
        lower: Lower bound (inclusive)
        upper: Upper bound (inclusive)

    Returns:
        Expression: Boolean Expression indicating whether values are between lower and upper, inclusive.

    Examples:
        >>> import daft
        >>> from daft.functions import between
        >>> df = daft.from_pydict({"data": [1, 2, 3, 4]})
        >>> df = df.select(between(df["data"], 1, 2))
        >>> df.collect()
        ╭───────╮
        │ data  │
        │ ---   │
        │ Bool  │
        ╞═══════╡
        │ true  │
        ├╌╌╌╌╌╌╌┤
        │ true  │
        ├╌╌╌╌╌╌╌┤
        │ false │
        ├╌╌╌╌╌╌╌┤
        │ false │
        ╰───────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)

    """
    expr = Expression._to_expression(expr)
    lower = Expression._to_expression(lower)
    upper = Expression._to_expression(upper)

    return Expression._from_pyexpr(expr._expr.between(lower._expr, upper._expr))


def bin(expr: Expression) -> Expression:
    """Returns the string representation of the binary value of an integer.

    Inputs are promoted to 64-bit before conversion; negatives produce
    64-character two's-complement strings (e.g. ``bin(-1)`` returns 64 ones).
    """
    return Expression._call_builtin_scalar_fn("bin", expr)


def conv(expr: Expression, from_base: int, to_base: int) -> Expression:
    """Converts a number from base ``from_base`` to base ``to_base`` (bases 2-36).

    Positive ``to_base`` interprets negative inputs as 64-bit two's complement
    (``conv("-1", 10, 16) == "FFFFFFFFFFFFFFFF"``); negative ``to_base`` returns
    a signed result (``conv("-1", 10, -16) == "-1"``). Trailing invalid characters
    are silently truncated (``conv("11abc", 10, 16) == "B"``). Returns NULL on
    out-of-range bases, on u64 overflow during parsing, or when a negated
    magnitude exceeds 2^63.
    """
    return Expression._call_builtin_scalar_fn("conv", expr, from_base, to_base)


def bround(expr: Expression, decimals: Expression | int = 0) -> Expression:
    """Rounds a numeric expression to ``decimals`` places using HALF_EVEN (banker's) rounding.

    Negative ``decimals`` rounds to powers of 10 above the decimal point
    (e.g. ``bround(125, -1) == 120`` because 12.5 rounds to even -> 12).

    Args:
        expr: The expression to round.
        decimals: Number of decimal places to round to. Defaults to 0.
    """
    return Expression._call_builtin_scalar_fn("bround", expr, decimals)


def hex(expr: Expression) -> Expression:
    """Converts an integer/string/binary expression to its uppercase hexadecimal string.

    For integer inputs, negatives are encoded as 64-bit two's complement
    (``hex(-1) == 'FFFFFFFFFFFFFFFF'``). For string and binary inputs, returns
    the uppercase hex of the underlying bytes (``hex('Spark') == '537061726B'``).
    """
    return Expression._call_builtin_scalar_fn("hex", expr)


def unhex(expr: Expression) -> Expression:
    r"""Inverse of :func:`hex`: decodes a hexadecimal string into binary bytes.

    Odd-length inputs are left-padded with ``'0'`` (``unhex('F') == b'\x0f'``).
    Returns NULL when the input contains characters outside ``[0-9a-fA-F]``.
    """
    return Expression._call_builtin_scalar_fn("unhex", expr)


def greatest(*exprs: Expression) -> Expression:
    """Returns the largest value among the inputs, skipping NULLs row-wise.

    Returns NULL only when all inputs in a row are NULL. Inputs are promoted
    to a common supertype before comparison. Requires at least one argument.

    Examples:
        >>> import daft
        >>> from daft.functions import greatest
        >>> df = daft.from_pydict({"a": [1, None, 3], "b": [2, 5, 1], "c": [None, 4, 6]})
        >>> df = df.with_column("g", greatest(df["a"], df["b"], df["c"]))
        >>> df.show()
        ╭───────┬───────┬───────┬───────╮
        │ a     ┆ b     ┆ c     ┆ g     │
        │ ---   ┆ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╪═══════╡
        │ 1     ┆ 2     ┆ None  ┆ 2     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ None  ┆ 5     ┆ 4     ┆ 5     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 1     ┆ 6     ┆ 6     │
        ╰───────┴───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("greatest", *exprs)


def least(*exprs: Expression) -> Expression:
    """Returns the smallest value among the inputs, skipping NULLs row-wise.

    Returns NULL only when all inputs in a row are NULL. Inputs are promoted
    to a common supertype before comparison. Requires at least one argument.

    Examples:
        >>> import daft
        >>> from daft.functions import least
        >>> df = daft.from_pydict({"a": [1, None, 3], "b": [2, 5, 1], "c": [None, 4, 6]})
        >>> df = df.with_column("l", least(df["a"], df["b"], df["c"]))
        >>> df.show()
        ╭───────┬───────┬───────┬───────╮
        │ a     ┆ b     ┆ c     ┆ l     │
        │ ---   ┆ ---   ┆ ---   ┆ ---   │
        │ Int64 ┆ Int64 ┆ Int64 ┆ Int64 │
        ╞═══════╪═══════╪═══════╪═══════╡
        │ 1     ┆ 2     ┆ None  ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ None  ┆ 5     ┆ 4     ┆ 4     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 1     ┆ 6     ┆ 1     │
        ╰───────┴───────┴───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)
    """
    return Expression._call_builtin_scalar_fn("least", *exprs)


def is_nan(expr: Expression) -> Expression:
    """Checks if values are NaN (a special float value indicating not-a-number).

    Returns:
        Expression: Boolean Expression indicating whether values are invalid.

    Note:
        Nulls will be propagated! I.e. this operation will return a null for null values.

    Examples:
        >>> import daft
        >>> from daft.functions import is_nan
        >>>
        >>> df = daft.from_pydict({"data": [1.0, None, float("nan")]})
        >>> df = df.select(is_nan(df["data"]))
        >>> df.collect()
        ╭───────╮
        │ data  │
        │ ---   │
        │ Bool  │
        ╞═══════╡
        │ false │
        ├╌╌╌╌╌╌╌┤
        │ None  │
        ├╌╌╌╌╌╌╌┤
        │ true  │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("is_nan", expr)


def is_inf(expr: Expression) -> Expression:
    """Checks if values in the Expression are Infinity.

    Returns:
        Expression: Boolean Expression indicating whether values are Infinity.

    Note:
        Nulls will be propagated! I.e. this operation will return a null for null values.

    Examples:
        >>> import daft
        >>> from daft.functions import is_inf
        >>>
        >>> df = daft.from_pydict({"data": [-float("inf"), 0.0, float("inf"), None]})
        >>> df = df.select(is_inf(df["data"]))
        >>> df.collect()
        ╭───────╮
        │ data  │
        │ ---   │
        │ Bool  │
        ╞═══════╡
        │ true  │
        ├╌╌╌╌╌╌╌┤
        │ false │
        ├╌╌╌╌╌╌╌┤
        │ true  │
        ├╌╌╌╌╌╌╌┤
        │ None  │
        ╰───────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)

    """
    return Expression._call_builtin_scalar_fn("is_inf", expr)


def not_nan(expr: Expression) -> Expression:
    """Checks if values are not NaN (a special float value indicating not-a-number).

    Returns:
        Expression: Boolean Expression indicating whether values are not invalid.

    Note:
        Nulls will be propagated! I.e. this operation will return a null for null values.

    Examples:
        >>> import daft
        >>> from daft.functions import not_nan
        >>>
        >>> df = daft.from_pydict({"x": [1.0, None, float("nan")]})
        >>> df = df.select(not_nan(df["x"]))
        >>> df.collect()
        ╭───────╮
        │ x     │
        │ ---   │
        │ Bool  │
        ╞═══════╡
        │ true  │
        ├╌╌╌╌╌╌╌┤
        │ None  │
        ├╌╌╌╌╌╌╌┤
        │ false │
        ╰───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("not_nan", expr)


def fill_nan(expr: Expression, fill_value: Expression) -> Expression:
    """Fills NaN values in the Expression with the provided fill_value.

    Returns:
        Expression: Expression with Nan values filled with the provided fill_value

    Examples:
        >>> import daft
        >>> from daft.functions import fill_nan
        >>>
        >>> df = daft.from_pydict({"data": [1.1, float("nan"), 3.3]})
        >>> df = df.with_column("filled", fill_nan(df["data"], 2.2))
        >>> df.show()
        ╭─────────┬─────────╮
        │ data    ┆ filled  │
        │ ---     ┆ ---     │
        │ Float64 ┆ Float64 │
        ╞═════════╪═════════╡
        │ 1.1     ┆ 1.1     │
        ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ NaN     ┆ 2.2     │
        ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
        │ 3.3     ┆ 3.3     │
        ╰─────────┴─────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._call_builtin_scalar_fn("fill_nan", expr, fill_value)
