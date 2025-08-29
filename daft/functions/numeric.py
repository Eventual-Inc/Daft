from __future__ import annotations

import math

from daft.expressions import Expression


def abs(expr: Expression | int | float) -> Expression:
    """Absolute of a numeric expression."""
    return Expression._call_builtin_scalar_fn("abs", expr)


def ceil(expr: Expression | int | float) -> Expression:
    """The ceiling of a numeric expression."""
    return Expression._call_builtin_scalar_fn("ceil", expr)


def floor(expr: Expression | int | float) -> Expression:
    """The floor of a numeric expression."""
    return Expression._call_builtin_scalar_fn("floor", expr)


def clip(
    expr: Expression | int | float,
    min: Expression | int | float | None = None,
    max: Expression | int | float | None = None,
) -> Expression:
    """Clips an expression to the given minimum and maximum values.

    Args:
        expr: The expression to clip
        min: Minimum value to clip to. If None (or column value is Null), no lower clipping is applied.
        max: Maximum value to clip to. If None (or column value is Null), no upper clipping is applied.
    """
    return Expression._call_builtin_scalar_fn("clip", expr, min, max)


def sign(expr: Expression | int | float) -> Expression:
    """The sign of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sign", expr)


def negate(expr: Expression | int | float) -> Expression:
    """The negative of a numeric expression."""
    return Expression._call_builtin_scalar_fn("negative", expr)


def round(expr: Expression | int | float, decimals: Expression | int = 0) -> Expression:
    """The round of a numeric expression.

    Args:
        expr: The expression to round
        decimals: number of decimal places to round to. Defaults to 0.
    """
    return Expression._call_builtin_scalar_fn("round", expr, decimals)


def sqrt(expr: Expression | int | float) -> Expression:
    """The square root of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sqrt", expr)


def cbrt(expr: Expression | int | float) -> Expression:
    """The cube root of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cbrt", expr)


def sin(expr: Expression | int | float) -> Expression:
    """The elementwise sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sin", expr)


def cos(expr: Expression | int | float) -> Expression:
    """The elementwise cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cos", expr)


def tan(expr: Expression | int | float) -> Expression:
    """The elementwise tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("tan", expr)


def csc(expr: Expression | int | float) -> Expression:
    """The elementwise cosecant of a numeric expression."""
    return Expression._call_builtin_scalar_fn("csc", expr)


def sec(expr: Expression | int | float) -> Expression:
    """The elementwise secant of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sec", expr)


def cot(expr: Expression | int | float) -> Expression:
    """The elementwise cotangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cot", expr)


def sinh(expr: Expression | int | float) -> Expression:
    """The elementwise hyperbolic sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("sinh", expr)


def cosh(expr: Expression | int | float) -> Expression:
    """The elementwise hyperbolic cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("cosh", expr)


def tanh(expr: Expression | int | float) -> Expression:
    """The elementwise hyperbolic tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("tanh", expr)


def arcsin(expr: Expression | int | float) -> Expression:
    """The elementwise arc sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arcsin", expr)


def arccos(expr: Expression | int | float) -> Expression:
    """The elementwise arc cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arccos", expr)


def arctan(expr: Expression | int | float) -> Expression:
    """The elementwise arc tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arctan", expr)


def arctan2(y: Expression | int | float, x: Expression | int | float) -> Expression:
    """Calculates the four quadrant arctangent of coordinates (y, x), in radians.

    * ``x = 0``, ``y = 0``: ``0``
    * ``x >= 0``: ``[-pi/2, pi/2]``
    * ``y >= 0``: ``(pi/2, pi]``
    * ``y < 0``: ``(-pi, -pi/2)``
    """
    return Expression._call_builtin_scalar_fn("arctan2", y, x)


def arctanh(expr: Expression | int | float) -> Expression:
    """The elementwise inverse hyperbolic tangent of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arctanh", expr)


def arccosh(expr: Expression | int | float) -> Expression:
    """The elementwise inverse hyperbolic cosine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arccosh", expr)


def arcsinh(expr: Expression | int | float) -> Expression:
    """The elementwise inverse hyperbolic sine of a numeric expression."""
    return Expression._call_builtin_scalar_fn("arcsinh", expr)


def radians(expr: Expression | int | float) -> Expression:
    """The elementwise radians of a numeric expression."""
    return Expression._call_builtin_scalar_fn("radians", expr)


def degrees(expr: Expression | int | float) -> Expression:
    """The elementwise degrees of a numeric expression."""
    return Expression._call_builtin_scalar_fn("degrees", expr)


def log2(expr: Expression | int | float) -> Expression:
    """The elementwise log base 2 of a numeric expression."""
    return Expression._call_builtin_scalar_fn("log2", expr)


def log10(expr: Expression | int | float) -> Expression:
    """The elementwise log base 10 of a numeric expression."""
    return Expression._call_builtin_scalar_fn("log10", expr)


def log(expr: Expression | int | float, base: int | float = math.e) -> Expression:
    """The elementwise log with given base, of a numeric expression.

    Args:
        expr: The expression to take the logarithm of
        base: The base of the logarithm. Defaults to e.
    """
    return Expression._call_builtin_scalar_fn("log", expr, base)


def ln(expr: Expression | int | float) -> Expression:
    """The elementwise natural log of a numeric expression."""
    return Expression._call_builtin_scalar_fn("ln", expr)


def log1p(expr: Expression | int | float) -> Expression:
    """The ln(expr + 1) of a numeric expression."""
    return Expression._call_builtin_scalar_fn("log1p", expr)


def exp(expr: Expression | int | float) -> Expression:
    """The e^expr of a numeric expression."""
    return Expression._call_builtin_scalar_fn("exp", expr)


def expm1(expr: Expression | int | float) -> Expression:
    """The e^expr - 1 of a numeric expression."""
    return Expression._call_builtin_scalar_fn("expm1", expr)


def between(
    expr: Expression | int | float, lower: Expression | int | float, upper: Expression | int | float
) -> Expression:
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
        ╭─────────╮
        │ data    │
        │ ---     │
        │ Boolean │
        ╞═════════╡
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ true    │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ├╌╌╌╌╌╌╌╌╌┤
        │ false   │
        ╰─────────╯
        <BLANKLINE>
        (Showing first 4 of 4 rows)

    """
    expr = Expression._to_expression(expr)
    lower = Expression._to_expression(lower)
    upper = Expression._to_expression(upper)

    return Expression._from_pyexpr(expr._expr.between(lower._expr, upper._expr))
