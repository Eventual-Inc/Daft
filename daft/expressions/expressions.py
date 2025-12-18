from __future__ import annotations

import builtins
import math
from collections.abc import Iterable, Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    overload,
)

import daft.daft as native
from daft.daft import (
    CountMode,
    ImageFormat,
    ImageMode,
    ImageProperty,
    ResourceRequest,
    initialize_udfs,
    resolved_col,
    unresolved_col,
)
from daft.daft import PyExpr as _PyExpr
from daft.daft import lit as _lit
from daft.daft import udf as _udf
from daft.datatype import DataType, DataTypeLike, TimeUnit
from daft.expressions.testing import expr_structurally_equal
from daft.logical.schema import Field, Schema

if TYPE_CHECKING:
    from daft.dependencies import pc
    from daft.io import IOConfig
    from daft.series import Series
    from daft.udf.legacy import BoundUDFArgs, InitArgsType, UninitializedUdf
    from daft.window import Window

    ENCODING_CHARSET = Literal["utf-8", "utf8", "base64"]
    COMPRESSION_CODEC = Literal["deflate", "gzip", "gz", "zlib"]


def lit(value: object) -> Expression:
    """Creates an Expression representing a column with every value set to the provided value.

    Args:
        value: value of the literal

    Returns:
        Expression: Expression representing the value provided

    Examples:
        >>> import daft
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("y", daft.lit(1))
        >>> df.show()
        ╭───────┬───────╮
        │ x     ┆ y     │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int64 │
        ╞═══════╪═══════╡
        │ 1     ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 1     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    return Expression._from_pyexpr(_lit(value))


def element() -> Expression:
    """Creates an expression referring to an elementwise list operation.

    This is used to create an expression that operates on each element of a list column.

    If used outside of a list column, it will raise an error.
    """
    return col("")


def col(name: str) -> Expression:
    """Creates an Expression referring to the column with the provided name.

    Args:
        name: Name of column

    Returns:
        Expression: Expression representing the selected column

    Examples:
        >>> import daft
        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> df = df.select(daft.col("x"))
        >>> df.show()
        ╭───────╮
        │ x     │
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
    return Expression._from_pyexpr(unresolved_col(name))


def _resolved_col(name: str) -> Expression:
    """Creates a resolved column."""
    return Expression._from_pyexpr(resolved_col(name))


def interval(
    years: int | None = None,
    months: int | None = None,
    days: int | None = None,
    hours: int | None = None,
    minutes: int | None = None,
    seconds: int | None = None,
    millis: int | None = None,
    nanos: int | None = None,
) -> Expression:
    """Creates an Expression representing an interval."""
    lit_value = native.interval_lit(
        years=years, months=months, days=days, hours=hours, minutes=minutes, seconds=seconds, millis=millis, nanos=nanos
    )
    return Expression._from_pyexpr(lit_value)


class Expression:
    _expr: _PyExpr = None  # type: ignore

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Expression via __init__ ")

    @staticmethod
    def _from_pyexpr(pyexpr: _PyExpr) -> Expression:
        expr = Expression.__new__(Expression)
        expr._expr = pyexpr
        return expr

    def is_column(self) -> bool:
        return self._expr.is_column()

    def is_literal(self) -> bool:
        return self._expr.is_literal()

    def column_name(self) -> builtins.str | None:
        return self._expr.column_name()

    @staticmethod
    def _to_expression(obj: object) -> Expression:
        if isinstance(obj, Expression):
            return obj
        else:
            return lit(obj)

    def to_arrow_expr(self) -> pc.Expression:
        """Returns this expression as a pyarrow.compute.Expression for integrations with other systems."""
        from daft.expressions.pyarrow_visitor import _PyArrowExpressionVisitor

        return _PyArrowExpressionVisitor().visit(self)

    def as_py(self) -> Any:
        """Returns this literal expression as a python value, raises a ValueError if this is not a literal expression."""
        return self._expr.as_py()

    @staticmethod
    def udf(
        name: builtins.str,
        inner: UninitializedUdf,
        bound_args: BoundUDFArgs,
        expressions: builtins.list[Expression],
        return_dtype: DataType,
        init_args: InitArgsType,
        resource_request: ResourceRequest | None,
        batch_size: int | None,
        concurrency: int | None,
        use_process: bool | None,
        ray_options: dict[builtins.str, builtins.str] | None = None,
    ) -> Expression:
        return Expression._from_pyexpr(
            _udf(
                name,
                inner,
                bound_args,
                [e._expr for e in expressions],
                return_dtype._dtype,
                init_args,
                resource_request,
                batch_size,
                concurrency,
                use_process,
                ray_options,
            )
        )

    def unnest(self) -> Expression:
        """Flatten the fields of a struct expression into columns in a DataFrame.

        Tip: See Also
            [`daft.functions.unnest`](https://docs.daft.ai/en/stable/api/functions/unnest/)
        """
        from daft.functions import unnest

        return unnest(self)

    def __bool__(self) -> bool:
        raise ValueError(
            "Expressions don't have a truth value. "
            "If you used Python keywords `and` `not` `or` on an expression, use `&` `~` `|` instead."
        )

    def __abs__(self) -> Expression:
        """Absolute of a numeric expression."""
        return self.abs()

    def abs(self) -> Expression:
        """Absolute of a numeric expression.

        Tip: See Also
            [`daft.functions.abs`](https://docs.daft.ai/en/stable/api/functions/abs/)
        """
        from daft.functions import abs

        return abs(self)

    def __add__(self, other: object) -> Expression:
        """Adds two numeric expressions or concatenates two string expressions (``e1 + e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr + expr._expr)

    def __radd__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr + self._expr)

    def __sub__(self, other: object) -> Expression:
        """Subtracts two numeric expressions (``e1 - e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr - expr._expr)

    def __rsub__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr - self._expr)

    def __mul__(self, other: object) -> Expression:
        """Multiplies two numeric expressions (``e1 * e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr * expr._expr)

    def __rmul__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr * self._expr)

    def __truediv__(self, other: object) -> Expression:
        """True divides two numeric expressions (``e1 / e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr / expr._expr)

    def __rtruediv__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr / self._expr)

    def __mod__(self, other: Expression) -> Expression:
        """Takes the mod of two numeric expressions (``e1 % e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr % expr._expr)

    def __rmod__(self, other: Expression) -> Expression:
        """Takes the mod of two numeric expressions (``e1 % e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr % self._expr)

    def __and__(self, other: Expression) -> Expression:
        """Takes the logical AND of two boolean expressions, or bitwise AND of two integer expressions (``e1 & e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr & expr._expr)

    def __rand__(self, other: Expression) -> Expression:
        """Takes the logical reverse AND of two boolean expressions (``e1 & e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr & self._expr)

    def __or__(self, other: Expression) -> Expression:
        """Takes the logical OR of two boolean or integer expressions, or bitwise OR of two integer expressions (``e1 | e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr | expr._expr)

    def __xor__(self, other: Expression) -> Expression:
        """Takes the logical XOR of two boolean or integer expressions, or bitwise XOR of two integer expressions (``e1 ^ e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr ^ expr._expr)

    def __ror__(self, other: Expression) -> Expression:
        """Takes the logical reverse OR of two boolean expressions (``e1 | e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr | self._expr)

    def __lt__(self, other: Expression) -> Expression:
        """Compares if an expression is less than another (``e1 < e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr < expr._expr)

    def __le__(self, other: Expression) -> Expression:
        """Compares if an expression is less than or equal to another (``e1 <= e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr <= expr._expr)

    def __eq__(self, other: Expression) -> Expression:  # type: ignore
        """Compares if an expression is equal to another (``e1 == e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr == expr._expr)

    def eq_null_safe(self, other: Expression | Any) -> Expression:
        """Performs a null-safe equality comparison between two expressions.

        Tip: See Also
            [`daft.functions.eq_null_safe`](https://docs.daft.ai/en/stable/api/functions/eq_null_safe/)
        """
        from daft.functions import eq_null_safe

        return eq_null_safe(self, other)

    def __ne__(self, other: Expression) -> Expression:  # type: ignore
        """Compares if an expression is not equal to another (``e1 != e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr != expr._expr)

    def __gt__(self, other: Expression) -> Expression:
        """Compares if an expression is greater than another (``e1 > e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr > expr._expr)

    def __ge__(self, other: Expression) -> Expression:
        """Compares if an expression is greater than or equal to another (``e1 >= e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >= expr._expr)

    def __lshift__(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the left (``e1 << e2``).

        Args:
            other: The number of bits to shift the expression to the left
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr << expr._expr)

    def __rshift__(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the right (``e1 >> e2``).

        .. NOTE::

            For unsigned integers, this expression perform a logical right shift.
            For signed integers, this expression perform an arithmetic right shift.

        Args:
            other: The number of bits to shift the expression to the right
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >> expr._expr)

    def __invert__(self) -> Expression:
        """Inverts a boolean expression (``~e``)."""
        expr = self._expr.__invert__()
        return Expression._from_pyexpr(expr)

    def __floordiv__(self, other: Expression) -> Expression:
        """Floor divides two numeric expressions (``e1 / e2``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr // expr._expr)

    def __rfloordiv__(self, other: object) -> Expression:
        """Reverse floor divides two numeric expressions (``e2 / e1``)."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr // self._expr)

    def __getitem__(self, key: builtins.str | int | slice) -> Expression:
        """Syntactic sugar for `daft.functions.get` for string and int, and `daft.functions.slice` for slice.

        Examples:
            Getting a single value:
            >>> import daft
            >>> df = daft.from_pydict({"struct": [{"x": 1, "y": 2}, {"x": 3, "y": 4}], "list": [[10, 20], [30, 40]]})
            >>> df = df.select(df["struct"]["x"], df["list"][0].alias("first"))
            >>> df.show()
            ╭───────┬───────╮
            │ x     ┆ first │
            │ ---   ┆ ---   │
            │ Int64 ┆ Int64 │
            ╞═══════╪═══════╡
            │ 1     ┆ 10    │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 3     ┆ 30    │
            ╰───────┴───────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

            Getting a slice:
            >>> df = daft.from_pydict({"x": [[1, 2, 3], [4, 5, 6, 7], [8]]})
            >>> df = df.select(df["x"][1:-1])
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

        Tip: See Also
            [`daft.functions.get`](https://docs.daft.ai/en/stable/api/functions/get/)
            [`daft.functions.slice`](https://docs.daft.ai/en/stable/api/functions/slice/)

        """
        if isinstance(key, builtins.slice):
            from daft.functions import slice

            if key.step is not None:
                raise ValueError(
                    "`Expression.__getitem__` does not yet support slicing with step: `expr[start:stop:step]`"
                )

            start = key.start if key.start is not None else 0
            return slice(self, start, key.stop)
        else:
            from daft.functions import get

            return get(self, key)

    @classmethod
    def _call_builtin_scalar_fn(cls, func_name: builtins.str, *args: Any, **kwargs: Any) -> Expression:
        expr_args = [cls._to_expression(v)._expr for v in args]
        expr_kwargs = {k: cls._to_expression(v)._expr for k, v in kwargs.items() if v is not None}
        f = native.get_function_from_registry(func_name)
        return cls._from_pyexpr(f(*expr_args, **expr_kwargs))

    def _eval_expressions(self, func_name: builtins.str, *args: Any, **kwargs: Any) -> Expression:
        expr_args = [Expression._to_expression(v)._expr for v in args]
        expr_kwargs = {k: Expression._to_expression(v)._expr for k, v in kwargs.items() if v is not None}
        f = native.get_function_from_registry(func_name)
        return Expression._from_pyexpr(f(self._expr, *expr_args, **expr_kwargs))

    def alias(self, name: builtins.str) -> Expression:
        """Gives the expression a new name.

        Args:
            name: New name for expression

        Returns:
            Expression: Renamed expression

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1, 2, 3]})
            >>> df = df.select(col("x").alias("y"))
            >>> df.show()
            ╭───────╮
            │ y     │
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
        assert isinstance(name, str)
        expr = self._expr.alias(name)
        return Expression._from_pyexpr(expr)

    def cast(self, dtype: DataTypeLike) -> Expression:
        """Casts an expression to the given datatype if possible.

        Tip: See Also
            [`daft.functions.cast`](https://docs.daft.ai/en/stable/api/functions/cast/)
        """
        from daft.functions import cast

        return cast(self, dtype)

    def ceil(self) -> Expression:
        """The ceiling of a numeric expression.

        Tip: See Also
            [`daft.functions.ceil`](https://docs.daft.ai/en/stable/api/functions/ceil/)
        """
        from daft.functions import ceil

        return ceil(self)

    def floor(self) -> Expression:
        """The floor of a numeric expression.

        Tip: See Also
            [`daft.functions.floor`](https://docs.daft.ai/en/stable/api/functions/floor/)
        """
        from daft.functions import floor

        return floor(self)

    def clip(
        self,
        min: Expression | None = None,
        max: Expression | None = None,
    ) -> Expression:
        """Clips an expression to the given minimum and maximum values.

        Tip: See Also
            [`daft.functions.clip`](https://docs.daft.ai/en/stable/api/functions/clip/)
        """
        from daft.functions import clip

        return clip(self, min, max)

    def sign(self) -> Expression:
        """The sign of a numeric expression.

        Tip: See Also
            [`daft.functions.sign`](https://docs.daft.ai/en/stable/api/functions/sign/)
        """
        from daft.functions import sign

        return sign(self)

    def negate(self) -> Expression:
        """The negative of a numeric expression.

        Tip: See Also
            [`daft.functions.negate`](https://docs.daft.ai/en/stable/api/functions/negate/)
        """
        from daft.functions import negate

        return negate(self)

    def round(self, decimals: Expression | int = 0) -> Expression:
        """The round of a numeric expression.

        Tip: See Also
            [`daft.functions.round`](https://docs.daft.ai/en/stable/api/functions/round/)
        """
        from daft.functions import round

        return round(self, decimals)

    def sqrt(self) -> Expression:
        """The square root of a numeric expression.

        Tip: See Also
            [`daft.functions.sqrt`](https://docs.daft.ai/en/stable/api/functions/sqrt/)
        """
        from daft.functions import sqrt

        return sqrt(self)

    def cbrt(self) -> Expression:
        """The cube root of a numeric expression.

        Tip: See Also
            [`daft.functions.cbrt`](https://docs.daft.ai/en/stable/api/functions/cbrt/)
        """
        from daft.functions import cbrt

        return cbrt(self)

    def sin(self) -> Expression:
        """The elementwise sine of a numeric expression.

        Tip: See Also
            [`daft.functions.sin`](https://docs.daft.ai/en/stable/api/functions/sin/)
        """
        from daft.functions import sin

        return sin(self)

    def cos(self) -> Expression:
        """The elementwise cosine of a numeric expression.

        Tip: See Also
            [`daft.functions.cos`](https://docs.daft.ai/en/stable/api/functions/cos/)
        """
        from daft.functions import cos

        return cos(self)

    def tan(self) -> Expression:
        """The elementwise tangent of a numeric expression.

        Tip: See Also
            [`daft.functions.tan`](https://docs.daft.ai/en/stable/api/functions/tan/)
        """
        from daft.functions import tan

        return tan(self)

    def csc(self) -> Expression:
        """The elementwise cosecant of a numeric expression.

        Tip: See Also
            [`daft.functions.csc`](https://docs.daft.ai/en/stable/api/functions/csc/)
        """
        from daft.functions import csc

        return csc(self)

    def sec(self) -> Expression:
        """The elementwise secant of a numeric expression.

        Tip: See Also
            [`daft.functions.sec`](https://docs.daft.ai/en/stable/api/functions/sec/)
        """
        from daft.functions import sec

        return sec(self)

    def cot(self) -> Expression:
        """The elementwise cotangent of a numeric expression.

        Tip: See Also
            [`daft.functions.cot`](https://docs.daft.ai/en/stable/api/functions/cot/)
        """
        from daft.functions import cot

        return cot(self)

    def sinh(self) -> Expression:
        """The elementwise hyperbolic sine of a numeric expression.

        Tip: See Also
            [`daft.functions.sinh`](https://docs.daft.ai/en/stable/api/functions/sinh/)
        """
        from daft.functions import sinh

        return sinh(self)

    def cosh(self) -> Expression:
        """The elementwise hyperbolic cosine of a numeric expression.

        Tip: See Also
            [`daft.functions.cosh`](https://docs.daft.ai/en/stable/api/functions/cosh/)
        """
        from daft.functions import cosh

        return cosh(self)

    def tanh(self) -> Expression:
        """The elementwise hyperbolic tangent of a numeric expression.

        Tip: See Also
            [`daft.functions.tanh`](https://docs.daft.ai/en/stable/api/functions/tanh/)
        """
        from daft.functions import tanh

        return tanh(self)

    def arcsin(self) -> Expression:
        """The elementwise arc sine of a numeric expression.

        Tip: See Also
            [`daft.functions.arcsin`](https://docs.daft.ai/en/stable/api/functions/arcsin/)
        """
        from daft.functions import arcsin

        return arcsin(self)

    def arccos(self) -> Expression:
        """The elementwise arc cosine of a numeric expression.

        Tip: See Also
            [`daft.functions.arccos`](https://docs.daft.ai/en/stable/api/functions/arccos/)
        """
        from daft.functions import arccos

        return arccos(self)

    def arctan(self) -> Expression:
        """The elementwise arc tangent of a numeric expression.

        Tip: See Also
            [`daft.functions.arctan`](https://docs.daft.ai/en/stable/api/functions/arctan/)
        """
        from daft.functions import arctan

        return arctan(self)

    def arctan2(self, other: Expression) -> Expression:
        """Calculates the four quadrant arctangent of coordinates (y, x), in radians.

        Tip: See Also
            [`daft.functions.arctan2`](https://docs.daft.ai/en/stable/api/functions/arctan2/)
        """
        from daft.functions import arctan2

        return arctan2(self, other)

    def arctanh(self) -> Expression:
        """The elementwise inverse hyperbolic tangent of a numeric expression.

        Tip: See Also
            [`daft.functions.arctanh`](https://docs.daft.ai/en/stable/api/functions/arctanh/)
        """
        from daft.functions import arctanh

        return arctanh(self)

    def arccosh(self) -> Expression:
        """The elementwise inverse hyperbolic cosine of a numeric expression.

        Tip: See Also
            [`daft.functions.arccosh`](https://docs.daft.ai/en/stable/api/functions/arccosh/)
        """
        from daft.functions import arccosh

        return arccosh(self)

    def arcsinh(self) -> Expression:
        """The elementwise inverse hyperbolic sine of a numeric expression.

        Tip: See Also
            [`daft.functions.arcsinh`](https://docs.daft.ai/en/stable/api/functions/arcsinh/)
        """
        from daft.functions import arcsinh

        return arcsinh(self)

    def radians(self) -> Expression:
        """The elementwise radians of a numeric expression.

        Tip: See Also
            [`daft.functions.radians`](https://docs.daft.ai/en/stable/api/functions/radians/)
        """
        from daft.functions import radians

        return radians(self)

    def degrees(self) -> Expression:
        """The elementwise degrees of a numeric expression.

        Tip: See Also
            [`daft.functions.degrees`](https://docs.daft.ai/en/stable/api/functions/degrees/)
        """
        from daft.functions import degrees

        return degrees(self)

    def log2(self) -> Expression:
        """The elementwise log base 2 of a numeric expression.

        Tip: See Also
            [`daft.functions.log2`](https://docs.daft.ai/en/stable/api/functions/log2/)
        """
        from daft.functions import log2

        return log2(self)

    def log10(self) -> Expression:
        """The elementwise log base 10 of a numeric expression.

        Tip: See Also
            [`daft.functions.log10`](https://docs.daft.ai/en/stable/api/functions/log10/)
        """
        from daft.functions import log10

        return log10(self)

    def log(self, base: int | builtins.float = math.e) -> Expression:
        """The elementwise log with given base, of a numeric expression.

        Tip: See Also
            [`daft.functions.log`](https://docs.daft.ai/en/stable/api/functions/log/)
        """
        from daft.functions import log

        return log(self, base=base)

    def ln(self) -> Expression:
        """The elementwise natural log of a numeric expression.

        Tip: See Also
            [`daft.functions.ln`](https://docs.daft.ai/en/stable/api/functions/ln/)
        """
        from daft.functions import ln

        return ln(self)

    def log1p(self) -> Expression:
        """The ln(self + 1) of a numeric expression.

        Tip: See Also
            [`daft.functions.log1p`](https://docs.daft.ai/en/stable/api/functions/log1p/)
        """
        from daft.functions import log1p

        return log1p(self)

    def pow(self, exp: Expression) -> Expression:
        """The elementwise exponentiation of a numeric series.

        Args:
            exp: The exponent to raise each element to.
        """
        from daft.functions import pow

        return pow(self, exp)

    def power(self, exp: Expression) -> Expression:
        """The elementwise exponentiation of a numeric series.

        Args:
            exp: The exponent to raise each element to.
        """
        from daft.functions import power

        return power(self, exp)

    def exp(self) -> Expression:
        """The e^self of a numeric expression.

        Tip: See Also
            [`daft.functions.exp`](https://docs.daft.ai/en/stable/api/functions/exp/)
        """
        from daft.functions import exp

        return exp(self)

    def expm1(self) -> Expression:
        """The e^self - 1 of a numeric expression.

        Tip: See Also
            [`daft.functions.expm1`](https://docs.daft.ai/en/stable/api/functions/expm1/)
        """
        from daft.functions import expm1

        return expm1(self)

    def bitwise_and(self, other: Expression) -> Expression:
        """Bitwise AND of two integer expressions.

        Tip: See Also
            [`daft.functions.bitwise_and`](https://docs.daft.ai/en/stable/api/functions/bitwise_and/)
        """
        from daft.functions import bitwise_and

        return bitwise_and(self, other)

    def bitwise_or(self, other: Expression) -> Expression:
        """Bitwise OR of two integer expressions.

        Tip: See Also
            [`daft.functions.bitwise_or`](https://docs.daft.ai/en/stable/api/functions/bitwise_or/)
        """
        from daft.functions import bitwise_or

        return bitwise_or(self, other)

    def bitwise_xor(self, other: Expression) -> Expression:
        """Bitwise XOR of two integer expressions.

        Tip: See Also
            [`daft.functions.bitwise_xor`](https://docs.daft.ai/en/stable/api/functions/bitwise_xor/)
        """
        from daft.functions import bitwise_xor

        return bitwise_xor(self, other)

    def shift_left(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the left (``expr << other``).

        Tip: See Also
            [`daft.functions.shift_left`](https://docs.daft.ai/en/stable/api/functions/shift_left/)
        """
        from daft.functions import shift_left

        return shift_left(self, other)

    def shift_right(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the right (``expr >> other``).

        Tip: See Also
            [`daft.functions.shift_right`](https://docs.daft.ai/en/stable/api/functions/shift_right/)
        """
        from daft.functions import shift_right

        return shift_right(self, other)

    def count(self, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid) -> Expression:
        """Counts the number of values in the expression.

        Tip: See Also
            [`daft.functions.count`](https://docs.daft.ai/en/stable/api/functions/count)
        """
        from daft.functions import count

        return count(self, mode=mode)

    def count_distinct(self) -> Expression:
        """Counts the number of distinct values in the expression.

        Tip: See Also
            [`daft.functions.count_distinct`](https://docs.daft.ai/en/stable/api/functions/count_distinct)
        """
        from daft.functions import count_distinct

        return count_distinct(self)

    def sum(self) -> Expression:
        """Calculates the sum of the values in the expression.

        Tip: See Also
            [`daft.functions.sum`](https://docs.daft.ai/en/stable/api/functions/sum/)
        """
        from daft.functions import sum

        return sum(self)

    def product(self) -> Expression:
        """Calculates the product of the values in the expression.

        Tip: See Also
            [`daft.functions.product`](https://docs.daft.ai/en/stable/api/functions/product/)
        """
        from daft.functions import product

        return product(self)

    def approx_count_distinct(self) -> Expression:
        """Calculates the approximate number of non-`NULL` distinct values in the expression.

        Tip: See Also
              [`daft.functions.approx_count_distinct`](https://docs.daft.ai/en/stable/api/functions/approx_count_distinct/)
        """
        from daft.functions import approx_count_distinct

        return approx_count_distinct(self)

    def approx_percentiles(self, percentiles: builtins.float | builtins.list[builtins.float]) -> Expression:
        """Calculates the approximate percentile(s) for a column of numeric values.

        Tip: See Also
            [`daft.functions.approx_percentiles`](https://docs.daft.ai/en/stable/api/functions/approx_percentiles/)
        """
        from daft.functions import approx_percentiles

        return approx_percentiles(self, percentiles)

    def mean(self) -> Expression:
        """Calculates the mean of the values in the expression.

        Tip: See Also
            [`daft.functions.mean`](https://docs.daft.ai/en/stable/api/functions/mean/)
        """
        from daft.functions import mean

        return mean(self)

    def avg(self) -> Expression:
        """Alias for `Expression.mean()`. Check [`Expression.mean`](https://docs.daft.ai/en/stable/api/expressions/#daft.expressions.Expression.mean) for more details."""
        from daft.functions import mean

        return mean(self)

    def stddev(self) -> Expression:
        """Calculates the standard deviation of the values in the expression.

        Tip: See Also
            [`daft.functions.stddev`](https://docs.daft.ai/en/stable/api/functions/stddev/)
        """
        from daft.functions import stddev

        return stddev(self)

    def min(self) -> Expression:
        """Calculates the minimum value in the expression.

        Tip: See Also
            [`daft.functions.min`](https://docs.daft.ai/en/stable/api/functions/min/)
        """
        from daft.functions import min

        return min(self)

    def max(self) -> Expression:
        """Calculates the maximum value in the expression.

        Tip: See Also
            [`daft.functions.max`](https://docs.daft.ai/en/stable/api/functions/max/)
        """
        from daft.functions import max

        return max(self)

    def bool_and(self) -> Expression:
        """Calculates the boolean AND of all values in a list.

        Tip: See Also
            [`daft.functions.bool_and`](https://docs.daft.ai/en/stable/api/functions/bool_and/)
        """
        from daft.functions import bool_and

        return bool_and(self)

    def bool_or(self) -> Expression:
        """Calculates the boolean OR of all values in a list.

        Tip: See Also
            [`daft.functions.bool_or`](https://docs.daft.ai/en/stable/api/functions/bool_or/)
        """
        from daft.functions import bool_or

        return bool_or(self)

    def any_value(self, ignore_nulls: bool = False) -> Expression:
        """Returns any value in the expression.

        Tip: See Also
            [`daft.functions.any_value`](https://docs.daft.ai/en/stable/api/functions/any_value/)
        """
        from daft.functions import any_value

        return any_value(self, ignore_nulls=ignore_nulls)

    def skew(self) -> Expression:
        """Calculates the skewness of the values from the expression.

        Tip: See Also
            [`daft.functions.skew`](https://docs.daft.ai/en/stable/api/functions/skew/)
        """
        from daft.functions import skew

        return skew(self)

    def list_agg(self) -> Expression:
        """Aggregates the values in the expression into a list.

        Tip: See Also
            [`daft.functions.list_agg`](https://docs.daft.ai/en/stable/api/functions/list_agg/)
        """
        from daft.functions import list_agg

        return list_agg(self)

    def list_agg_distinct(self) -> Expression:
        """Aggregates the values in the expression into a list of distinct values (ignoring nulls).

        Tip: See Also
            [`daft.functions.list_agg_distinct`](https://docs.daft.ai/en/stable/api/functions/list_agg_distinct/)
        """
        from daft.functions import list_agg_distinct

        return list_agg_distinct(self)

    def string_agg(self) -> Expression:
        """Aggregates the values in the expression into a single string by concatenating them.

        Tip: See Also
            [`daft.functions.string_agg`](https://docs.daft.ai/en/stable/api/functions/string_agg/)
        """
        from daft.functions import string_agg

        return string_agg(self)

    def _explode(self) -> Expression:
        f = native.get_function_from_registry("explode")
        return Expression._from_pyexpr(f(self._expr))

    def apply(self, func: Callable[..., Any], return_dtype: DataTypeLike) -> Expression:
        """Apply a function on each value in a given expression.

        Args:
            func: Function to run per value of the expression
            return_dtype: Return datatype of the function that was ran

        Returns:
            Expression: New expression after having run the function on the expression

        Note:
            This is just syntactic sugar on top of a UDF and is convenient to use when your function only operates
            on a single column, and does not benefit from executing on batches. For either of those other use-cases,
            use a UDF instead.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["1", "2", "tim"]})
            >>> def f(x_val: str) -> int:
            ...     if x_val.isnumeric():
            ...         return int(x_val)
            ...     else:
            ...         return 0
            >>> df.with_column("num_x", df["x"].apply(f, return_dtype=daft.DataType.int64())).collect()
            ╭────────┬───────╮
            │ x      ┆ num_x │
            │ ---    ┆ ---   │
            │ String ┆ Int64 │
            ╞════════╪═══════╡
            │ 1      ┆ 1     │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2      ┆ 2     │
            ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ tim    ┆ 0     │
            ╰────────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        from daft.udf import UDF

        inferred_return_dtype = DataType._infer(return_dtype)

        def batch_func(self_series: Series) -> list[Any]:
            return [func(x) for x in self_series]

        name = getattr(func, "__module__", "")
        if name:
            name = name + "."
        if hasattr(func, "__qualname__"):
            name = name + getattr(func, "__qualname__")
        elif hasattr(func, "__class__"):
            name = name + func.__class__.__name__
        else:
            name = name + func.__name__

        return UDF(
            inner=batch_func,
            name=name,
            return_dtype=inferred_return_dtype,
        )(self)

    def is_null(self) -> Expression:
        """Checks if values in the Expression are Null (a special value indicating missing data).

        Tip: See Also
            [`daft.functions.is_null`](https://docs.daft.ai/en/stable/api/functions/is_null/)
        """
        from daft.functions import is_null

        return is_null(self)

    def not_null(self) -> Expression:
        """Checks if values in the Expression are not Null (a special value indicating missing data).

        Tip: See Also
            [`daft.functions.not_null`](https://docs.daft.ai/en/stable/api/functions/not_null/)
        """
        from daft.functions import not_null

        return not_null(self)

    def fill_null(self, fill_value: Expression | Any) -> Expression:
        """Fills null values in the Expression with the provided fill_value.

        Tip: See Also
            [`daft.functions.fill_null`](https://docs.daft.ai/en/stable/api/functions/fill_null/)
        """
        from daft.functions import fill_null

        return fill_null(self, fill_value)

    def is_in(self, other: Any) -> Expression:
        """Checks if values in the Expression are in the provided list.

        Tip: See Also
            [`daft.functions.is_in`](https://docs.daft.ai/en/stable/api/functions/is_in/)
        """
        from daft.functions import is_in

        return is_in(self, other)

    def between(self, lower: int | builtins.float, upper: int | builtins.float) -> Expression:
        """Checks if values in the Expression are between lower and upper, inclusive.

        Tip: See Also
            [`daft.functions.between`](https://docs.daft.ai/en/stable/api/functions/between/)
        """
        from daft.functions import between

        return between(self, lower, upper)

    def hash(
        self,
        seed: Any | None = None,
        hash_function: Literal["xxhash", "xxhash32", "xxhash64", "xxhash3_64", "murmurhash3", "sha1"] | None = "xxhash",
    ) -> Expression:
        """Hashes the values in the Expression.

        Tip: See Also
            [`daft.functions.hash`](https://docs.daft.ai/en/stable/api/functions/hash/): use the function for hashing multiple columns together.
        """
        from daft.functions import hash

        return hash(self, seed=seed, hash_function=hash_function)

    def minhash(
        self,
        *,
        num_hashes: int,
        ngram_size: int,
        seed: int = 1,
        hash_function: Literal["murmurhash3", "xxhash", "xxhash32", "xxhash64", "xxhash3_64", "sha1"] = "murmurhash3",
    ) -> Expression:
        """Runs the MinHash algorithm on the series.

        Tip: See Also
            [`daft.functions.minhash`](https://docs.daft.ai/en/stable/api/functions/minhash/)
        """
        from daft.functions import minhash

        return minhash(self, num_hashes=num_hashes, ngram_size=ngram_size, seed=seed, hash_function=hash_function)

    def encode(self, charset: ENCODING_CHARSET) -> Expression:
        """Encode binary or string values using the specified character set.

        Tip: See Also
            [`daft.functions.encode`](https://docs.daft.ai/en/stable/api/functions/encode/)
        """
        from daft.functions import encode

        return encode(self, charset=charset)

    def decode(self, charset: ENCODING_CHARSET) -> Expression:
        """Decodes binary values using the specified character set.

        Tip: See Also
            [`daft.functions.decode`](https://docs.daft.ai/en/stable/api/functions/decode/)
        """
        from daft.functions import decode

        return decode(self, charset=charset)

    def try_encode(self, charset: ENCODING_CHARSET) -> Expression:
        """Encode or null if unsuccessful.

        Tip: See Also
            [`daft.functions.try_encode`](https://docs.daft.ai/en/stable/api/functions/try_encode/)
        """
        from daft.functions import try_encode

        return try_encode(self, charset=charset)

    def try_decode(self, charset: ENCODING_CHARSET) -> Expression:
        """Decode or null if unsuccessful.

        Tip: See Also
            [`daft.functions.try_decode`](https://docs.daft.ai/en/stable/api/functions/try_decode/)
        """
        from daft.functions import try_decode

        return try_decode(self, charset=charset)

    def compress(self, codec: COMPRESSION_CODEC) -> Expression:
        """Compress binary or string values using the specified codec.

        Tip: See Also
            [`daft.functions.compress`](https://docs.daft.ai/en/stable/api/functions/compress/)
        """
        from daft.functions import compress

        return compress(self, codec=codec)

    def decompress(self, codec: COMPRESSION_CODEC) -> Expression:
        """Decompress binary values using the specified codec.

        Tip: See Also
            [`daft.functions.decompress`](https://docs.daft.ai/en/stable/api/functions/decompress/)
        """
        from daft.functions import decompress

        return decompress(self, codec=codec)

    def try_compress(self, codec: COMPRESSION_CODEC) -> Expression:
        """Compress or null if unsuccessful.

        Tip: See Also
            [`daft.functions.try_compress`](https://docs.daft.ai/en/stable/api/functions/try_compress/)
        """
        from daft.functions import try_compress

        return try_compress(self, codec=codec)

    def try_decompress(self, codec: COMPRESSION_CODEC) -> Expression:
        """Decompress or null if unsuccessful.

        Tip: See Also
            [`daft.functions.try_decompress`](https://docs.daft.ai/en/stable/api/functions/try_decompress/)
        """
        from daft.functions import try_decompress

        return try_decompress(self, codec=codec)

    def deserialize(self, format: Literal["json"], dtype: DataTypeLike) -> Expression:
        """Deserializes the expression (string) using the specified format and data type.

        Tip: See Also
            [`daft.functions.deserialize`](https://docs.daft.ai/en/stable/api/functions/deserialize/)
        """
        from daft.functions import deserialize

        return deserialize(self, format=format, dtype=dtype)

    def try_deserialize(self, format: Literal["json"], dtype: DataTypeLike) -> Expression:
        """Deserializes the expression (string) using the specified format and data type, inserting nulls on failures.

        Tip: See Also
            [`daft.functions.try_deserialize`](https://docs.daft.ai/en/stable/api/functions/try_deserialize/)
        """
        from daft.functions import try_deserialize

        return try_deserialize(self, format=format, dtype=dtype)

    def serialize(self, format: Literal["json"]) -> Expression:
        """Serializes the expression as a string using the specified format.

        Tip: See Also
            [`daft.functions.serialize`](https://docs.daft.ai/en/stable/api/functions/serialize/)
        """
        from daft.functions import serialize

        return serialize(self, format=format)

    def jq(self, filter: builtins.str) -> Expression:
        """Applies a [jq](https://jqlang.github.io/jq/manual/) filter to the expression (string), returning the results as a string.

        Tip: See Also
            [`daft.functions.jq`](https://docs.daft.ai/en/stable/api/functions/jq/)
        """
        from daft.functions import jq

        return jq(self, filter)

    def name(self) -> builtins.str:
        return self._expr.name()

    def over(self, window: Window) -> Expression:
        """Apply the expression as a window function.

        Tip: See Also
            [`daft.functions.over`](https://docs.daft.ai/en/stable/api/functions/over/)
        """
        from daft.functions import over

        return over(self, window)

    def lag(self, offset: int = 1, default: Any | None = None) -> Expression:
        """Get the value from a previous row within a window partition.

        Tip: See Also
              [`daft.functions.lag`](https://docs.daft.ai/en/stable/api/functions/lag/)
        """
        from daft.functions import lag

        return lag(self, offset=offset, default=default)

    def lead(self, offset: int = 1, default: Any | None = None) -> Expression:
        """Get the value from a future row within a window partition.

        Tip: See Also
              [`daft.functions.lead`](https://docs.daft.ai/en/stable/api/functions/lead/)
        """
        from daft.functions import lead

        return lead(self, offset=offset, default=default)

    def __repr__(self) -> builtins.str:
        return repr(self._expr)

    def _to_sql(self) -> builtins.str | None:
        return self._expr.to_sql()

    def _to_field(self, schema: Schema) -> Field:
        return Field._from_pyfield(self._expr.to_field(schema._schema))

    def __hash__(self) -> int:
        return self._expr.__hash__()

    def __reduce__(self) -> tuple[Callable[[_PyExpr], Expression], tuple[_PyExpr]]:
        return Expression._from_pyexpr, (self._expr,)

    def _input_mapping(self) -> builtins.str | None:
        return self._expr._input_mapping()

    def _initialize_udfs(self) -> Expression:
        return Expression._from_pyexpr(initialize_udfs(self._expr))

    def parse_url(self) -> Expression:
        """Parse string URLs and extract URL components.

        Tip: See Also
            [`daft.functions.parse_url`](https://docs.daft.ai/en/stable/api/functions/parse_url/)
        """
        from daft.functions import parse_url

        return parse_url(self)

    def explode(self) -> Expression:
        """Explode a list expression.

        Tip: See Also
            [`daft.functions.explode`](https://docs.daft.ai/en/stable/api/functions/explode/)
        """
        from daft.functions import explode

        return explode(self)

    def cosine_distance(self, other: Expression) -> Expression:
        """Compute the cosine distance between two embeddings.

        Tip: See Also
            [`daft.functions.cosine_distance`](https://docs.daft.ai/en/stable/api/functions/cosine_distance/)
        """
        from daft.functions import cosine_distance

        return cosine_distance(self, other)

    def length(self) -> Expression:
        """Retrieves the length of the given expression.

        Tip: See Also
            [`daft.functions.length`](https://docs.daft.ai/en/stable/api/functions/length/)
        """
        from daft.functions import length

        return length(self)

    def concat(self, other: Expression | builtins.str | bytes) -> Expression:
        """Concatenate two string expressions.

        Tip: See Also
            [`daft.functions.concat`](https://docs.daft.ai/en/stable/api/functions/concat/)
        """
        from daft.functions import concat

        return concat(self, other)

    def download(
        self,
        max_connections: int = 32,
        on_error: Literal["raise", "null"] = "raise",
        io_config: IOConfig | None = None,
    ) -> Expression:
        """Treats each string as a URL, and downloads the bytes contents as a bytes column.

        Tip: See Also
            [`daft.functions.download`](https://docs.daft.ai/en/stable/api/functions/download/)
        """
        from daft.functions import download

        return download(self, max_connections, on_error, io_config)

    def upload(
        self,
        location: builtins.str | Expression,
        max_connections: int = 32,
        on_error: Literal["raise", "null"] = "raise",
        io_config: IOConfig | None = None,
    ) -> Expression:
        """Uploads a column of binary data to the provided location(s) (also supports S3, local etc).

        Tip: See Also
            [`daft.functions.upload`](https://docs.daft.ai/en/stable/api/functions/upload/)
        """
        from daft.functions import upload

        return upload(self, location, max_connections, on_error, io_config)

    def date(self) -> Expression:
        """Retrieves the date for a datetime column."""
        from daft.functions import date

        return date(self)

    def day(self) -> Expression:
        """Retrieves the day for a datetime column.

        Tip: See Also
            [`daft.functions.day`](https://docs.daft.ai/en/stable/api/functions/day/)
        """
        from daft.functions import day

        return day(self)

    def hour(self) -> Expression:
        """Retrieves the hour for a datetime column.

        Tip: See Also
            [`daft.functions.hour`](https://docs.daft.ai/en/stable/api/functions/hour/)
        """
        from daft.functions import hour

        return hour(self)

    def minute(self) -> Expression:
        """Retrieves the minute for a datetime column.

        Tip: See Also
            [`daft.functions.minute`](https://docs.daft.ai/en/stable/api/functions/minute/)
        """
        from daft.functions import minute

        return minute(self)

    def second(self) -> Expression:
        """Retrieves the second for a datetime column.

        Tip: See Also
            [`daft.functions.second`](https://docs.daft.ai/en/stable/api/functions/second/)
        """
        from daft.functions import second

        return second(self)

    def millisecond(self) -> Expression:
        """Retrieves the millisecond for a datetime column.

        Tip: See Also
            [`daft.functions.millisecond`](https://docs.daft.ai/en/stable/api/functions/millisecond/)
        """
        from daft.functions import millisecond

        return millisecond(self)

    def microsecond(self) -> Expression:
        """Retrieves the microsecond for a datetime column.

        Tip: See Also
            [`daft.functions.microsecond`](https://docs.daft.ai/en/stable/api/functions/microsecond/)
        """
        from daft.functions import microsecond

        return microsecond(self)

    def nanosecond(self) -> Expression:
        """Retrieves the nanosecond for a datetime column.

        Tip: See Also
            [`daft.functions.nanosecond`](https://docs.daft.ai/en/stable/api/functions/nanosecond/)
        """
        from daft.functions import nanosecond

        return nanosecond(self)

    def unix_date(self) -> Expression:
        """Retrieves the number of days since 1970-01-01 00:00:00 UTC.

        Tip: See Also
            [`daft.functions.unix_date`](https://docs.daft.ai/en/stable/api/functions/unix_date/)
        """
        from daft.functions import unix_date

        return unix_date(self)

    def time(self) -> Expression:
        """Retrieves the time for a datetime column."""
        from daft.functions import time

        return time(self)

    def month(self) -> Expression:
        """Retrieves the month for a datetime column.

        Tip: See Also
            [`daft.functions.month`](https://docs.daft.ai/en/stable/api/functions/month/)
        """
        from daft.functions import month

        return month(self)

    def quarter(self) -> Expression:
        """Retrieves the quarter for a datetime column.

        Tip: See Also
            [`daft.functions.quarter`](https://docs.daft.ai/en/stable/api/functions/quarter/)
        """
        from daft.functions import quarter

        return quarter(self)

    def year(self) -> Expression:
        """Retrieves the year for a datetime column.

        Tip: See Also
            [`daft.functions.year`](https://docs.daft.ai/en/stable/api/functions/year/)
        """
        from daft.functions import year

        return year(self)

    def day_of_week(self) -> Expression:
        """Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday.

        Tip: See Also
            [`daft.functions.day_of_week`](https://docs.daft.ai/en/stable/api/functions/day_of_week/)
        """
        from daft.functions import day_of_week

        return day_of_week(self)

    def day_of_month(self) -> Expression:
        """Retrieves the day of the month for a datetime column.

        Tip: See Also
            [`daft.functions.day_of_month`](https://docs.daft.ai/en/stable/api/functions/day_of_month/)
        """
        from daft.functions import day_of_month

        return day_of_month(self)

    def day_of_year(self) -> Expression:
        """Retrieves the ordinal day for a datetime column. Starting at 1 for January 1st and ending at 365 or 366 for December 31st.

        Tip: See Also
            [`daft.functions.day_of_year`](https://docs.daft.ai/en/stable/api/functions/day_of_year/)
        """
        from daft.functions import day_of_year

        return day_of_year(self)

    def week_of_year(self) -> Expression:
        """Retrieves the week of the year for a datetime column.

        Tip: See Also
            [`daft.functions.week_of_year`](https://docs.daft.ai/en/stable/api/functions/week_of_year/)
        """
        from daft.functions import week_of_year

        return week_of_year(self)

    def strftime(self, format: builtins.str | None = None) -> Expression:
        """Converts a datetime/date column to a string column.

        Tip: See Also
            [`daft.functions.strftime`](https://docs.daft.ai/en/stable/api/functions/strftime/)
        """
        from daft.functions import strftime

        return strftime(self, format)

    def total_seconds(self) -> Expression:
        """Calculates the total number of seconds for a duration column.

        Tip: See Also
            [`daft.functions.total_seconds`](https://docs.daft.ai/en/stable/api/functions/total_seconds/)
        """
        from daft.functions import total_seconds

        return total_seconds(self)

    def total_milliseconds(self) -> Expression:
        """Calculates the total number of milliseconds for a duration column.

        Tip: See Also
            [`daft.functions.total_milliseconds`](https://docs.daft.ai/en/stable/api/functions/total_milliseconds/)
        """
        from daft.functions import total_milliseconds

        return total_milliseconds(self)

    def total_microseconds(self) -> Expression:
        """Calculates the total number of microseconds for a duration column.

        Tip: See Also
            [`daft.functions.total_microseconds`](https://docs.daft.ai/en/stable/api/functions/total_microseconds/)
        """
        from daft.functions import total_microseconds

        return total_microseconds(self)

    def total_nanoseconds(self) -> Expression:
        """Calculates the total number of nanoseconds for a duration column.

        Tip: See Also
            [`daft.functions.total_nanoseconds`](https://docs.daft.ai/en/stable/api/functions/total_nanoseconds/)
        """
        from daft.functions import total_nanoseconds

        return total_nanoseconds(self)

    def total_minutes(self) -> Expression:
        """Calculates the total number of minutes for a duration column.

        Tip: See Also
            [`daft.functions.total_minutes`](https://docs.daft.ai/en/stable/api/functions/total_minutes/)
        """
        from daft.functions import total_minutes

        return total_minutes(self)

    def total_hours(self) -> Expression:
        """Calculates the total number of hours for a duration column.

        Tip: See Also
            [`daft.functions.total_hours`](https://docs.daft.ai/en/stable/api/functions/total_hours/)
        """
        from daft.functions import total_hours

        return total_hours(self)

    def total_days(self) -> Expression:
        """Calculates the total number of days for a duration column.

        Tip: See Also
            [`daft.functions.total_days`](https://docs.daft.ai/en/stable/api/functions/total_days/)
        """
        from daft.functions import total_days

        return total_days(self)

    def to_date(self, format: builtins.str) -> Expression:
        """Converts a string to a date using the specified format.

        Tip: See Also
            [`daft.functions.to_date`](https://docs.daft.ai/en/stable/api/functions/to_date/)
        """
        from daft.functions import to_date

        return to_date(self, format)

    def to_datetime(self, format: builtins.str, timezone: builtins.str | None = None) -> Expression:
        """Converts a string to a datetime using the specified format and timezone.

        Tip: See Also
            [`daft.functions.to_datetime`](https://docs.daft.ai/en/stable/api/functions/to_datetime/)
        """
        from daft.functions import to_datetime

        return to_datetime(self, format, timezone)

    def contains(self, substr: builtins.str | Expression) -> Expression:
        """Checks whether each string contains the given pattern in a string column.

        Tip: See Also
            [`daft.functions.contains`](https://docs.daft.ai/en/stable/api/functions/contains/)
        """
        from daft.functions import contains

        return contains(self, substr)

    def split(self, split_on: builtins.str | Expression) -> Expression:
        """Splits each string on the given string, into a list of strings.

        Tip: See Also
            [`daft.functions.split`](https://docs.daft.ai/en/stable/api/functions/split/)
        """
        from daft.functions import split

        return split(self, split_on)

    def regexp_split(self, pattern: builtins.str | Expression) -> Expression:
        """Splits each string on the given regex pattern, into a list of strings.

        Tip: See Also
            [`daft.functions.regexp_split`](https://docs.daft.ai/en/stable/api/functions/regexp_split/)
        """
        from daft.functions import regexp_split

        return regexp_split(self, pattern)

    def lower(self) -> Expression:
        """Convert UTF-8 string to all lowercase.

        Tip: See Also
            [`daft.functions.lower`](https://docs.daft.ai/en/stable/api/functions/lower/)
        """
        from daft.functions import lower

        return lower(self)

    def upper(self) -> Expression:
        """Convert UTF-8 string to all upper.

        Tip: See Also
            [`daft.functions.upper`](https://docs.daft.ai/en/stable/api/functions/upper/)
        """
        from daft.functions import upper

        return upper(self)

    def lstrip(self) -> Expression:
        """Strip whitespace from the left side of a UTF-8 string.

        Tip: See Also
            [`daft.functions.lstrip`](https://docs.daft.ai/en/stable/api/functions/lstrip/)
        """
        from daft.functions import lstrip

        return lstrip(self)

    def rstrip(self) -> Expression:
        """Strip whitespace from the right side of a UTF-8 string.

        Tip: See Also
            [`daft.functions.rstrip`](https://docs.daft.ai/en/stable/api/functions/rstrip/)
        """
        from daft.functions import rstrip

        return rstrip(self)

    def reverse(self) -> Expression:
        """Reverse a UTF-8 string.

        Tip: See Also
            [`daft.functions.reverse`](https://docs.daft.ai/en/stable/api/functions/reverse/)
        """
        from daft.functions import reverse

        return reverse(self)

    def capitalize(self) -> Expression:
        """Capitalize a UTF-8 string.

        Tip: See Also
            [`daft.functions.capitalize`](https://docs.daft.ai/en/stable/api/functions/capitalize/)
        """
        from daft.functions import capitalize

        return capitalize(self)

    def left(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) left-most characters of each string.

        Tip: See Also
            [`daft.functions.left`](https://docs.daft.ai/en/stable/api/functions/left/)
        """
        from daft.functions import left

        return left(self, nchars)

    def right(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) right-most characters of each string.

        Tip: See Also
            [`daft.functions.right`](https://docs.daft.ai/en/stable/api/functions/right/)
        """
        from daft.functions import right

        return right(self, nchars)

    def rpad(self, length: int | Expression, pad: builtins.str | Expression) -> Expression:
        """Right-pads each string by truncating or padding with the character.

        Tip: See Also
            [`daft.functions.rpad`](https://docs.daft.ai/en/stable/api/functions/rpad/)
        """
        from daft.functions import rpad

        return rpad(self, length, pad)

    def lpad(self, length: int | Expression, pad: builtins.str | Expression) -> Expression:
        """Left-pads each string by truncating or padding with the character.

        Tip: See Also
            [`daft.functions.lpad`](https://docs.daft.ai/en/stable/api/functions/lpad/)
        """
        from daft.functions import lpad

        return lpad(self, length, pad)

    def repeat(self, n: int | Expression) -> Expression:
        """Repeats each string n times.

        Tip: See Also
            [`daft.functions.repeat`](https://docs.daft.ai/en/stable/api/functions/repeat/)
        """
        from daft.functions import repeat

        return repeat(self, n)

    def like(self, pattern: builtins.str | Expression) -> Expression:
        """Checks whether each string matches the given SQL LIKE pattern, case sensitive.

        Tip: See Also
            [`daft.functions.like`](https://docs.daft.ai/en/stable/api/functions/like/)
        """
        from daft.functions import like

        return like(self, pattern)

    def ilike(self, pattern: builtins.str | Expression) -> Expression:
        """Checks whether each string matches the given SQL ILIKE pattern, case insensitive.

        Tip: See Also
            [`daft.functions.ilike`](https://docs.daft.ai/en/stable/api/functions/ilike/)
        """
        from daft.functions import ilike

        return ilike(self, pattern)

    def substr(self, start: int | Expression, length: int | Expression | None = None) -> Expression:
        """Extract a substring from a string, starting at a specified index and extending for a given length.

        Tip: See Also
            [`daft.functions.substr`](https://docs.daft.ai/en/stable/api/functions/substr/)
        """
        from daft.functions import substr

        return substr(self, start, length)

    def endswith(self, suffix: builtins.str | Expression) -> Expression:
        """Checks whether each string ends with the given pattern in a string column.

        Tip: See Also
            [`daft.functions.endswith`](https://docs.daft.ai/en/stable/api/functions/endswith/)
        """
        from daft.functions import endswith

        return endswith(self, suffix)

    def startswith(self, prefix: builtins.str | Expression) -> Expression:
        """Checks whether each string starts with the given pattern in a string column.

        Tip: See Also
            [`daft.functions.startswith`](https://docs.daft.ai/en/stable/api/functions/startswith/)
        """
        from daft.functions import startswith

        return startswith(self, prefix)

    def normalize(
        self,
        *,
        remove_punct: bool = False,
        lowercase: bool = False,
        nfd_unicode: bool = False,
        white_space: bool = False,
    ) -> Expression:
        """Normalizes a string for more useful deduplication.

        Tip: See Also
            [`daft.functions.normalize`](https://docs.daft.ai/en/stable/api/functions/normalize/)
        """
        from daft.functions import normalize

        return normalize(
            self, remove_punct=remove_punct, lowercase=lowercase, nfd_unicode=nfd_unicode, white_space=white_space
        )

    def tokenize_encode(
        self,
        tokens_path: builtins.str,
        *,
        io_config: IOConfig | None = None,
        pattern: builtins.str | None = None,
        special_tokens: builtins.str | None = None,
        use_special_tokens: bool | None = None,
    ) -> Expression:
        """Encodes each string as a list of integer tokens using a tokenizer.

        Tip: See Also
            [`daft.functions.tokenize_encode`](https://docs.daft.ai/en/stable/api/functions/tokenize_encode/)
        """
        from daft.functions import tokenize_encode

        return tokenize_encode(
            self,
            tokens_path,
            io_config=io_config,
            pattern=pattern,
            special_tokens=special_tokens,
            use_special_tokens=use_special_tokens,
        )

    def tokenize_decode(
        self,
        tokens_path: builtins.str,
        *,
        io_config: IOConfig | None = None,
        pattern: builtins.str | None = None,
        special_tokens: builtins.str | None = None,
    ) -> Expression:
        """Decodes each list of integer tokens into a string using a tokenizer.

        Tip: See Also
            [`daft.functions.tokenize_decode`](https://docs.daft.ai/en/stable/api/functions/tokenize_decode/)
        """
        from daft.functions import tokenize_decode

        return tokenize_decode(
            self,
            tokens_path,
            io_config=io_config,
            pattern=pattern,
            special_tokens=special_tokens,
        )

    def count_matches(
        self,
        patterns: Any,
        *,
        whole_words: bool = False,
        case_sensitive: bool = True,
    ) -> Expression:
        """Counts the number of times a pattern, or multiple patterns, appear in a string.

        Tip: See Also
            [`daft.functions.count_matches`](https://docs.daft.ai/en/stable/api/functions/count_matches/)
        """
        from daft.functions import count_matches

        return count_matches(self, patterns, whole_words=whole_words, case_sensitive=case_sensitive)

    def regexp_count(
        self,
        pattern: builtins.str | Expression,
    ) -> Expression:
        """Counts the number of times a regex pattern appears in a string.

        Tip: See Also
            [`daft.functions.regexp_count`](https://docs.daft.ai/en/stable/api/functions/regexp_count/)
        """
        from daft.functions import regexp_count

        return regexp_count(self, pattern)

    def length_bytes(self) -> Expression:
        """Retrieves the length for a UTF-8 string column in bytes.

        Tip: See Also
            [`daft.functions.length_bytes`](https://docs.daft.ai/en/stable/api/functions/length_bytes/)
        """
        from daft.functions import length_bytes

        return length_bytes(self)

    def value_counts(self) -> Expression:
        """Counts the occurrences of each distinct value in the list.

        Tip: See Also
            [`daft.functions.value_counts`](https://docs.daft.ai/en/stable/api/functions/value_counts/)
        """
        from daft.functions import value_counts

        return value_counts(self)

    def chunk(self, size: int) -> Expression:
        """Splits each list into chunks of the given size.

        Tip: See Also
            [`daft.functions.chunk`](https://docs.daft.ai/en/stable/api/functions/chunk/)
        """
        from daft.functions import chunk

        return chunk(self, size)

    def resize(self, w: int, h: int) -> Expression:
        """Resize image into the provided width and height.

        Tip: See Also
            [`daft.functions.resize`](https://docs.daft.ai/en/stable/api/functions/resize/)
        """
        from daft.functions import resize

        return resize(self, w, h)

    def crop(self, bbox: tuple[int, int, int, int] | Expression) -> Expression:
        """Crops images with the provided bounding box.

        Tip: See Also
            [`daft.functions.crop`](https://docs.daft.ai/en/stable/api/functions/crop/)
        """
        from daft.functions import crop

        return crop(self, bbox)

    def list_join(self, delimiter: builtins.str | Expression) -> Expression:
        """Joins every element of a list using the specified string delimiter.

        Tip: See Also
            [`daft.functions.list_join`](https://docs.daft.ai/en/stable/api/functions/list_join/)
        """
        from daft.functions import list_join

        return list_join(self, delimiter)

    def list_count(self, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid) -> Expression:
        """Counts the number of elements in each list.

        Tip: See Also
            [`daft.functions.list_count`](https://docs.daft.ai/en/stable/api/functions/list_count/)
        """
        from daft.functions import list_count

        return list_count(self, mode)

    def list_sum(self) -> Expression:
        """Sums each list. Empty lists and lists with all nulls yield null.

        Tip: See Also
            [`daft.functions.list_sum`](https://docs.daft.ai/en/stable/api/functions/list_sum/)
        """
        from daft.functions import list_sum

        return list_sum(self)

    def list_mean(self) -> Expression:
        """Calculates the mean of each list. If no non-null values in a list, the result is null.

        Tip: See Also
            [`daft.functions.list_mean`](https://docs.daft.ai/en/stable/api/functions/list_mean/)
        """
        from daft.functions import list_mean

        return list_mean(self)

    def list_min(self) -> Expression:
        """Calculates the minimum of each list. If no non-null values in a list, the result is null.

        Tip: See Also
            [`daft.functions.list_min`](https://docs.daft.ai/en/stable/api/functions/list_min/)
        """
        from daft.functions import list_min

        return list_min(self)

    def list_max(self) -> Expression:
        """Calculates the maximum of each list. If no non-null values in a list, the result is null.

        Tip: See Also
            [`daft.functions.list_max`](https://docs.daft.ai/en/stable/api/functions/list_max/)
        """
        from daft.functions import list_max

        return list_max(self)

    def list_bool_and(self) -> Expression:
        """Calculates the boolean AND of all values in a list.

        Tip: See Also
            [`daft.functions.list_bool_and`](https://docs.daft.ai/en/stable/api/functions/list_bool_and/)
        """
        from daft.functions import list_bool_and

        return list_bool_and(self)

    def list_bool_or(self) -> Expression:
        """Calculates the boolean OR of all values in a list.

        Tip: See Also
            [`daft.functions.list_bool_or`](https://docs.daft.ai/en/stable/api/functions/list_bool_or/)
        """
        from daft.functions import list_bool_or

        return list_bool_or(self)

    def list_sort(
        self, desc: bool | Expression | None = None, nulls_first: bool | Expression | None = None
    ) -> Expression:
        """Sorts the inner lists of a list column.

        Tip: See Also
            [`daft.functions.list_sort`](https://docs.daft.ai/en/stable/api/functions/list_sort/)
        """
        from daft.functions import list_sort

        return list_sort(self, desc, nulls_first)

    def list_distinct(self) -> Expression:
        """Returns a list of unique elements in each list, preserving order of first occurrence and ignoring nulls.

        Tip: See Also
            [`daft.functions.list_distinct`](https://docs.daft.ai/en/stable/api/functions/list_distinct/)
        """
        from daft.functions import list_distinct

        return list_distinct(self)

    def list_map(self, mapper: Expression) -> Expression:
        """Evaluates an expression on all elements in the list.

        Tip: See Also
            [`daft.functions.list_map`](https://docs.daft.ai/en/stable/api/functions/list_map/)
        """
        from daft.functions import list_map

        return list_map(self, mapper)

    def encode_image(self, image_format: builtins.str | ImageFormat) -> Expression:
        """Encode an image column as the provided image file format, returning a binary column of encoded bytes.

        Tip: See Also
            [`daft.functions.encode_image`](https://docs.daft.ai/en/stable/api/functions/encode_image/)
        """
        from daft.functions import encode_image

        return encode_image(self, image_format)

    def decode_image(
        self,
        on_error: Literal["raise", "null"] = "raise",
        mode: builtins.str | ImageMode | None = ImageMode.RGB,
    ) -> Expression:
        """Decodes the binary data in this column into images.

        Tip: See Also
            [`daft.functions.decode_image`](https://docs.daft.ai/en/stable/api/functions/decode_image/)
        """
        from daft.functions import decode_image

        return decode_image(self, on_error=on_error, mode=mode)

    def coalesce(self, *others: Expression) -> Expression:
        """Returns the first non-null value among this expression and the provided expressions.

        Tip: See Also
            [`daft.functions.coalesce`](https://docs.daft.ai/en/stable/api/functions/coalesce/)
        """
        from daft.functions import coalesce

        return coalesce(self, *others)

    def date_trunc(self, interval: builtins.str, relative_to: Expression | None = None) -> Expression:
        """Truncates the datetime column to the specified interval.

        Tip: See Also
            [`daft.functions.date_trunc`](https://docs.daft.ai/en/stable/api/functions/date_trunc/)
        """
        from daft.functions import date_trunc

        return date_trunc(interval, self, relative_to=relative_to)

    def regexp(self, pattern: builtins.str | Expression) -> Expression:
        """Check whether each string matches the given regular expression pattern in a string column.

        Tip: See Also
            [`daft.functions.regexp`](https://docs.daft.ai/en/stable/api/functions/regexp/)
        """
        from daft.functions import regexp

        return regexp(self, pattern)

    def regexp_extract(self, pattern: builtins.str | Expression, index: int = 0) -> Expression:
        """Extracts the specified match group from the first regex match in each string in a string column.

        Tip: See Also
            [`daft.functions.regexp_extract`](https://docs.daft.ai/en/stable/api/functions/regexp_extract/)
        """
        from daft.functions import regexp_extract

        return regexp_extract(self, pattern, index=index)

    def regexp_extract_all(self, pattern: builtins.str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from all regex matches in each string in a string column.

        Tip: See Also
            [`daft.functions.regexp_extract_all`](https://docs.daft.ai/en/stable/api/functions/regexp_extract_all/)
        """
        from daft.functions import regexp_extract_all

        return regexp_extract_all(self, pattern, index=index)

    def replace(
        self,
        search: builtins.str | Expression,
        replacement: builtins.str | Expression,
    ) -> Expression:
        """Replaces all occurrences of a substring in a string with a replacement string.

        Tip: See Also
            [`daft.functions.replace`](https://docs.daft.ai/en/stable/api/functions/replace/)
        """
        from daft.functions import replace

        return replace(self, search, replacement)

    def regexp_replace(
        self,
        pattern: builtins.str | Expression,
        replacement: builtins.str | Expression,
    ) -> Expression:
        """Replaces all occurrences of a regex pattern in a string column with a replacement string.

        Tip: See Also
            [`daft.functions.regexp_replace`](https://docs.daft.ai/en/stable/api/functions/regexp_replace/)
        """
        from daft.functions import regexp_replace

        return regexp_replace(self, pattern, replacement)

    def find(self, substr: builtins.str | Expression) -> Expression:
        """Returns the index of the first occurrence of the substring in each string.

        Tip: See Also
            [`daft.functions.find`](https://docs.daft.ai/en/stable/api/functions/find/)
        """
        from daft.functions import find

        return find(self, substr)

    def convert_image(self, mode: builtins.str | ImageMode) -> Expression:
        """Convert an image expression to the specified mode.

        Tip: See Also
            [`daft.functions.convert_image`](https://docs.daft.ai/en/stable/api/functions/convert_image/)
        """
        from daft.functions import convert_image

        return convert_image(self, mode)

    def list_append(self, other: Expression) -> Expression:
        """Appends a value to each list in the column.

        Tip: See Also
            [`daft.functions.list_append`](https://docs.daft.ai/en/stable/api/functions/list_append/)
        """
        from daft.functions import list_append

        return list_append(self, other)

    def get(self, index: int | builtins.str | Expression, default: Any = None) -> Expression:
        """Get an index from a list expression or a field from a struct expression.

        Tip: See Also
            [`daft.functions.get`](https://docs.daft.ai/en/stable/api/functions/get/)
        """
        from daft.functions import get

        return get(self, index, default)

    def map_get(self, key: Expression) -> Expression:
        """Retrieves the value for a key in a map column.

        Tip: See Also
            [`daft.functions.map_get`](https://docs.daft.ai/en/stable/api/functions/map_get/)
        """
        from daft.functions import map_get

        return map_get(self, key)

    def slice(self, start: int | Expression, end: int | Expression | None = None) -> Expression:
        """Get a subset of each list or binary value.

        Tip: See Also
            [`daft.functions.slice`](https://docs.daft.ai/en/stable/api/functions/slice/)
        """
        from daft.functions import slice

        return slice(self, start, end)

    def to_unix_epoch(self, time_unit: builtins.str | TimeUnit | None = None) -> Expression:
        """Converts a datetime column to a Unix timestamp with the specified time unit. (default: seconds).

        Tip: See Also
            [`daft.functions.to_unix_epoch`](https://docs.daft.ai/en/stable/api/functions/to_unix_epoch/)
        """
        from daft.functions import to_unix_epoch

        return to_unix_epoch(self, time_unit=time_unit)

    def partition_days(self) -> Expression:
        """Partitioning Transform that returns the number of days since epoch (1970-01-01).

        Tip: See Also
            [`daft.functions.partition_days`](https://docs.daft.ai/en/stable/api/functions/partition_days/)
        """
        from daft.functions import partition_days

        return partition_days(self)

    def partition_hours(self) -> Expression:
        """Partitioning Transform that returns the number of hours since epoch (1970-01-01).

        Tip: See Also
            [`daft.functions.partition_hours`](https://docs.daft.ai/en/stable/api/functions/partition_hours/)
        """
        from daft.functions import partition_hours

        return partition_hours(self)

    def partition_months(self) -> Expression:
        """Partitioning Transform that returns the number of months since epoch (1970-01-01).

        Tip: See Also
            [`daft.functions.partition_months`](https://docs.daft.ai/en/stable/api/functions/partition_months/)
        """
        from daft.functions import partition_months

        return partition_months(self)

    def partition_years(self) -> Expression:
        """Partitioning Transform that returns the number of years since epoch (1970-01-01).

        Tip: See Also
            [`daft.functions.partition_years`](https://docs.daft.ai/en/stable/api/functions/partition_years/)
        """
        from daft.functions import partition_years

        return partition_years(self)

    def partition_iceberg_bucket(self, n: int) -> Expression:
        """Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86.

        Tip: See Also
            [`daft.functions.partition_iceberg_bucket`](https://docs.daft.ai/en/stable/api/functions/partition_iceberg_bucket/)
        """
        from daft.functions import partition_iceberg_bucket

        return partition_iceberg_bucket(self, n)

    def partition_iceberg_truncate(self, w: int) -> Expression:
        """Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.

        Tip: See Also
            [`daft.functions.partition_iceberg_truncate`](https://docs.daft.ai/en/stable/api/functions/partition_iceberg_truncate/)
        """
        from daft.functions import partition_iceberg_truncate

        return partition_iceberg_truncate(self, w)

    def is_nan(self) -> Expression:
        """Checks if values are NaN (a special float value indicating not-a-number).

        Tip: See Also
            [`daft.functions.is_nan`](https://docs.daft.ai/en/stable/api/functions/is_nan/)
        """
        from daft.functions import is_nan

        return is_nan(self)

    def is_inf(self) -> Expression:
        """Checks if values in the Expression are Infinity.

        Tip: See Also
            [`daft.functions.is_inf`](https://docs.daft.ai/en/stable/api/functions/is_inf/)
        """
        from daft.functions import is_inf

        return is_inf(self)

    def not_nan(self) -> Expression:
        """Checks if values are not NaN (a special float value indicating not-a-number).

        Tip: See Also
            [`daft.functions.not_nan`](https://docs.daft.ai/en/stable/api/functions/not_nan/)
        """
        from daft.functions import not_nan

        return not_nan(self)

    def fill_nan(self, fill_value: Expression) -> Expression:
        """Fills NaN values in the Expression with the provided fill_value.

        Tip: See Also
            [`daft.functions.fill_nan`](https://docs.daft.ai/en/stable/api/functions/fill_nan/)
        """
        from daft.functions import fill_nan

        return fill_nan(self, fill_value)

    def image_attribute(self, name: Literal["width", "height", "channel", "mode"] | ImageProperty) -> Expression:
        """Get a property of the image, such as 'width', 'height', 'channel', or 'mode'.

        Tip: See Also
            [`daft.functions.image_attribute`](https://docs.daft.ai/en/stable/api/functions/image_attribute/)
        """
        from daft.functions import image_attribute

        return image_attribute(self, name)

    def image_width(self) -> Expression:
        """Gets the width of an image in pixels.

        Tip: See Also
            [`daft.functions.image_width`](https://docs.daft.ai/en/stable/api/functions/image_width/)
        """
        from daft.functions import image_width

        return image_width(self)

    def image_height(self) -> Expression:
        """Gets the height of an image in pixels.

        Tip: See Also
            [`daft.functions.image_height`](https://docs.daft.ai/en/stable/api/functions/image_height/)
        """
        from daft.functions import image_height

        return image_height(self)

    def image_channel(self) -> Expression:
        """Gets the number of channels in an image.

        Tip: See Also
            [`daft.functions.image_channel`](https://docs.daft.ai/en/stable/api/functions/image_channel/)
        """
        from daft.functions import image_channel

        return image_channel(self)

    def image_mode(self) -> Expression:
        """Gets the mode of an image as a string.

        Tip: See Also
            [`daft.functions.image_mode`](https://docs.daft.ai/en/stable/api/functions/image_mode/)
        """
        from daft.functions import image_mode

        return image_mode(self)

    def file_size(self) -> Expression:
        """Gets the size of a file in bytes.

        Tip: See Also
            [`daft.functions.file_size`](https://docs.daft.ai/en/stable/api/functions/file_size/)
        """
        from daft.functions import file_size

        return file_size(self)


class WhenExpr(Expression):
    """Helper class for building a SQL-style CASE WHEN expression.

    Tip: See Also
        [`daft.functions.when`](https://docs.daft.ai/en/stable/api/functions/when/)
    """

    def __init__(self, cases: list[tuple[_PyExpr, _PyExpr]]):
        """This constructor should not be called directly. Please use `daft.functions.when` instead."""
        self._cases = cases
        self._expr = self._construct_pyexpr(self._cases, _lit(None))

    @staticmethod
    def _construct_pyexpr(cases: list[tuple[_PyExpr, _PyExpr]], otherwise: _PyExpr) -> _PyExpr:
        expr = otherwise
        for condition, then in reversed(cases):
            expr = condition.if_else(then, expr)
        return expr

    def when(self, condition: Expression | bool, then: Expression | Any) -> WhenExpr:
        """Adds a WHEN ... THEN ... clause to the CASE expression.

        Args:
            condition: The Boolean expression to evaluate
            then: The value to return if the condition is true

        Returns:
            A new WhenExpr with the added condition and value.

        Tip: See Also
            [`daft.functions.when`](https://docs.daft.ai/en/stable/api/functions/when/)
        """
        condition = Expression._to_expression(condition)
        then = Expression._to_expression(then)
        new_cases = self._cases + [(condition._expr, then._expr)]
        return WhenExpr(new_cases)

    def otherwise(self, value: Expression | Any) -> Expression:
        """Adds an ELSE ... clause to the CASE expression and returns a complete Expression.

        Tip: See Also
            [`daft.functions.when`](https://docs.daft.ai/en/stable/api/functions/when/)
        """
        value = Expression._to_expression(value)
        return Expression._from_pyexpr(self._construct_pyexpr(self._cases, value._expr))


class ExpressionsProjection(Iterable[Expression]):
    """A collection of Expressions that can be projected onto a Table to produce another Table.

    Invariants:
        1. All Expressions have names
        2. All Expressions have unique names
    """

    def __init__(self, exprs: list[Expression]) -> None:
        # Check invariants
        seen: set[str] = set()
        for e in exprs:
            if e.name() in seen:
                raise ValueError(f"Expressions must all have unique names; saw {e.name()} twice")
            seen.add(e.name())

        self._output_name_to_exprs = {e.name(): e for e in exprs}

    @classmethod
    def from_schema(cls, schema: Schema) -> ExpressionsProjection:
        return cls([col(field.name) for field in schema])

    def __len__(self) -> int:
        return len(self._output_name_to_exprs)

    def __iter__(self) -> Iterator[Expression]:
        return iter(self._output_name_to_exprs.values())

    @overload
    def __getitem__(self, idx: slice) -> list[Expression]: ...

    @overload
    def __getitem__(self, idx: int) -> Expression: ...

    def __getitem__(self, idx: int | slice) -> Expression | list[Expression]:
        # Relies on the fact that Python dictionaries are ordered
        return list(self._output_name_to_exprs.values())[idx]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExpressionsProjection):
            return False

        return len(self._output_name_to_exprs) == len(other._output_name_to_exprs) and all(
            (s.name() == o.name()) and expr_structurally_equal(s, o)
            for s, o in zip(
                self._output_name_to_exprs.values(),
                other._output_name_to_exprs.values(),
            )
        )

    def union(self, other: ExpressionsProjection, rename_dup: str | None = None) -> ExpressionsProjection:
        """Unions two Expressions. Output naming conflicts are handled with keyword arguments.

        Args:
            other (ExpressionsProjection): other ExpressionsProjection to union with this one
            rename_dup (Optional[str], optional): when conflicts in naming happen, append this string to the conflicting column in `other`. Defaults to None.
        """
        unioned: dict[str, Expression] = {}
        for expr in list(self) + list(other):
            name = expr.name()

            # Handle naming conflicts
            if name in unioned:
                if rename_dup is not None:
                    while name in unioned:
                        name = f"{rename_dup}{name}"
                    expr = expr.alias(name)
                else:
                    raise ValueError(
                        f"Duplicate name found with different expression. name: {name}, seen: {unioned[name]}, current: {expr}"
                    )

            unioned[name] = expr
        return ExpressionsProjection(list(unioned.values()))

    def to_name_set(self) -> set[str]:
        return {e.name() for e in self}

    def input_mapping(self) -> dict[str, str]:
        """Returns a map of {output_name: input_name} for all expressions that are just no-ops/aliases of an existing input."""
        result = {}
        for e in self:
            input_map = e._input_mapping()
            if input_map is not None:
                result[e.name()] = input_map
        return result

    def to_column_expressions(self) -> ExpressionsProjection:
        return ExpressionsProjection([col(e.name()) for e in self])

    def get_expression_by_name(self, name: str) -> Expression:
        if name not in self._output_name_to_exprs:
            raise ValueError(f"{name} not found in ExpressionsProjection")
        return self._output_name_to_exprs[name]

    def to_inner_py_exprs(self) -> list[_PyExpr]:
        return [expr._expr for expr in self]

    def resolve_schema(self, schema: Schema) -> Schema:
        fields = [e._to_field(schema) for e in self]
        return Schema._from_field_name_and_types([(f.name, f.dtype) for f in fields])

    def __repr__(self) -> str:
        return f"{self._output_name_to_exprs.values()}"

    @classmethod
    def _from_serialized(cls, _output_name_to_exprs: dict[str, Expression]) -> ExpressionsProjection:
        obj = cls.__new__(cls)
        obj._output_name_to_exprs = _output_name_to_exprs
        return obj

    def __reduce__(
        self,
    ) -> tuple[Callable[[dict[str, Expression]], ExpressionsProjection], tuple[dict[str, Expression]]]:
        return ExpressionsProjection._from_serialized, (self._output_name_to_exprs,)
