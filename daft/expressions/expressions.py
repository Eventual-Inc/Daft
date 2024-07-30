from __future__ import annotations

import builtins
import math
import os
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    Literal,
    TypeVar,
    overload,
)

import pyarrow as pa

import daft.daft as native
from daft import context
from daft.daft import CountMode, ImageFormat, ImageMode
from daft.daft import PyExpr as _PyExpr
from daft.daft import col as _col
from daft.daft import date_lit as _date_lit
from daft.daft import decimal_lit as _decimal_lit
from daft.daft import lit as _lit
from daft.daft import series_lit as _series_lit
from daft.daft import time_lit as _time_lit
from daft.daft import timestamp_lit as _timestamp_lit
from daft.daft import tokenize_decode as _tokenize_decode
from daft.daft import tokenize_encode as _tokenize_encode
from daft.daft import udf as _udf
from daft.daft import url_download as _url_download
from daft.daft import utf8_count_matches as _utf8_count_matches
from daft.datatype import DataType, TimeUnit
from daft.expressions.testing import expr_structurally_equal
from daft.logical.schema import Field, Schema
from daft.series import Series, item_to_series

if TYPE_CHECKING:
    from daft.io import IOConfig
# This allows Sphinx to correctly work against our "namespaced" accessor functions by overriding @property to
# return a class instance of the namespace instead of a property object.
elif os.getenv("DAFT_SPHINX_BUILD") == "1":
    from typing import Any

    # when building docs (with Sphinx) we need access to the functions
    # associated with the namespaces from the class, as we don't have
    # an instance; @sphinx_accessor is a @property that allows this.
    NS = TypeVar("NS")

    class sphinx_accessor(property):
        def __get__(  # type: ignore[override]
            self,
            instance: Any,
            cls: type[NS],
        ) -> NS:
            try:
                return self.fget(instance if isinstance(instance, cls) else cls)  # type: ignore[misc]
            except (AttributeError, ImportError):
                return self  # type: ignore[return-value]

    property = sphinx_accessor  # type: ignore[misc]


def lit(value: object) -> Expression:
    """Creates an Expression representing a column with every value set to the provided value

    Example:
        >>> import daft
        >>> df = daft.from_pydict({"x": [1, 2, 3]})
        >>> df = df.with_column("y", daft.lit(1))
        >>> df.show()
        ╭───────┬───────╮
        │ x     ┆ y     │
        │ ---   ┆ ---   │
        │ Int64 ┆ Int32 │
        ╞═══════╪═══════╡
        │ 1     ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 2     ┆ 1     │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
        │ 3     ┆ 1     │
        ╰───────┴───────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    Args:
        val: value of column

    Returns:
        Expression: Expression representing the value provided
    """
    if isinstance(value, datetime):
        # pyo3 datetime (PyDateTime) is not available when running in abi3 mode, workaround
        pa_timestamp = pa.scalar(value)
        i64_value = pa_timestamp.cast(pa.int64()).as_py()
        time_unit = TimeUnit.from_str(pa_timestamp.type.unit)._timeunit
        tz = pa_timestamp.type.tz
        lit_value = _timestamp_lit(i64_value, time_unit, tz)
    elif isinstance(value, date):
        # pyo3 date (PyDate) is not available when running in abi3 mode, workaround
        epoch_time = value - date(1970, 1, 1)
        lit_value = _date_lit(epoch_time.days)
    elif isinstance(value, time):
        # pyo3 time (PyTime) is not available when running in abi3 mode, workaround
        pa_time = pa.scalar(value)
        i64_value = pa_time.cast(pa.int64()).as_py()
        time_unit = TimeUnit.from_str(pa.type_for_alias(str(pa_time.type)).unit)._timeunit
        lit_value = _time_lit(i64_value, time_unit)
    elif isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        lit_value = _decimal_lit(sign == 1, digits, exponent)
    elif isinstance(value, Series):
        lit_value = _series_lit(value._series)
    else:
        lit_value = _lit(value)
    return Expression._from_pyexpr(lit_value)


def col(name: str) -> Expression:
    """Creates an Expression referring to the column with the provided name

    Example:
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

    Args:
        name: Name of column

    Returns:
        Expression: Expression representing the selected column
    """
    return Expression._from_pyexpr(_col(name))


class Expression:
    _expr: _PyExpr = None  # type: ignore

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Expression via __init__ ")

    @property
    def str(self) -> ExpressionStringNamespace:
        """Access methods that work on columns of strings"""
        return ExpressionStringNamespace.from_expression(self)

    @property
    def dt(self) -> ExpressionDatetimeNamespace:
        """Access methods that work on columns of datetimes"""
        return ExpressionDatetimeNamespace.from_expression(self)

    @property
    def embedding(self) -> ExpressionEmbeddingNamespace:
        """Access methods that work on columns of embeddings"""
        return ExpressionEmbeddingNamespace.from_expression(self)

    @property
    def float(self) -> ExpressionFloatNamespace:
        """Access methods that work on columns of floats"""
        return ExpressionFloatNamespace.from_expression(self)

    @property
    def url(self) -> ExpressionUrlNamespace:
        """Access methods that work on columns of URLs"""
        return ExpressionUrlNamespace.from_expression(self)

    @property
    def list(self) -> ExpressionListNamespace:
        """Access methods that work on columns of lists"""
        return ExpressionListNamespace.from_expression(self)

    @property
    def struct(self) -> ExpressionStructNamespace:
        """Access methods that work on columns of structs"""
        return ExpressionStructNamespace.from_expression(self)

    @property
    def map(self) -> ExpressionMapNamespace:
        """Access methods that work on columns of maps"""
        return ExpressionMapNamespace.from_expression(self)

    @property
    def image(self) -> ExpressionImageNamespace:
        """Access methods that work on columns of images"""
        return ExpressionImageNamespace.from_expression(self)

    @property
    def partitioning(self) -> ExpressionPartitioningNamespace:
        """Access methods that support partitioning operators"""
        return ExpressionPartitioningNamespace.from_expression(self)

    @property
    def json(self) -> ExpressionJsonNamespace:
        """Access methods that work on columns of json"""
        return ExpressionJsonNamespace.from_expression(self)

    @staticmethod
    def _from_pyexpr(pyexpr: _PyExpr) -> Expression:
        expr = Expression.__new__(Expression)
        expr._expr = pyexpr
        return expr

    @staticmethod
    def _to_expression(obj: object) -> Expression:
        if isinstance(obj, Expression):
            return obj
        else:
            return lit(obj)

    @staticmethod
    def udf(func: Callable, expressions: builtins.list[Expression], return_dtype: DataType) -> Expression:
        return Expression._from_pyexpr(_udf(func, [e._expr for e in expressions], return_dtype._dtype))

    def __bool__(self) -> bool:
        raise ValueError(
            "Expressions don't have a truth value. "
            "If you used Python keywords `and` `not` `or` on an expression, use `&` `~` `|` instead."
        )

    def __abs__(self) -> Expression:
        """Absolute of a numeric expression (``abs(expr)``)"""
        return self.abs()

    def abs(self) -> Expression:
        """Absolute of a numeric expression (``expr.abs()``)"""
        return Expression._from_pyexpr(abs(self._expr))

    def __add__(self, other: object) -> Expression:
        """Adds two numeric expressions or concatenates two string expressions (``e1 + e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr + expr._expr)

    def __radd__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr + self._expr)

    def __sub__(self, other: object) -> Expression:
        """Subtracts two numeric expressions (``e1 - e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr - expr._expr)

    def __rsub__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr - self._expr)

    def __mul__(self, other: object) -> Expression:
        """Multiplies two numeric expressions (``e1 * e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr * expr._expr)

    def __rmul__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr * self._expr)

    def __truediv__(self, other: object) -> Expression:
        """True divides two numeric expressions (``e1 / e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr / expr._expr)

    def __rtruediv__(self, other: object) -> Expression:
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr / self._expr)

    def __mod__(self, other: Expression) -> Expression:
        """Takes the mod of two numeric expressions (``e1 % e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr % expr._expr)

    def __rmod__(self, other: Expression) -> Expression:
        """Takes the mod of two numeric expressions (``e1 % e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr % self._expr)

    def __and__(self, other: Expression) -> Expression:
        """Takes the logical AND of two boolean expressions, or bitwise AND of two integer expressions (``e1 & e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr & expr._expr)

    def __rand__(self, other: Expression) -> Expression:
        """Takes the logical reverse AND of two boolean expressions (``e1 & e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr & self._expr)

    def __or__(self, other: Expression) -> Expression:
        """Takes the logical OR of two boolean or integer expressions, or bitwise OR of two integer expressions (``e1 | e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr | expr._expr)

    def __xor__(self, other: Expression) -> Expression:
        """Takes the logical XOR of two boolean or integer expressions, or bitwise XOR of two integer expressions (``e1 ^ e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr ^ expr._expr)

    def __ror__(self, other: Expression) -> Expression:
        """Takes the logical reverse OR of two boolean expressions (``e1 | e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr | self._expr)

    def __lt__(self, other: Expression) -> Expression:
        """Compares if an expression is less than another (``e1 < e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr < expr._expr)

    def __le__(self, other: Expression) -> Expression:
        """Compares if an expression is less than or equal to another (``e1 <= e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr <= expr._expr)

    def __eq__(self, other: Expression) -> Expression:  # type: ignore
        """Compares if an expression is equal to another (``e1 == e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr == expr._expr)

    def __ne__(self, other: Expression) -> Expression:  # type: ignore
        """Compares if an expression is not equal to another (``e1 != e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr != expr._expr)

    def __gt__(self, other: Expression) -> Expression:
        """Compares if an expression is greater than another (``e1 > e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr > expr._expr)

    def __ge__(self, other: Expression) -> Expression:
        """Compares if an expression is greater than or equal to another (``e1 >= e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >= expr._expr)

    def __lshift__(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the left (``e1 << e2``)
        Args:
            other: The number of bits to shift the expression to the left
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr << expr._expr)

    def __rshift__(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the right (``e1 >> e2``)
        .. NOTE::
            For unsigned integers, this expression perform a logical right shift.
            For signed integers, this expression perform an arithmetic right shift.

        Args:
            other: The number of bits to shift the expression to the right
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >> expr._expr)

    def __invert__(self) -> Expression:
        """Inverts a boolean expression (``~e``)"""
        expr = self._expr.__invert__()
        return Expression._from_pyexpr(expr)

    def alias(self, name: builtins.str) -> Expression:
        """Gives the expression a new name, which is its column's name in the DataFrame schema and the name
        by which subsequent expressions can refer to the results of this expression.

        Example:
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

        Args:
            name: New name for expression

        Returns:
            Expression: Renamed expression
        """
        assert isinstance(name, str)
        expr = self._expr.alias(name)
        return Expression._from_pyexpr(expr)

    def cast(self, dtype: DataType) -> Expression:
        """Casts an expression to the given datatype if possible

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"float": [1.0, 2.5, None]})
            >>> df = df.select(daft.col("float").cast(daft.DataType.int64()))
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

        Returns:
            Expression: Expression with the specified new datatype
        """
        assert isinstance(dtype, DataType)
        expr = self._expr.cast(dtype._dtype)
        return Expression._from_pyexpr(expr)

    def ceil(self) -> Expression:
        """The ceiling of a numeric expression (``expr.ceil()``)"""
        expr = self._expr.ceil()
        return Expression._from_pyexpr(expr)

    def floor(self) -> Expression:
        """The floor of a numeric expression (``expr.floor()``)"""
        expr = self._expr.floor()
        return Expression._from_pyexpr(expr)

    def sign(self) -> Expression:
        """The sign of a numeric expression (``expr.sign()``)"""
        expr = self._expr.sign()
        return Expression._from_pyexpr(expr)

    def round(self, decimals: int = 0) -> Expression:
        """The round of a numeric expression (``expr.round(decimals = 0)``)

        Args:
            decimals: number of decimal places to round to. Defaults to 0.
        """
        assert isinstance(decimals, int)
        expr = self._expr.round(decimals)
        return Expression._from_pyexpr(expr)

    def sqrt(self) -> Expression:
        """The square root of a numeric expression (``expr.sqrt()``)"""
        expr = self._expr.sqrt()
        return Expression._from_pyexpr(expr)

    def sin(self) -> Expression:
        """The elementwise sine of a numeric expression (``expr.sin()``)"""
        expr = self._expr.sin()
        return Expression._from_pyexpr(expr)

    def cos(self) -> Expression:
        """The elementwise cosine of a numeric expression (``expr.cos()``)"""
        expr = self._expr.cos()
        return Expression._from_pyexpr(expr)

    def tan(self) -> Expression:
        """The elementwise tangent of a numeric expression (``expr.tan()``)"""
        expr = self._expr.tan()
        return Expression._from_pyexpr(expr)

    def cot(self) -> Expression:
        """The elementwise cotangent of a numeric expression (``expr.cot()``)"""
        expr = self._expr.cot()
        return Expression._from_pyexpr(expr)

    def arcsin(self) -> Expression:
        """The elementwise arc sine of a numeric expression (``expr.arcsin()``)"""
        expr = self._expr.arcsin()
        return Expression._from_pyexpr(expr)

    def arccos(self) -> Expression:
        """The elementwise arc cosine of a numeric expression (``expr.arccos()``)"""
        expr = self._expr.arccos()
        return Expression._from_pyexpr(expr)

    def arctan(self) -> Expression:
        """The elementwise arc tangent of a numeric expression (``expr.arctan()``)"""
        expr = self._expr.arctan()
        return Expression._from_pyexpr(expr)

    def arctan2(self, other: Expression) -> Expression:
        """Calculates the four quadrant arctangent of coordinates (y, x), in radians (``expr_y.arctan2(expr_x)``)

        * ``x = 0``, ``y = 0``: ``0``
        * ``x >= 0``: ``[-pi/2, pi/2]``
        * ``y >= 0``: ``(pi/2, pi]``
        * ``y < 0``: ``(-pi, -pi/2)``"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr.arctan2(expr._expr))

    def arctanh(self) -> Expression:
        """The elementwise inverse hyperbolic tangent of a numeric expression (``expr.arctanh()``)"""
        expr = self._expr.arctanh()
        return Expression._from_pyexpr(expr)

    def arccosh(self) -> Expression:
        """The elementwise inverse hyperbolic cosine of a numeric expression (``expr.arccosh()``)"""
        expr = self._expr.arccosh()
        return Expression._from_pyexpr(expr)

    def arcsinh(self) -> Expression:
        """The elementwise inverse hyperbolic sine of a numeric expression (``expr.arcsinh()``)"""
        expr = self._expr.arcsinh()
        return Expression._from_pyexpr(expr)

    def radians(self) -> Expression:
        """The elementwise radians of a numeric expression (``expr.radians()``)"""
        expr = self._expr.radians()
        return Expression._from_pyexpr(expr)

    def degrees(self) -> Expression:
        """The elementwise degrees of a numeric expression (``expr.degrees()``)"""
        expr = self._expr.degrees()
        return Expression._from_pyexpr(expr)

    def log2(self) -> Expression:
        """The elementwise log base 2 of a numeric expression (``expr.log2()``)"""
        expr = self._expr.log2()
        return Expression._from_pyexpr(expr)

    def log10(self) -> Expression:
        """The elementwise log base 10 of a numeric expression (``expr.log10()``)"""
        expr = self._expr.log10()
        return Expression._from_pyexpr(expr)

    def log(self, base: float = math.e) -> Expression:  # type: ignore
        """The elementwise log with given base, of a numeric expression (``expr.log(base = math.e)``)
        Args:
            base: The base of the logarithm. Defaults to e.
        """
        assert isinstance(base, (int, float)), f"base must be an int or float, but {type(base)} was provided."
        expr = self._expr.log(float(base))
        return Expression._from_pyexpr(expr)

    def ln(self) -> Expression:
        """The elementwise natural log of a numeric expression (``expr.ln()``)"""
        expr = self._expr.ln()
        return Expression._from_pyexpr(expr)

    def exp(self) -> Expression:
        """The e^self of a numeric expression (``expr.exp()``)"""
        expr = self._expr.exp()
        return Expression._from_pyexpr(expr)

    def bitwise_and(self, other: Expression) -> Expression:
        """Bitwise AND of two integer expressions (``expr.bitwise_and(other)``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr & expr._expr)

    def bitwise_or(self, other: Expression) -> Expression:
        """Bitwise OR of two integer expressions (``expr.bitwise_or(other)``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr | expr._expr)

    def bitwise_xor(self, other: Expression) -> Expression:
        """Bitwise XOR of two integer expressions (``expr.bitwise_xor(other)``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr ^ expr._expr)

    def shift_left(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the left (``expr << other``)
        Args:
            other: The number of bits to shift the expression to the left
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr << expr._expr)

    def shift_right(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the right (``expr >> other``)
        .. NOTE::
            For unsigned integers, this expression perform a logical right shift.
            For signed integers, this expression perform an arithmetic right shift.

        Args:
            other: The number of bits to shift the expression to the right
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >> expr._expr)

    def count(self, mode: CountMode = CountMode.Valid) -> Expression:
        """Counts the number of values in the expression.

        Args:
            mode: whether to count all values, non-null (valid) values, or null values. Defaults to CountMode.Valid.
        """
        expr = self._expr.count(mode)
        return Expression._from_pyexpr(expr)

    def sum(self) -> Expression:
        """Calculates the sum of the values in the expression"""
        expr = self._expr.sum()
        return Expression._from_pyexpr(expr)

    def approx_percentiles(self, percentiles: builtins.float | builtins.list[builtins.float]) -> Expression:
        """Calculates the approximate percentile(s) for a column of numeric values

        For numeric columns, we use the `sketches_ddsketch crate <https://docs.rs/sketches-ddsketch/latest/sketches_ddsketch/index.html>`_.
        This is a Rust implementation of the paper `DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees (Masson et al.) <https://arxiv.org/pdf/1908.10693>`_

        1. Null values are ignored in the computation of the percentiles
        2. If all values are Null then the result will also be Null
        3. If ``percentiles`` are supplied as a single float, then the resultant column is a ``Float64`` column
        4. If ``percentiles`` is supplied as a list, then the resultant column is a ``FixedSizeList[Float64; N]`` column, where ``N`` is the length of the supplied list.

        Example:
            A global calculation of approximate percentiles:

            >>> import daft
            >>> df = daft.from_pydict({"scores": [1, 2, 3, 4, 5, None]})
            >>> df = df.agg(
            ...     df["scores"].approx_percentiles(0.5).alias("approx_median_score"),
            ...     df["scores"].approx_percentiles([0.25, 0.5, 0.75]).alias("approx_percentiles_scores"),
            ... )
            >>> df.show()
            ╭─────────────────────┬────────────────────────────────╮
            │ approx_median_score ┆ approx_percentiles_scores      │
            │ ---                 ┆ ---                            │
            │ Float64             ┆ FixedSizeList[Float64; 3]      │
            ╞═════════════════════╪════════════════════════════════╡
            │ 2.9742334234767167  ┆ [1.993661701417351, 2.9742334… │
            ╰─────────────────────┴────────────────────────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

            A grouped calculation of approximate percentiles:

            >>> df = daft.from_pydict({"class":  ["a", "a", "a", "b", "c"], "scores": [1, 2, 3, 1, None]})
            >>> df = df.groupby("class").agg(
            ...     df["scores"].approx_percentiles(0.5).alias("approx_median_score"),
            ...     df["scores"].approx_percentiles([0.25, 0.5, 0.75]).alias("approx_percentiles_scores"),
            ... )
            >>> df.show()
            ╭───────┬─────────────────────┬────────────────────────────────╮
            │ class ┆ approx_median_score ┆ approx_percentiles_scores      │
            │ ---   ┆ ---                 ┆ ---                            │
            │ Utf8  ┆ Float64             ┆ FixedSizeList[Float64; 3]      │
            ╞═══════╪═════════════════════╪════════════════════════════════╡
            │ c     ┆ None                ┆ None                           │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ a     ┆ 1.993661701417351   ┆ [0.9900000000000001, 1.993661… │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b     ┆ 0.9900000000000001  ┆ [0.9900000000000001, 0.990000… │
            ╰───────┴─────────────────────┴────────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            percentiles: the percentile(s) at which to find approximate values at. Can be provided as a single
                float or a list of floats.

        Returns:
            A new expression representing the approximate percentile(s). If `percentiles` was a single float, this will be a new `Float64` expression. If `percentiles` was a list of floats, this will be a new expression with type: `FixedSizeList[Float64, len(percentiles)]`.
        """
        expr = self._expr.approx_percentiles(percentiles)
        return Expression._from_pyexpr(expr)

    def mean(self) -> Expression:
        """Calculates the mean of the values in the expression"""
        expr = self._expr.mean()
        return Expression._from_pyexpr(expr)

    def min(self) -> Expression:
        """Calculates the minimum value in the expression"""
        expr = self._expr.min()
        return Expression._from_pyexpr(expr)

    def max(self) -> Expression:
        """Calculates the maximum value in the expression"""
        expr = self._expr.max()
        return Expression._from_pyexpr(expr)

    def any_value(self, ignore_nulls=False) -> Expression:
        """Returns any value in the expression

        Args:
            ignore_nulls: whether to ignore null values when selecting the value. Defaults to False.
        """
        expr = self._expr.any_value(ignore_nulls)
        return Expression._from_pyexpr(expr)

    def agg_list(self) -> Expression:
        """Aggregates the values in the expression into a list"""
        expr = self._expr.agg_list()
        return Expression._from_pyexpr(expr)

    def agg_concat(self) -> Expression:
        """Aggregates the values in the expression into a single string by concatenating them"""
        expr = self._expr.agg_concat()
        return Expression._from_pyexpr(expr)

    def _explode(self) -> Expression:
        expr = self._expr.explode()
        return Expression._from_pyexpr(expr)

    def if_else(self, if_true: Expression, if_false: Expression) -> Expression:
        """Conditionally choose values between two expressions using the current boolean expression as a condition

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})
            >>> df = df.with_column("A_if_bigger_else_B", (df["A"] > df["B"]).if_else(df["A"], df["B"]),)
            >>> df.collect()
            ╭───────┬───────┬────────────────────╮
            │ A     ┆ B     ┆ A_if_bigger_else_B │
            │ ---   ┆ ---   ┆ ---                │
            │ Int64 ┆ Int64 ┆ Int64              │
            ╞═══════╪═══════╪════════════════════╡
            │ 1     ┆ 0     ┆ 1                  │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2     ┆ 2     ┆ 2                  │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 3     ┆ 4     ┆ 4                  │
            ╰───────┴───────┴────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            if_true (Expression): Values to choose if condition is true
            if_false (Expression): Values to choose if condition is false

        Returns:
            Expression: New expression where values are chosen from `if_true` and `if_false`.
        """
        if_true = Expression._to_expression(if_true)
        if_false = Expression._to_expression(if_false)
        return Expression._from_pyexpr(self._expr.if_else(if_true._expr, if_false._expr))

    def apply(self, func: Callable, return_dtype: DataType) -> Expression:
        """Apply a function on each value in a given expression

        .. NOTE::
            This is just syntactic sugar on top of a UDF and is convenient to use when your function only operates
            on a single column, and does not benefit from executing on batches. For either of those other use-cases,
            use a UDF instead.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["1", "2", "tim"]})
            >>> def f(x_val: str) -> int:
            ...     if x_val.isnumeric():
            ...         return int(x_val)
            ...     else:
            ...         return 0
            >>> df.with_column("num_x", df['x'].apply(f, return_dtype=daft.DataType.int64())).collect()
            ╭──────┬───────╮
            │ x    ┆ num_x │
            │ ---  ┆ ---   │
            │ Utf8 ┆ Int64 │
            ╞══════╪═══════╡
            │ 1    ┆ 1     │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2    ┆ 2     │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ tim  ┆ 0     │
            ╰──────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            func: Function to run per value of the expression
            return_dtype: Return datatype of the function that was ran

        Returns:
            Expression: New expression after having run the function on the expression
        """
        from daft.udf import UDF

        def batch_func(self_series):
            return [func(x) for x in self_series.to_pylist()]

        return UDF(func=batch_func, return_dtype=return_dtype)(self)

    def is_null(self) -> Expression:
        """Checks if values in the Expression are Null (a special value indicating missing data)

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1., None, float("nan")]})
            >>> df = df.select(df['x'].is_null())
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

        Returns:
            Expression: Boolean Expression indicating whether values are missing
        """
        expr = self._expr.is_null()
        return Expression._from_pyexpr(expr)

    def not_null(self) -> Expression:
        """Checks if values in the Expression are not Null (a special value indicating missing data)

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1., None, float("nan")]})
            >>> df = df.select(df['x'].not_null())
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

        Returns:
            Expression: Boolean Expression indicating whether values are not missing
        """
        expr = self._expr.not_null()
        return Expression._from_pyexpr(expr)

    def fill_null(self, fill_value: Expression) -> Expression:
        """Fills null values in the Expression with the provided fill_value

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": [1, None, 3]})
            >>> df = df.select(df["data"].fill_null(2))
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

        Returns:
            Expression: Expression with null values filled with the provided fill_value
        """

        fill_value = Expression._to_expression(fill_value)
        expr = self._expr.fill_null(fill_value._expr)
        return Expression._from_pyexpr(expr)

    def is_in(self, other: Any) -> Expression:
        """Checks if values in the Expression are in the provided list

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": [1, 2, 3]})
            >>> df = df.select(df["data"].is_in([1, 3]))
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

        Returns:
            Expression: Boolean Expression indicating whether values are in the provided list
        """

        if not isinstance(other, Expression):
            series = item_to_series("items", other)
            other = Expression._to_expression(series)

        expr = self._expr.is_in(other._expr)
        return Expression._from_pyexpr(expr)

    def between(self, lower: Any, upper: Any) -> Expression:
        """Checks if values in the Expression are between lower and upper, inclusive.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": [1, 2, 3, 4]})
            >>> df = df.select(df["data"].between(1, 2))
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

        Returns:
            Expression: Boolean Expression indicating whether values are between lower and upper, inclusive.
        """
        lower = Expression._to_expression(lower)
        upper = Expression._to_expression(upper)

        expr = self._expr.between(lower._expr, upper._expr)
        return Expression._from_pyexpr(expr)

    def hash(self, seed: Any | None = None) -> Expression:
        """Hashes the values in the Expression"""
        if seed is None:
            expr = native.hash(self._expr)
        else:
            if not isinstance(seed, Expression):
                seed = lit(seed)
            expr = native.hash(self._expr, seed._expr)
        return Expression._from_pyexpr(expr)

    def minhash(
        self,
        num_hashes: int,
        ngram_size: int,
        seed: int = 1,
    ) -> Expression:
        """
        Runs the MinHash algorithm on the series.

        For a string, calculates the minimum hash over all its ngrams,
        repeating with `num_hashes` permutations. Returns as a list of 32-bit unsigned integers.

        Tokens for the ngrams are delimited by spaces.
        MurmurHash is used for the initial hash.
        The strings are not normalized or pre-processed, so it is recommended
        to normalize the strings yourself.

        Args:
            num_hashes: The number of hash permutations to compute.
            ngram_size: The number of tokens in each shingle/ngram.
            seed (optional): Seed used for generating permutations and the initial string hashes. Defaults to 1.
        """
        assert isinstance(num_hashes, int)
        assert isinstance(ngram_size, int)
        assert isinstance(seed, int)
        return Expression._from_pyexpr(native.minhash(self._expr, num_hashes, ngram_size, seed))

    def name(self) -> builtins.str:
        return self._expr.name()

    def __repr__(self) -> builtins.str:
        return repr(self._expr)

    def _to_sql(self) -> builtins.str | None:
        return self._expr.to_sql()

    def _to_field(self, schema: Schema) -> Field:
        return Field._from_pyfield(self._expr.to_field(schema._schema))

    def __hash__(self) -> int:
        return self._expr.__hash__()

    def __reduce__(self) -> tuple:
        return Expression._from_pyexpr, (self._expr,)

    def _input_mapping(self) -> builtins.str | None:
        return self._expr._input_mapping()


SomeExpressionNamespace = TypeVar("SomeExpressionNamespace", bound="ExpressionNamespace")


class ExpressionNamespace:
    _expr: _PyExpr

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a ExpressionNamespace via __init__ ")

    @classmethod
    def from_expression(cls: type[SomeExpressionNamespace], expr: Expression) -> SomeExpressionNamespace:
        ns = cls.__new__(cls)
        ns._expr = expr._expr
        return ns


class ExpressionUrlNamespace(ExpressionNamespace):
    @staticmethod
    def _should_use_multithreading_tokio_runtime() -> bool:
        """Whether or not our expression should use the multithreaded tokio runtime under the hood, or a singlethreaded one

        This matters because for distributed workloads, each process has its own tokio I/O runtime. if each distributed process
        is multithreaded (by default we spin up `N_CPU` threads) then we will be running `(N_CPU * N_PROC)` number of threads, and
        opening `(N_CPU * N_PROC * max_connections)` number of connections. This is too large for big machines with many CPU cores.

        Hence for Ray we default to doing the singlethreaded runtime. This means that we will have a limit of
        `(singlethreaded=1 * N_PROC * max_connections)` number of open connections per machine, which works out to be reasonable at ~2-4k connections.

        For local execution, we run in a single process which means that it all shares the same tokio I/O runtime and connection pool.
        Thus we just have `(multithreaded=N_CPU * max_connections)` number of open connections, which is usually reasonable as well.
        """
        using_ray_runner = context.get_context().is_ray_runner
        return not using_ray_runner

    @staticmethod
    def _override_io_config_max_connections(max_connections: int, io_config: IOConfig | None) -> IOConfig:
        """Use a user-provided `max_connections` argument to override the value in S3Config

        This is because our Rust code under the hood actually does `min(S3Config's max_connections, url_download's max_connections)` to
        determine how many connections to allow per-thread. Thus we need to override the io_config here to ensure that the user's max_connections
        is correctly applied in our Rust code.
        """
        io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
        io_config = io_config.replace(s3=io_config.s3.replace(max_connections=max_connections))
        return io_config

    def download(
        self,
        max_connections: int = 32,
        on_error: Literal["raise"] | Literal["null"] = "raise",
        io_config: IOConfig | None = None,
        use_native_downloader: bool = True,
    ) -> Expression:
        """Treats each string as a URL, and downloads the bytes contents as a bytes column

        .. NOTE::
            If you are observing excessive S3 issues (such as timeouts, DNS errors or slowdown errors) during URL downloads,
            you may wish to reduce the value of ``max_connections`` (defaults to 32) to reduce the amount of load you are placing
            on your S3 servers.

            Alternatively, if you are running on machines with lower number of cores but very high network bandwidth, you can increase
            ``max_connections`` to get higher throughput with additional parallelism

        Args:
            max_connections: The maximum number of connections to use per thread to use for downloading URLs. Defaults to 32.
            on_error: Behavior when a URL download error is encountered - "raise" to raise the error immediately or "null" to log
                the error but fallback to a Null value. Defaults to "raise".
            io_config: IOConfig to use when accessing remote storage. Note that the S3Config's `max_connections` parameter will be overridden
                with `max_connections` that is passed in as a kwarg.
            use_native_downloader (bool): Use the native downloader rather than python based one.
                Defaults to True.

        Returns:
            Expression: a Binary expression which is the bytes contents of the URL, or None if an error occurred during download
        """
        if use_native_downloader:
            raise_on_error = False
            if on_error == "raise":
                raise_on_error = True
            elif on_error == "null":
                raise_on_error = False
            else:
                raise NotImplementedError(f"Unimplemented on_error option: {on_error}.")

            if not (isinstance(max_connections, int) and max_connections > 0):
                raise ValueError(f"Invalid value for `max_connections`: {max_connections}")

            multi_thread = ExpressionUrlNamespace._should_use_multithreading_tokio_runtime()
            io_config = ExpressionUrlNamespace._override_io_config_max_connections(max_connections, io_config)
            return Expression._from_pyexpr(
                _url_download(self._expr, max_connections, raise_on_error, multi_thread, io_config)
            )
        else:
            from daft.udf_library import url_udfs

            return url_udfs.download_udf(
                Expression._from_pyexpr(self._expr),
                max_worker_threads=max_connections,
                on_error=on_error,
            )

    def upload(
        self,
        location: str,
        max_connections: int = 32,
        io_config: IOConfig | None = None,
    ) -> Expression:
        """Uploads a column of binary data to the provided location (also supports S3, local etc)

        Files will be written into the location (folder) with a generated UUID filename, and the result
        will be returned as a column of string paths that is compatible with the ``.url.download()`` Expression.

        Example:
            >>> col("data").url.upload("s3://my-bucket/my-folder")

        Args:
            location: a folder location to upload data into
            max_connections: The maximum number of connections to use per thread to use for uploading data. Defaults to 32.
            io_config: IOConfig to use when uploading data

        Returns:
            Expression: a String expression containing the written filepath
        """
        if not (isinstance(max_connections, int) and max_connections > 0):
            raise ValueError(f"Invalid value for `max_connections`: {max_connections}")

        multi_thread = ExpressionUrlNamespace._should_use_multithreading_tokio_runtime()
        io_config = ExpressionUrlNamespace._override_io_config_max_connections(max_connections, io_config)
        return Expression._from_pyexpr(
            native.url_upload(self._expr, location, max_connections, multi_thread, io_config)
        )


class ExpressionFloatNamespace(ExpressionNamespace):
    def is_nan(self) -> Expression:
        """Checks if values are NaN (a special float value indicating not-a-number)

        .. NOTE::
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": [1., None, float("nan")]})
            >>> df = df.select(df["data"].float.is_nan())
            >>> df.collect()
            ╭─────────╮
            │ data    │
            │ ---     │
            │ Boolean │
            ╞═════════╡
            │ false   │
            ├╌╌╌╌╌╌╌╌╌┤
            │ None    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ true    │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: Boolean Expression indicating whether values are invalid.
        """
        return Expression._from_pyexpr(self._expr.is_nan())

    def is_inf(self) -> Expression:
        """Checks if values in the Expression are Infinity.

        .. NOTE::
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": [-float("inf"), 0., float("inf"), None]})
            >>> df = df.select(df["data"].float.is_inf())
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
            ├╌╌╌╌╌╌╌╌╌┤
            │ None    │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)

        Returns:
            Expression: Boolean Expression indicating whether values are Infinity.
        """
        return Expression._from_pyexpr(self._expr.is_inf())

    def not_nan(self) -> Expression:
        """Checks if values are not NaN (a special float value indicating not-a-number)

        .. NOTE::
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1.0, None, float("nan")]})
            >>> df = df.select(df["x"].float.not_nan())
            >>> df.collect()
            ╭─────────╮
            │ x       │
            │ ---     │
            │ Boolean │
            ╞═════════╡
            │ true    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ None    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: Boolean Expression indicating whether values are not invalid.
        """
        return Expression._from_pyexpr(self._expr.not_nan())

    def fill_nan(self, fill_value: Expression) -> Expression:
        """Fills NaN values in the Expression with the provided fill_value

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": [1.1, float("nan"), 3.3]})
            >>> df = df.with_column("filled", df["data"].float.fill_nan(2.2))
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

        Returns:
            Expression: Expression with Nan values filled with the provided fill_value
        """

        fill_value = Expression._to_expression(fill_value)
        expr = self._expr.fill_nan(fill_value._expr)
        return Expression._from_pyexpr(expr)


class ExpressionDatetimeNamespace(ExpressionNamespace):
    def date(self) -> Expression:
        """Retrieves the date for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [
            ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
            ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
            ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("date", df["x"].dt.date())
            >>> df.show()
            ╭───────────────────────────────┬────────────╮
            │ x                             ┆ date       │
            │ ---                           ┆ ---        │
            │ Timestamp(Microseconds, None) ┆ Date       │
            ╞═══════════════════════════════╪════════════╡
            │ 2021-01-01 05:01:01           ┆ 2021-01-01 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 06:01:59           ┆ 2021-01-02 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-03 07:02:00           ┆ 2021-01-03 │
            ╰───────────────────────────────┴────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a Date expression
        """
        return Expression._from_pyexpr(self._expr.dt_date())

    def day(self) -> Expression:
        """Retrieves the day for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [
            ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
            ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
            ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("day", df["x"].dt.day())
            >>> df.show()
            ╭───────────────────────────────┬────────╮
            │ x                             ┆ day    │
            │ ---                           ┆ ---    │
            │ Timestamp(Microseconds, None) ┆ UInt32 │
            ╞═══════════════════════════════╪════════╡
            │ 2021-01-01 05:01:01           ┆ 1      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 06:01:59           ┆ 2      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-03 07:02:00           ┆ 3      │
            ╰───────────────────────────────┴────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a UInt32 expression with just the day extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_day())

    def hour(self) -> Expression:
        """Retrieves the day for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [
            ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
            ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
            ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("hour", df["x"].dt.hour())
            >>> df.show()
            ╭───────────────────────────────┬────────╮
            │ x                             ┆ hour   │
            │ ---                           ┆ ---    │
            │ Timestamp(Microseconds, None) ┆ UInt32 │
            ╞═══════════════════════════════╪════════╡
            │ 2021-01-01 05:01:01           ┆ 5      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 06:01:59           ┆ 6      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-03 07:02:00           ┆ 7      │
            ╰───────────────────────────────┴────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a UInt32 expression with just the day extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_hour())

    def minute(self) -> Expression:
        """Retrieves the minute for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [
            ...             datetime.datetime(2021, 1, 1, 5, 1, 1),
            ...             datetime.datetime(2021, 1, 2, 6, 1, 59),
            ...             datetime.datetime(2021, 1, 3, 7, 2, 0),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("minute", df["x"].dt.minute())
            >>> df.show()
            ╭───────────────────────────────┬────────╮
            │ x                             ┆ minute │
            │ ---                           ┆ ---    │
            │ Timestamp(Microseconds, None) ┆ UInt32 │
            ╞═══════════════════════════════╪════════╡
            │ 2021-01-01 05:01:01           ┆ 1      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 06:01:59           ┆ 1      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-03 07:02:00           ┆ 2      │
            ╰───────────────────────────────┴────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a UInt32 expression with just the minute extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_minute())

    def second(self) -> Expression:
        """Retrieves the second for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [
            ...             datetime.datetime(2021, 1, 1, 0, 1, 1),
            ...             datetime.datetime(2021, 1, 1, 0, 1, 59),
            ...             datetime.datetime(2021, 1, 1, 0, 2, 0),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("second", df["x"].dt.second())
            >>> df.show()
            ╭───────────────────────────────┬────────╮
            │ x                             ┆ second │
            │ ---                           ┆ ---    │
            │ Timestamp(Microseconds, None) ┆ UInt32 │
            ╞═══════════════════════════════╪════════╡
            │ 2021-01-01 00:01:01           ┆ 1      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-01 00:01:59           ┆ 59     │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2021-01-01 00:02:00           ┆ 0      │
            ╰───────────────────────────────┴────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a UInt32 expression with just the second extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_second())

    def time(self) -> Expression:
        """Retrieves the time for a datetime column

        Example:
            >>> col("x").dt.time()

        Returns:
            Expression: a Time expression
        """
        return Expression._from_pyexpr(self._expr.dt_time())

    def month(self) -> Expression:
        """Retrieves the month for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict({
            ...         "datetime": [
            ...             datetime.datetime(2024, 7, 3, 0, 0, 0),
            ...             datetime.datetime(2024, 6, 4, 0, 0, 0),
            ...             datetime.datetime(2024, 5, 5, 0, 0, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("month", df["datetime"].dt.month()).collect()
            ╭───────────────────────────────┬────────╮
            │ datetime                      ┆ month  │
            │ ---                           ┆ ---    │
            │ Timestamp(Microseconds, None) ┆ UInt32 │
            ╞═══════════════════════════════╪════════╡
            │ 2024-07-03 00:00:00           ┆ 7      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2024-06-04 00:00:00           ┆ 6      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 2024-05-05 00:00:00           ┆ 5      │
            ╰───────────────────────────────┴────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a UInt32 expression with just the month extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_month())

    def year(self) -> Expression:
        """Retrieves the year for a datetime column

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict({
            ...         "datetime": [
            ...             datetime.datetime(2024, 7, 3, 0, 0, 0),
            ...             datetime.datetime(2023, 7, 4, 0, 0, 0),
            ...             datetime.datetime(2022, 7, 5, 0, 0, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("year", df["datetime"].dt.year()).collect()
            ╭───────────────────────────────┬───────╮
            │ datetime                      ┆ year  │
            │ ---                           ┆ ---   │
            │ Timestamp(Microseconds, None) ┆ Int32 │
            ╞═══════════════════════════════╪═══════╡
            │ 2024-07-03 00:00:00           ┆ 2024  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2023-07-04 00:00:00           ┆ 2023  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 2022-07-05 00:00:00           ┆ 2022  │
            ╰───────────────────────────────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)


        Returns:
            Expression: a UInt32 expression with just the year extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_year())

    def day_of_week(self) -> Expression:
        """Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict({
            ...         "datetime": [
            ...             datetime.datetime(2024, 7, 3, 0, 0, 0),
            ...             datetime.datetime(2024, 7, 4, 0, 0, 0),
            ...             datetime.datetime(2024, 7, 5, 0, 0, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("day_of_week", df["datetime"].dt.day_of_week()).collect()
            ╭───────────────────────────────┬─────────────╮
            │ datetime                      ┆ day_of_week │
            │ ---                           ┆ ---         │
            │ Timestamp(Microseconds, None) ┆ UInt32      │
            ╞═══════════════════════════════╪═════════════╡
            │ 2024-07-03 00:00:00           ┆ 2           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-07-04 00:00:00           ┆ 3           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-07-05 00:00:00           ┆ 4           │
            ╰───────────────────────────────┴─────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a UInt32 expression with just the day_of_week extracted from a datetime column
        """
        return Expression._from_pyexpr(self._expr.dt_day_of_week())

    def truncate(self, interval: str, relative_to: Expression | None = None) -> Expression:
        """Truncates the datetime column to the specified interval

        Example:
            >>> import daft, datetime
            >>> df = daft.from_pydict({
            ...         "datetime": [
            ...             datetime.datetime(2021, 1, 1, 0, 1, 1),
            ...             datetime.datetime(2021, 1, 1, 0, 1, 59),
            ...             datetime.datetime(2021, 1, 1, 0, 2, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("truncated", df["datetime"].dt.truncate("1 minute")).collect()
            ╭───────────────────────────────┬───────────────────────────────╮
            │ datetime                      ┆ truncated                     │
            │ ---                           ┆ ---                           │
            │ Timestamp(Microseconds, None) ┆ Timestamp(Microseconds, None) │
            ╞═══════════════════════════════╪═══════════════════════════════╡
            │ 2021-01-01 00:01:01           ┆ 2021-01-01 00:01:00           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-01 00:01:59           ┆ 2021-01-01 00:01:00           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-01 00:02:00           ┆ 2021-01-01 00:02:00           │
            ╰───────────────────────────────┴───────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            interval: The interval to truncate to. Must be a string representing a valid interval in "{integer} {unit}" format, e.g. "1 day". Valid time units are: 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week'.
            relative_to: Optional timestamp to truncate relative to. If not provided, truncates to the start of the Unix epoch: 1970-01-01 00:00:00.

        Returns:
            Expression: a DateTime expression truncated to the specified interval
        """
        relative_to = Expression._to_expression(relative_to)
        return Expression._from_pyexpr(self._expr.dt_truncate(interval, relative_to._expr))


class ExpressionStringNamespace(ExpressionNamespace):
    def contains(self, substr: str | Expression) -> Expression:
        """Checks whether each string contains the given pattern in a string column

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df = df.select(df["x"].str.contains("o"))
            >>> df.show()
            ╭─────────╮
            │ x       │
            │ ---     │
            │ Boolean │
            ╞═════════╡
            │ true    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value contains the provided pattern
        """
        substr_expr = Expression._to_expression(substr)
        return Expression._from_pyexpr(self._expr.utf8_contains(substr_expr._expr))

    def match(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given regular expression pattern in a string column

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df.with_column("match", df["x"].str.match("ba.")).collect()
            ╭──────┬─────────╮
            │ x    ┆ match   │
            │ ---  ┆ ---     │
            │ Utf8 ┆ Boolean │
            ╞══════╪═════════╡
            │ foo  ┆ false   │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ bar  ┆ true    │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ baz  ┆ true    │
            ╰──────┴─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: Regex pattern to search for as string or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_match(pattern_expr._expr))

    def endswith(self, suffix: str | Expression) -> Expression:
        """Checks whether each string ends with the given pattern in a string column

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["geftdaft", "lazy", "daft.io"]})
            >>> df.with_column("match", df["x"].str.endswith("daft")).collect()
            ╭──────────┬─────────╮
            │ x        ┆ match   │
            │ ---      ┆ ---     │
            │ Utf8     ┆ Boolean │
            ╞══════════╪═════════╡
            │ geftdaft ┆ true    │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ lazy     ┆ false   │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ daft.io  ┆ false   │
            ╰──────────┴─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value ends with the provided pattern
        """
        suffix_expr = Expression._to_expression(suffix)
        return Expression._from_pyexpr(self._expr.utf8_endswith(suffix_expr._expr))

    def startswith(self, prefix: str | Expression) -> Expression:
        """Checks whether each string starts with the given pattern in a string column

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["geftdaft", "lazy", "daft.io"]})
            >>> df.with_column("match", df["x"].str.startswith("daft")).collect()
            ╭──────────┬─────────╮
            │ x        ┆ match   │
            │ ---      ┆ ---     │
            │ Utf8     ┆ Boolean │
            ╞══════════╪═════════╡
            │ geftdaft ┆ false   │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ lazy     ┆ false   │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ daft.io  ┆ true    │
            ╰──────────┴─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value starts with the provided pattern
        """
        prefix_expr = Expression._to_expression(prefix)
        return Expression._from_pyexpr(self._expr.utf8_startswith(prefix_expr._expr))

    def split(self, pattern: str | Expression, regex: bool = False) -> Expression:
        r"""Splits each string on the given literal or regex pattern, into a list of strings.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": ["daft.distributed.query", "a.b.c", "1.2.3"]})
            >>> df.with_column("split", df["data"].str.split(".")).collect()
            ╭────────────────────────┬────────────────────────────╮
            │ data                   ┆ split                      │
            │ ---                    ┆ ---                        │
            │ Utf8                   ┆ List[Utf8]                 │
            ╞════════════════════════╪════════════════════════════╡
            │ daft.distributed.query ┆ [daft, distributed, query] │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ a.b.c                  ┆ [a, b, c]                  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1.2.3                  ┆ [1, 2, 3]                  │
            ╰────────────────────────┴────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            Split on a regex pattern

            >>> import daft
            >>> df = daft.from_pydict({"data": ["daft.distributed...query", "a.....b.c", "1.2...3.."]})
            >>> df.with_column("split", df["data"].str.split(r"\.+", regex=True)).collect()
            ╭──────────────────────────┬────────────────────────────╮
            │ data                     ┆ split                      │
            │ ---                      ┆ ---                        │
            │ Utf8                     ┆ List[Utf8]                 │
            ╞══════════════════════════╪════════════════════════════╡
            │ daft.distributed...query ┆ [daft, distributed, query] │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ a.....b.c                ┆ [a, b, c]                  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1.2...3..                ┆ [1, 2, 3, ]                │
            ╰──────────────────────────┴────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)


        Args:
            pattern: The pattern on which each string should be split, or a column to pick such patterns from.
            regex: Whether the pattern is a regular expression. Defaults to False.

        Returns:
            Expression: A List[Utf8] expression containing the string splits for each string in the column.
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_split(pattern_expr._expr, regex))

    def concat(self, other: str) -> Expression:
        """Concatenates two string expressions together

        .. NOTE::
            Another (easier!) way to invoke this functionality is using the Python `+` operator which is
            aliased to using `.str.concat`. These are equivalent:

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"], "y": ["a", "b", "c"]})
            >>> df.select(col("x").str.concat(col("y"))).collect()
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

        Args:
            other (Expression): a string expression to concatenate with

        Returns:
            Expression: a String expression which is `self` concatenated with `other`
        """
        # Delegate to + operator implementation.
        return Expression._from_pyexpr(self._expr) + other

    def extract(self, pattern: str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from the first regex match in each string in a string column.

        Notes:
            If index is 0, the entire match is returned.
            If the pattern does not match or the group does not exist, a null value is returned.

        Example:
            >>> import daft
            >>> regex = r"(\d)(\d*)"
            >>> df = daft.from_pydict({"x": ["123-456", "789-012", "345-678"]})
            >>> df.with_column("match", df["x"].str.extract(regex)).collect()
            ╭─────────┬───────╮
            │ x       ┆ match │
            │ ---     ┆ ---   │
            │ Utf8    ┆ Utf8  │
            ╞═════════╪═══════╡
            │ 123-456 ┆ 123   │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 789-012 ┆ 789   │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 345-678 ┆ 345   │
            ╰─────────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            Extract the first capture group

            >>> df.with_column("match", df["x"].str.extract(regex, 1)).collect()
            ╭─────────┬───────╮
            │ x       ┆ match │
            │ ---     ┆ ---   │
            │ Utf8    ┆ Utf8  │
            ╞═════════╪═══════╡
            │ 123-456 ┆ 1     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 789-012 ┆ 7     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ 345-678 ┆ 3     │
            ╰─────────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: The regex pattern to extract
            index: The index of the regex match group to extract

        Returns:
            Expression: a String expression with the extracted regex match

        See also:
            `extract_all`
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_extract(pattern_expr._expr, index))

    def extract_all(self, pattern: str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from all regex matches in each string in a string column.

        Notes:
            This expression always returns a list of strings.
            If index is 0, the entire match is returned. If the pattern does not match or the group does not exist, an empty list is returned.

        Example:
            >>> import daft
            >>> regex = r"(\d)(\d*)"
            >>> df = daft.from_pydict({"x": ["123-456", "789-012", "345-678"]})
            >>> df.with_column("match", df["x"].str.extract_all(regex)).collect()
            ╭─────────┬────────────╮
            │ x       ┆ match      │
            │ ---     ┆ ---        │
            │ Utf8    ┆ List[Utf8] │
            ╞═════════╪════════════╡
            │ 123-456 ┆ [123, 456] │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 789-012 ┆ [789, 012] │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 345-678 ┆ [345, 678] │
            ╰─────────┴────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            Extract the first capture group

            >>> df.with_column("match", df["x"].str.extract_all(regex, 1)).collect()
            ╭─────────┬────────────╮
            │ x       ┆ match      │
            │ ---     ┆ ---        │
            │ Utf8    ┆ List[Utf8] │
            ╞═════════╪════════════╡
            │ 123-456 ┆ [1, 4]     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 789-012 ┆ [7, 0]     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 345-678 ┆ [3, 6]     │
            ╰─────────┴────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: The regex pattern to extract
            index: The index of the regex match group to extract

        Returns:
            Expression: a List[Utf8] expression with the extracted regex matches

        See also:
            `extract`
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_extract_all(pattern_expr._expr, index))

    def replace(
        self,
        pattern: str | Expression,
        replacement: str | Expression,
        regex: bool = False,
    ) -> Expression:
        """Replaces all occurrences of a pattern in a string column with a replacement string. The pattern can be a literal string or a regex pattern.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"data": ["foo", "bar", "baz"]})
            >>> df.with_column("replace", df["data"].str.replace("ba", "123")).collect()
            ╭──────┬─────────╮
            │ data ┆ replace │
            │ ---  ┆ ---     │
            │ Utf8 ┆ Utf8    │
            ╞══════╪═════════╡
            │ foo  ┆ foo     │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ bar  ┆ 123r    │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ baz  ┆ 123z    │
            ╰──────┴─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            Replace with a regex pattern

            >>> import daft
            >>> df = daft.from_pydict({"data": ["foo", "fooo", "foooo"]})
            >>> df.with_column("replace", df["data"].str.replace(r"o+", "a", regex=True)).collect()
            ╭───────┬─────────╮
            │ data  ┆ replace │
            │ ---   ┆ ---     │
            │ Utf8  ┆ Utf8    │
            ╞═══════╪═════════╡
            │ foo   ┆ fa      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ fooo  ┆ fa      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ foooo ┆ fa      │
            ╰───────┴─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            pattern: The pattern to replace
            replacement: The replacement string
            regex: Whether the pattern is a regex pattern or an exact match. Defaults to False.

        Returns:
            Expression: a String expression with patterns replaced by the replacement string
        """
        pattern_expr = Expression._to_expression(pattern)
        replacement_expr = Expression._to_expression(replacement)
        return Expression._from_pyexpr(self._expr.utf8_replace(pattern_expr._expr, replacement_expr._expr, regex))

    def length(self) -> Expression:
        """Retrieves the length for a UTF-8 string column

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df = df.select(df["x"].str.length())
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
            │ 3      │
            ╰────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: an UInt64 expression with the length of each string
        """
        return Expression._from_pyexpr(self._expr.utf8_length())

    def lower(self) -> Expression:
        """Convert UTF-8 string to all lowercase

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["FOO", "BAR", "BAZ"]})
            >>> df = df.select(df["x"].str.lower())
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ foo  │
            ├╌╌╌╌╌╌┤
            │ bar  │
            ├╌╌╌╌╌╌┤
            │ baz  │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` lowercased
        """
        return Expression._from_pyexpr(self._expr.utf8_lower())

    def upper(self) -> Expression:
        """Convert UTF-8 string to all upper

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df = df.select(df["x"].str.upper())
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ FOO  │
            ├╌╌╌╌╌╌┤
            │ BAR  │
            ├╌╌╌╌╌╌┤
            │ BAZ  │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` uppercased
        """
        return Expression._from_pyexpr(self._expr.utf8_upper())

    def lstrip(self) -> Expression:
        """Strip whitespace from the left side of a UTF-8 string

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "  baz"]})
            >>> df = df.select(df["x"].str.lstrip())
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ foo  │
            ├╌╌╌╌╌╌┤
            │ bar  │
            ├╌╌╌╌╌╌┤
            │ baz  │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` with leading whitespace stripped
        """
        return Expression._from_pyexpr(self._expr.utf8_lstrip())

    def rstrip(self) -> Expression:
        """Strip whitespace from the right side of a UTF-8 string

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz   "]})
            >>> df = df.select(df["x"].str.rstrip())
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ foo  │
            ├╌╌╌╌╌╌┤
            │ bar  │
            ├╌╌╌╌╌╌┤
            │ baz  │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` with trailing whitespace stripped
        """
        return Expression._from_pyexpr(self._expr.utf8_rstrip())

    def reverse(self) -> Expression:
        """Reverse a UTF-8 string

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df = df.select(df["x"].str.reverse())
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ oof  │
            ├╌╌╌╌╌╌┤
            │ rab  │
            ├╌╌╌╌╌╌┤
            │ zab  │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` reversed
        """
        return Expression._from_pyexpr(self._expr.utf8_reverse())

    def capitalize(self) -> Expression:
        """Capitalize a UTF-8 string

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df = df.select(df["x"].str.capitalize())
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ Foo  │
            ├╌╌╌╌╌╌┤
            │ Bar  │
            ├╌╌╌╌╌╌┤
            │ Baz  │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` uppercased with the first character and lowercased the rest
        """
        return Expression._from_pyexpr(self._expr.utf8_capitalize())

    def left(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) left-most characters of each string

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.left(4))
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ daft │
            ├╌╌╌╌╌╌┤
            │ quer │
            ├╌╌╌╌╌╌┤
            │ engi │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is the `n` left-most characters of `self`
        """
        nchars_expr = Expression._to_expression(nchars)
        return Expression._from_pyexpr(self._expr.utf8_left(nchars_expr._expr))

    def right(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) right-most characters of each string

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "distributed", "engine"]})
            >>> df = df.select(df["x"].str.right(4))
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ daft │
            ├╌╌╌╌╌╌┤
            │ uted │
            ├╌╌╌╌╌╌┤
            │ gine │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is the `n` right-most characters of `self`
        """
        nchars_expr = Expression._to_expression(nchars)
        return Expression._from_pyexpr(self._expr.utf8_right(nchars_expr._expr))

    def find(self, substr: str | Expression) -> Expression:
        """Returns the index of the first occurrence of the substring in each string

        .. NOTE::
            The returned index is 0-based.
            If the substring is not found, -1 is returned.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query daft", "df_daft"]})
            >>> df = df.select(df["x"].str.find("daft"))
            >>> df.show()
            ╭───────╮
            │ x     │
            │ ---   │
            │ Int64 │
            ╞═══════╡
            │ 0     │
            ├╌╌╌╌╌╌╌┤
            │ 6     │
            ├╌╌╌╌╌╌╌┤
            │ 3     │
            ╰───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: an Int64 expression with the index of the first occurrence of the substring in each string
        """
        substr_expr = Expression._to_expression(substr)
        return Expression._from_pyexpr(self._expr.utf8_find(substr_expr._expr))

    def rpad(self, length: int | Expression, pad: str | Expression) -> Expression:
        """Right-pads each string by truncating or padding with the character

        .. NOTE::
            If the string is longer than the specified length, it will be truncated.
            The pad character must be a single character.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.rpad(6, "0"))
            >>> df.show()
            ╭────────╮
            │ x      │
            │ ---    │
            │ Utf8   │
            ╞════════╡
            │ daft00 │
            ├╌╌╌╌╌╌╌╌┤
            │ query0 │
            ├╌╌╌╌╌╌╌╌┤
            │ engine │
            ╰────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` truncated or right-padded with the pad character
        """
        length_expr = Expression._to_expression(length)
        pad_expr = Expression._to_expression(pad)
        return Expression._from_pyexpr(self._expr.utf8_rpad(length_expr._expr, pad_expr._expr))

    def lpad(self, length: int | Expression, pad: str | Expression) -> Expression:
        """Left-pads each string by truncating on the right or padding with the character

        .. NOTE::
            If the string is longer than the specified length, it will be truncated on the right.
            The pad character must be a single character.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.lpad(6, "0"))
            >>> df.show()
            ╭────────╮
            │ x      │
            │ ---    │
            │ Utf8   │
            ╞════════╡
            │ 00daft │
            ├╌╌╌╌╌╌╌╌┤
            │ 0query │
            ├╌╌╌╌╌╌╌╌┤
            │ engine │
            ╰────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` truncated or left-padded with the pad character
        """
        length_expr = Expression._to_expression(length)
        pad_expr = Expression._to_expression(pad)
        return Expression._from_pyexpr(self._expr.utf8_lpad(length_expr._expr, pad_expr._expr))

    def repeat(self, n: int | Expression) -> Expression:
        """Repeats each string n times

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.repeat(5))
            >>> df.show()
            ╭────────────────────────────────╮
            │ x                              │
            │ ---                            │
            │ Utf8                           │
            ╞════════════════════════════════╡
            │ daftdaftdaftdaftdaft           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ queryqueryqueryqueryquery      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ engineengineengineengineengin… │
            ╰────────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a String expression which is `self` repeated `n` times
        """
        n_expr = Expression._to_expression(n)
        return Expression._from_pyexpr(self._expr.utf8_repeat(n_expr._expr))

    def like(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given SQL LIKE pattern, case sensitive

        .. NOTE::
            Use % as a multiple-character wildcard or _ as a single-character wildcard.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.like("daf%"))
            >>> df.show()
            ╭─────────╮
            │ x       │
            │ ---     │
            │ Boolean │
            ╞═════════╡
            │ true    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_like(pattern_expr._expr))

    def ilike(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given SQL LIKE pattern, case insensitive

        .. NOTE::
            Use % as a multiple-character wildcard or _ as a single-character wildcard.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.ilike("%ft%"))
            >>> df.show()
            ╭─────────╮
            │ x       │
            │ ---     │
            │ Boolean │
            ╞═════════╡
            │ true    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ├╌╌╌╌╌╌╌╌╌┤
            │ false   │
            ╰─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_ilike(pattern_expr._expr))

    def substr(self, start: int | Expression, length: int | Expression | None = None) -> Expression:
        """Extract a substring from a string, starting at a specified index and extending for a given length.

        .. NOTE::
            If `length` is not provided, the substring will include all characters from `start` to the end of the string.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.substr(2,4))
            >>> df.show()
            ╭──────╮
            │ x    │
            │ ---  │
            │ Utf8 │
            ╞══════╡
            │ ft   │
            ├╌╌╌╌╌╌┤
            │ ery  │
            ├╌╌╌╌╌╌┤
            │ gine │
            ╰──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: A String expression representing the extracted substring.
        """
        start_expr = Expression._to_expression(start)
        length_expr = Expression._to_expression(length)
        return Expression._from_pyexpr(self._expr.utf8_substr(start_expr._expr, length_expr._expr))

    def to_date(self, format: str) -> Expression:
        """Converts a string to a date using the specified format

        .. NOTE::
            The format must be a valid date format string.
            See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["2021-01-01", "2021-01-02", None]})
            >>> df = df.with_column("date", df["x"].str.to_date("%Y-%m-%d"))
            >>> df.show()
            ╭────────────┬────────────╮
            │ x          ┆ date       │
            │ ---        ┆ ---        │
            │ Utf8       ┆ Date       │
            ╞════════════╪════════════╡
            │ 2021-01-01 ┆ 2021-01-01 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 ┆ 2021-01-02 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ None       ┆ None       │
            ╰────────────┴────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a Date expression which is parsed by given format
        """
        return Expression._from_pyexpr(self._expr.utf8_to_date(format))

    def to_datetime(self, format: str, timezone: str | None = None) -> Expression:
        """Converts a string to a datetime using the specified format and timezone

        .. NOTE::
            The format must be a valid datetime format string.
            See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["2021-01-01 00:00:00.123", "2021-01-02 12:30:00.456", None]})
            >>> df = df.with_column("datetime", df["x"].str.to_datetime("%Y-%m-%d %H:%M:%S%.3f"))
            >>> df.show()
            ╭─────────────────────────┬───────────────────────────────╮
            │ x                       ┆ datetime                      │
            │ ---                     ┆ ---                           │
            │ Utf8                    ┆ Timestamp(Milliseconds, None) │
            ╞═════════════════════════╪═══════════════════════════════╡
            │ 2021-01-01 00:00:00.123 ┆ 2021-01-01 00:00:00.123       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 12:30:00.456 ┆ 2021-01-02 12:30:00.456       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ None                    ┆ None                          │
            ╰─────────────────────────┴───────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

            If a timezone is provided, the datetime will be parsed in that timezone

            >>> df = daft.from_pydict({"x": ["2021-01-01 00:00:00.123 +0800", "2021-01-02 12:30:00.456 +0800", None]})
            >>> df = df.with_column("datetime", df["x"].str.to_datetime("%Y-%m-%d %H:%M:%S%.3f %z", timezone="Asia/Shanghai"))
            >>> df.show()
            ╭───────────────────────────────┬────────────────────────────────────────────────╮
            │ x                             ┆ datetime                                       │
            │ ---                           ┆ ---                                            │
            │ Utf8                          ┆ Timestamp(Milliseconds, Some("Asia/Shanghai")) │
            ╞═══════════════════════════════╪════════════════════════════════════════════════╡
            │ 2021-01-01 00:00:00.123 +0800 ┆ 2021-01-01 00:00:00.123 CST                    │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-02 12:30:00.456 +0800 ┆ 2021-01-02 12:30:00.456 CST                    │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ None                          ┆ None                                           │
            ╰───────────────────────────────┴────────────────────────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Returns:
            Expression: a DateTime expression which is parsed by given format and timezone
        """
        return Expression._from_pyexpr(self._expr.utf8_to_datetime(format, timezone))

    def normalize(
        self,
        *,
        remove_punct: bool = True,
        lowercase: bool = True,
        nfd_unicode: bool = True,
        white_space: bool = True,
    ):
        """Normalizes a string for more useful deduplication.

        .. NOTE::
            All processing options are on by default.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["hello world", "Hello, world!", "HELLO,   \\nWORLD!!!!"]})
            >>> df = df.with_column("normalized", df["x"].str.normalize())
            >>> df.show()
            ╭───────────────┬─────────────╮
            │ x             ┆ normalized  │
            │ ---           ┆ ---         │
            │ Utf8          ┆ Utf8        │
            ╞═══════════════╪═════════════╡
            │ hello world   ┆ hello world │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ Hello, world! ┆ hello world │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ HELLO,        ┆ hello world │
            │ WORLD!!!!     ┆             │
            ╰───────────────┴─────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            remove_punct: Whether to remove all punctuation (ASCII).
            lowercase: Whether to convert the string to lowercase.
            nfd_unicode: Whether to normalize and decompose Unicode characters according to NFD.
            white_space: Whether to normalize whitespace, replacing newlines etc with spaces and removing double spaces.

        Returns:
            Expression: a String expression which is normalized.
        """
        return Expression._from_pyexpr(self._expr.utf8_normalize(remove_punct, lowercase, nfd_unicode, white_space))

    def tokenize_encode(
        self,
        tokens_path: str,
        *,
        io_config: IOConfig | None = None,
        pattern: str | None = None,
        special_tokens: str | None = None,
        use_special_tokens: bool | None = None,
    ) -> Expression:
        """Encodes each string as a list of integer tokens using a tokenizer.

        Uses https://github.com/openai/tiktoken for tokenization.

        Supported built-in tokenizers: `cl100k_base`, `o200k_base`, `p50k_base`, `p50k_edit`, `r50k_base`. Also supports
        loading tokens from a file in tiktoken format.

        .. NOTE::
            If using this expression with Llama 3 tokens, note that Llama 3 does some extra preprocessing on
            strings in certain edge cases. This may result in slightly different encodings in these cases.

        Args:
            tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
            io_config (optional): IOConfig to use when accessing remote storage.
            pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
            special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.
            use_special_tokens (optional): Whether or not to parse special tokens included in input. Disabled by default. Automatically enabled if `special_tokens` is provided.

        Returns:
            Expression: An expression with the encodings of the strings as lists of unsigned 32-bit integers.
        """

        # if special tokens are passed in, enable using special tokens
        if use_special_tokens is None:
            use_special_tokens = special_tokens is not None

        return Expression._from_pyexpr(
            _tokenize_encode(
                self._expr,
                tokens_path,
                use_special_tokens,
                io_config,
                pattern,
                special_tokens,
            )
        )

    def tokenize_decode(
        self,
        tokens_path: str,
        *,
        io_config: IOConfig | None = None,
        pattern: str | None = None,
        special_tokens: str | None = None,
    ) -> Expression:
        """Decodes each list of integer tokens into a string using a tokenizer.

        Uses https://github.com/openai/tiktoken for tokenization.

        Supported built-in tokenizers: `cl100k_base`, `o200k_base`, `p50k_base`, `p50k_edit`, `r50k_base`. Also supports
        loading tokens from a file in tiktoken format.

        Args:
            tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
            io_config (optional): IOConfig to use when accessing remote storage.
            pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
            special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.

        Returns:
            Expression: An expression with decoded strings.
        """
        return Expression._from_pyexpr(_tokenize_decode(self._expr, tokens_path, io_config, pattern, special_tokens))

    def count_matches(
        self,
        patterns: Any,
        whole_words: bool = False,
        case_sensitive: bool = True,
    ):
        """
        Counts the number of times a pattern, or multiple patterns, appear in a string.

        .. NOTE::
            If a pattern is a substring of another pattern, the longest pattern is matched first.
            For example, in the string "hello world", with patterns "hello", "world", and "hello world",
            one match is counted for "hello world".

        If whole_words is true, then matches are only counted if they are whole words. This
        also applies to multi-word strings. For example, on the string "abc def", the strings
        "def" and "abc def" would be matched, but "bc de", "abc d", and "abc " (with the space)
        would not.

        If case_sensitive is false, then case will be ignored. This only applies to ASCII
        characters; unicode uppercase/lowercase will still be considered distinct.

        Args:
            patterns: A pattern or a list of patterns.
            whole_words: Whether to only match whole word(s). Defaults to false.
            case_sensitive: Whether the matching should be case sensitive. Defaults to true.
        """
        if isinstance(patterns, str):
            patterns = [patterns]
        if not isinstance(patterns, Expression):
            series = item_to_series("items", patterns)
            patterns = Expression._to_expression(series)

        return Expression._from_pyexpr(_utf8_count_matches(self._expr, patterns._expr, whole_words, case_sensitive))


class ExpressionListNamespace(ExpressionNamespace):
    def join(self, delimiter: str | Expression) -> Expression:
        """Joins every element of a list using the specified string delimiter

        Args:
            delimiter (str | Expression): the delimiter to use to join lists with

        Returns:
            Expression: a String expression which is every element of the list joined on the delimiter
        """
        delimiter_expr = Expression._to_expression(delimiter)
        return Expression._from_pyexpr(self._expr.list_join(delimiter_expr._expr))

    def count(self, mode: CountMode = CountMode.Valid) -> Expression:
        """Counts the number of elements in each list

        Args:
            mode: The mode to use for counting. Defaults to CountMode.Valid

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        return Expression._from_pyexpr(self._expr.list_count(mode))

    def lengths(self) -> Expression:
        """Gets the length of each list

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        return Expression._from_pyexpr(self._expr.list_count(CountMode.All))

    def get(self, idx: int | Expression, default: object = None) -> Expression:
        """Gets the element at an index in each list

        Args:
            idx: index or indices to retrieve from each list
            default: the default value if the specified index is out of bounds

        Returns:
            Expression: an expression with the type of the list values
        """
        idx_expr = Expression._to_expression(idx)
        default_expr = lit(default)
        return Expression._from_pyexpr(self._expr.list_get(idx_expr._expr, default_expr._expr))

    def slice(self, start: int | Expression, end: int | Expression) -> Expression:
        """Gets a subset of each list

        Args:
            start: index or column of indices. The slice will include elements starting from this index. If `start` is negative, it represents an offset from the end of the list
            end: index or column of indices. The slice will not include elements from this index onwards. If `end` is negative, it represents an offset from the end of the list

        Returns:
            Expression: an expression with a list of the type of the list values
        """
        start_expr = Expression._to_expression(start)
        end_expr = Expression._to_expression(end)
        return Expression._from_pyexpr(self._expr.list_slice(start_expr._expr, end_expr._expr))

    def chunk(self, size: int) -> Expression:
        """Splits each list into chunks of the given size

        Args:
            size: size of chunks to split the list into. Must be greater than 0
        Returns:
            Expression: an expression with lists of fixed size lists of the type of the list values
        """
        if not (isinstance(size, int) and size > 0):
            raise ValueError(f"Invalid value for `size`: {size}")
        return Expression._from_pyexpr(self._expr.list_chunk(size))

    def sum(self) -> Expression:
        """Sums each list. Empty lists and lists with all nulls yield null.

        Returns:
            Expression: an expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_sum())

    def mean(self) -> Expression:
        """Calculates the mean of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_mean())

    def min(self) -> Expression:
        """Calculates the minimum of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_min())

    def max(self) -> Expression:
        """Calculates the maximum of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return Expression._from_pyexpr(self._expr.list_max())


class ExpressionStructNamespace(ExpressionNamespace):
    def get(self, name: str) -> Expression:
        """Retrieves one field from a struct column

        Args:
            name: the name of the field to retrieve

        Returns:
            Expression: the field expression
        """
        return Expression._from_pyexpr(self._expr.struct_get(name))


class ExpressionMapNamespace(ExpressionNamespace):
    def get(self, key: Expression) -> Expression:
        """Retrieves the value for a key in a map column

        Example:
            >>> import pyarrow as pa
            >>> import daft
            >>> pa_array = pa.array([[("a", 1)],[],[("b", 2)]], type=pa.map_(pa.string(), pa.int64()))
            >>> df = daft.from_arrow(pa.table({"map_col": pa_array}))
            >>> df = df.with_column("a", df["map_col"].map.get("a"))
            >>> df.show()
            ╭──────────────────────────────────────┬───────╮
            │ map_col                              ┆ a     │
            │ ---                                  ┆ ---   │
            │ Map[Struct[key: Utf8, value: Int64]] ┆ Int64 │
            ╞══════════════════════════════════════╪═══════╡
            │ [{key: a,                            ┆ 1     │
            │ value: 1,                            ┆       │
            │ }]                                   ┆       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ []                                   ┆ None  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
            │ [{key: b,                            ┆ None  │
            │ value: 2,                            ┆       │
            │ }]                                   ┆       │
            ╰──────────────────────────────────────┴───────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            key: the key to retrieve

        Returns:
            Expression: the value expression
        """
        key_expr = Expression._to_expression(key)
        return Expression._from_pyexpr(self._expr.map_get(key_expr._expr))


class ExpressionsProjection(Iterable[Expression]):
    """A collection of Expressions that can be projected onto a Table to produce another Table

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
        """Returns a map of {output_name: input_name} for all expressions that are just no-ops/aliases of an existing input"""
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


class ExpressionImageNamespace(ExpressionNamespace):
    """Expression operations for image columns."""

    def decode(
        self,
        on_error: Literal["raise"] | Literal["null"] = "raise",
        mode: str | ImageMode | None = None,
    ) -> Expression:
        """
        Decodes the binary data in this column into images.

        This can only be applied to binary columns that contain encoded images (e.g. PNG, JPEG, etc.)

        Args:
            on_error: Whether to raise when encountering an error, or log a warning and return a null
            mode: What mode to convert the images into before storing it in the column. This may prevent
                errors relating to unsupported types.

        Returns:
            Expression: An Image expression represnting an image column.
        """
        raise_on_error = False
        if on_error == "raise":
            raise_on_error = True
        elif on_error == "null":
            raise_on_error = False
        else:
            raise NotImplementedError(f"Unimplemented on_error option: {on_error}.")

        if mode is not None:
            if isinstance(mode, str):
                mode = ImageMode.from_mode_string(mode.upper())
            if not isinstance(mode, ImageMode):
                raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        return Expression._from_pyexpr(self._expr.image_decode(raise_error_on_failure=raise_on_error, mode=mode))

    def encode(self, image_format: str | ImageFormat) -> Expression:
        """
        Encode an image column as the provided image file format, returning a binary column
        of encoded bytes.

        Args:
            image_format: The image file format into which the images will be encoded.

        Returns:
            Expression: A Binary expression representing a binary column of encoded image bytes.
        """
        if isinstance(image_format, str):
            image_format = ImageFormat.from_format_string(image_format.upper())
        if not isinstance(image_format, ImageFormat):
            raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
        return Expression._from_pyexpr(self._expr.image_encode(image_format))

    def resize(self, w: int, h: int) -> Expression:
        """
        Resize image into the provided width and height.

        Args:
            w: Desired width of the resized image.
            h: Desired height of the resized image.

        Returns:
            Expression: An Image expression representing an image column of the resized images.
        """
        if not isinstance(w, int):
            raise TypeError(f"expected int for w but got {type(w)}")
        if not isinstance(h, int):
            raise TypeError(f"expected int for h but got {type(h)}")
        return Expression._from_pyexpr(self._expr.image_resize(w, h))

    def crop(self, bbox: tuple[int, int, int, int] | Expression) -> Expression:
        """
        Crops images with the provided bounding box

        Args:
            bbox (tuple[float, float, float, float] | Expression): Either a tuple of (x, y, width, height)
                parameters for cropping, or a List Expression where each element is a length 4 List
                which represents the bounding box for the crop

        Returns:
            Expression: An Image expression representing the cropped image
        """
        if not isinstance(bbox, Expression):
            if len(bbox) != 4 or not all([isinstance(x, int) for x in bbox]):
                raise ValueError(
                    f"Expected `bbox` to be either a tuple of 4 ints or an Expression but received: {bbox}"
                )
            bbox = Expression._to_expression(bbox).cast(DataType.fixed_size_list(DataType.uint64(), 4))
        assert isinstance(bbox, Expression)
        return Expression._from_pyexpr(self._expr.image_crop(bbox._expr))

    def to_mode(self, mode: str | ImageMode) -> Expression:
        if isinstance(mode, str):
            mode = ImageMode.from_mode_string(mode.upper())
        if not isinstance(mode, ImageMode):
            raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        return Expression._from_pyexpr(self._expr.image_to_mode(mode))


class ExpressionPartitioningNamespace(ExpressionNamespace):
    def days(self) -> Expression:
        """Partitioning Transform that returns the number of days since epoch (1970-01-01)

        Returns:
            Expression: Date Type Expression
        """
        return Expression._from_pyexpr(self._expr.partitioning_days())

    def hours(self) -> Expression:
        """Partitioning Transform that returns the number of hours since epoch (1970-01-01)

        Returns:
            Expression: Int32 Expression in hours
        """
        return Expression._from_pyexpr(self._expr.partitioning_hours())

    def months(self) -> Expression:
        """Partitioning Transform that returns the number of months since epoch (1970-01-01)

        Returns:
            Expression: Int32 Expression in months
        """

        return Expression._from_pyexpr(self._expr.partitioning_months())

    def years(self) -> Expression:
        """Partitioning Transform that returns the number of years since epoch (1970-01-01)

        Returns:
            Expression: Int32 Expression in years
        """

        return Expression._from_pyexpr(self._expr.partitioning_years())

    def iceberg_bucket(self, n: int) -> Expression:
        """Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86
        https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements

        Args:
            n (int): Number of buckets

        Returns:
            Expression: Int32 Expression with the Hash Bucket
        """
        return Expression._from_pyexpr(self._expr.partitioning_iceberg_bucket(n))

    def iceberg_truncate(self, w: int) -> Expression:
        """Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.
        https://iceberg.apache.org/spec/#truncate-transform-details

        Args:
            w (int): width of the truncation

        Returns:
            Expression: Expression of the Same Type of the input
        """
        return Expression._from_pyexpr(self._expr.partitioning_iceberg_truncate(w))


class ExpressionJsonNamespace(ExpressionNamespace):
    def query(self, jq_query: str) -> Expression:
        """Query JSON data in a column using a JQ-style filter https://jqlang.github.io/jq/manual/
        This expression uses jaq as the underlying executor, see https://github.com/01mf02/jaq for the full list of supported filters.

        Example:
            >>> import daft
            >>> df = daft.from_pydict({"col": ['{"a": 1}', '{"a": 2}', '{"a": 3}']})
            >>> df.with_column("res", df["col"].json.query(".a")).collect()
            ╭──────────┬──────╮
            │ col      ┆ res  │
            │ ---      ┆ ---  │
            │ Utf8     ┆ Utf8 │
            ╞══════════╪══════╡
            │ {"a": 1} ┆ 1    │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
            │ {"a": 2} ┆ 2    │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
            │ {"a": 3} ┆ 3    │
            ╰──────────┴──────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        Args:
            jq_query (str): JQ query string

        Returns:
            Expression: Expression representing the result of the JQ query as a column of JSON-compatible strings
        """

        return Expression._from_pyexpr(self._expr.json_query(jq_query))


class ExpressionEmbeddingNamespace(ExpressionNamespace):
    def cosine_distance(self, other: Expression) -> Expression:
        """Compute the cosine distance between two embeddings"""
        return Expression._from_pyexpr(native.cosine_distance(self._expr, other._expr))
