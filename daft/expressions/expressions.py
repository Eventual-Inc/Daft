from __future__ import annotations

import builtins
import math
import warnings
from collections.abc import Collection, Iterable, Iterator
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    TypeVar,
    overload,
)

import daft.daft as native
from daft import context
from daft.daft import (
    CountMode,
    ImageFormat,
    ImageMode,
    ResourceRequest,
    initialize_udfs,
    resolved_col,
    sql_datatype,
    unresolved_col,
)
from daft.daft import PyExpr as _PyExpr
from daft.daft import date_lit as _date_lit
from daft.daft import decimal_lit as _decimal_lit
from daft.daft import duration_lit as _duration_lit
from daft.daft import lit as _lit
from daft.daft import series_lit as _series_lit
from daft.daft import time_lit as _time_lit
from daft.daft import timestamp_lit as _timestamp_lit
from daft.daft import udf as _udf
from daft.datatype import DataType, DataTypeLike, TimeUnit
from daft.dependencies import pa, pc
from daft.expressions.testing import expr_structurally_equal
from daft.logical.schema import Field, Schema
from daft.series import Series, item_to_series

if TYPE_CHECKING:
    from daft.io import IOConfig
    from daft.udf import BoundUDFArgs, InitArgsType, UninitializedUdf
    from daft.window import Window

    EncodingCodec = Literal["deflate", "gzip", "gz", "utf-8", "utf8" "zlib"]


def lit(value: object) -> Expression:
    """Creates an Expression representing a column with every value set to the provided value.

    Args:
        val: value of column

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
    elif isinstance(value, timedelta):
        # pyo3 timedelta (PyDelta) is not available when running in abi3 mode, workaround
        pa_duration = pa.scalar(value)
        i64_value = pa_duration.cast(pa.int64()).as_py()
        time_unit = TimeUnit.from_str(pa_duration.type.unit)._timeunit
        lit_value = _duration_lit(i64_value, time_unit)
    elif isinstance(value, Decimal):
        sign, digits, exponent = value.as_tuple()
        assert isinstance(exponent, int)
        lit_value = _decimal_lit(sign == 1, digits, exponent)
    elif isinstance(value, Series):
        agg_listed = value._series.agg_list()
        lit_value = _series_lit(agg_listed)
    else:
        lit_value = _lit(value)
    return Expression._from_pyexpr(lit_value)


def element() -> Expression:
    """Creates an expression referring to an elementwise list operation.

    This is used to create an expression that operates on each element of a list column.

    If used outside of a list column, it will raise an error.
    """
    return col("")


def col(name: str) -> Expression:
    """Creates an Expression referring to the column with the provided name.

    See [Column Wildcards](https://docs.getdaft.io/en/stable/core_concepts/#selecting-columns-using-wildcards) for details on wildcards.

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


def list_(*items: Expression | str) -> Expression:
    """Constructs a list from the item expressions.

    Args:
        *items (Union[Expression, str]): item expressions to construct the list

    Returns:
        Expression: Expression representing the constructed list

    Examples:
        >>> import daft
        >>> df = daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]})
        >>> df = df.select(daft.list_("x", "y").alias("fwd"), daft.list_("y", "x").alias("rev"))
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
    return Expression._from_pyexpr(native.list_([col(i)._expr if isinstance(i, str) else i._expr for i in items]))


def struct(*fields: Expression | str) -> Expression:
    """Constructs a struct from the input field expressions.

    Args:
        inputs: Expressions to be converted into struct fields.

    Returns:
        An expression for a struct column with the input columns as its fields.

    Examples:
        >>> import daft
        >>> from daft import col
        >>> df = daft.from_pydict({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        >>> df.select(daft.struct(col("a") * 2, col("b"))).show()
        ╭───────────────────────────╮
        │ struct                    │
        │ ---                       │
        │ Struct[a: Int64, b: Utf8] │
        ╞═══════════════════════════╡
        │ {a: 2,                    │
        │ b: a,                     │
        │ }                         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {a: 4,                    │
        │ b: b,                     │
        │ }                         │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ {a: 6,                    │
        │ b: c,                     │
        │ }                         │
        ╰───────────────────────────╯
        <BLANKLINE>
        (Showing first 3 of 3 rows)

    """
    pyinputs = []
    for field in fields:
        if isinstance(field, Expression):
            pyinputs.append(field._expr)
        elif isinstance(field, str):
            pyinputs.append(col(field)._expr)
        else:
            raise TypeError("expected Expression or str as input for struct()")
    f = native.get_function_from_registry("struct")
    return Expression._from_pyexpr(f(*pyinputs))


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


def coalesce(*args: Expression) -> Expression:
    """Returns the first non-null value in a list of expressions. If all inputs are null, returns null.

    Args:
        *args: Two or more expressions to coalesce

    Returns:
        Expression: Expression containing first non-null value encountered when evaluating arguments in order

    Examples:
        >>> import daft
        >>> df = daft.from_pydict({"x": [1, None, 3], "y": [None, 2, None]})
        >>> df = df.with_column("first_valid", daft.coalesce(df["x"], df["y"]))
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
    f = native.get_function_from_registry("coalesce")

    return Expression._from_pyexpr(f(*[arg._expr for arg in args]))


class Expression:
    _expr: _PyExpr = None  # type: ignore

    def __init__(self) -> None:
        raise NotImplementedError("We do not support creating a Expression via __init__ ")

    @property
    def str(self) -> ExpressionStringNamespace:
        """Access methods that work on columns of strings."""
        return ExpressionStringNamespace.from_expression(self)

    @property
    def dt(self) -> ExpressionDatetimeNamespace:
        """Access methods that work on columns of datetimes."""
        return ExpressionDatetimeNamespace.from_expression(self)

    @property
    def embedding(self) -> ExpressionEmbeddingNamespace:
        """Access methods that work on columns of embeddings."""
        return ExpressionEmbeddingNamespace.from_expression(self)

    @property
    def float(self) -> ExpressionFloatNamespace:
        """Access methods that work on columns of floats."""
        return ExpressionFloatNamespace.from_expression(self)

    @property
    def url(self) -> ExpressionUrlNamespace:
        """Access methods that work on columns of URLs."""
        return ExpressionUrlNamespace.from_expression(self)

    @property
    def list(self) -> ExpressionListNamespace:
        """Access methods that work on columns of lists."""
        return ExpressionListNamespace.from_expression(self)

    @property
    def struct(self) -> ExpressionStructNamespace:
        """Access methods that work on columns of structs."""
        return ExpressionStructNamespace.from_expression(self)

    @property
    def map(self) -> ExpressionMapNamespace:
        """Access methods that work on columns of maps."""
        return ExpressionMapNamespace.from_expression(self)

    @property
    def image(self) -> ExpressionImageNamespace:
        """Access methods that work on columns of images."""
        return ExpressionImageNamespace.from_expression(self)

    @property
    def partitioning(self) -> ExpressionPartitioningNamespace:
        """Access methods that support partitioning operators."""
        return ExpressionPartitioningNamespace.from_expression(self)

    @property
    def json(self) -> ExpressionJsonNamespace:
        """Access methods that work on columns of json."""
        return ExpressionJsonNamespace.from_expression(self)

    @property
    def binary(self) -> ExpressionBinaryNamespace:
        """Access binary string operations for this expression.

        Returns:
            ExpressionBinaryNamespace: A namespace containing binary string operations
        """
        return ExpressionBinaryNamespace.from_expression(self)

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

    def to_arrow_expr(self) -> pc.Expression:
        """Returns this expression as a pyarrow.compute.Expression for integrations with other systems."""
        from daft.expressions.visitor import _PyArrowExpressionVisitor

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
            )
        )

    @staticmethod
    def to_struct(*fields: Expression | builtins.str) -> Expression:
        """Constructs a struct from the input field expressions.

        Renamed to 'struct' in https://github.com/Eventual-Inc/Daft/pull/3755.
        """
        warnings.warn(
            "This function will be deprecated from Daft version >= 0.4.4!  Instead, please use 'struct'",
            category=DeprecationWarning,
        )
        return struct(*fields)

    def __bool__(self) -> bool:
        raise ValueError(
            "Expressions don't have a truth value. "
            "If you used Python keywords `and` `not` `or` on an expression, use `&` `~` `|` instead."
        )

    def __abs__(self) -> Expression:
        """Absolute of a numeric expression."""
        return self.abs()

    def abs(self) -> Expression:
        """Absolute of a numeric expression."""
        f = native.get_function_from_registry("abs")
        return Expression._from_pyexpr(f(self._expr))

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

    def eq_null_safe(self, other: Expression) -> Expression:
        """Performs a null-safe equality comparison between two expressions.

        Unlike regular equality (==), null-safe equality (<=> or IS NOT DISTINCT FROM):
        - Returns True when comparing NULL <=> NULL
        - Returns False when comparing NULL <=> any_value
        - Behaves like regular equality for non-NULL values

        Args:
            other: The expression to compare with

        Returns:
            Expression: A boolean expression indicating if the values are equal
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr.eq_null_safe(expr._expr))

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

    def __getitem__(self, key: builtins.str | int) -> Expression:
        """Syntactic sugar for `Expression.list.get` and `Expression.struct.get`.

        Examples:
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

        Tip: See Also
            [list.get](https://docs.getdaft.io/en/stable/api/expressions/#daft.expressions.expressions.ExpressionListNamespace.get) and [struct.get](https://docs.getdaft.io/en/stable/api/expressions/#daft.expressions.expressions.ExpressionStructNamespace.get)

        """
        if isinstance(key, int):
            return self.list.get(key)
        elif isinstance(key, str):
            return self.struct.get(key)
        else:
            raise TypeError(
                f"Argument {key} of type {type(key)} is not supported in Expression.__getitem__. Only int and string types are supported."
            )

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
        | Embedding          | Y    | N       | N        | N      | N          | n      | N      | N                 | N     | Y                 | N         | Y      | Y                  | Y      | Y    | Y               | N      | N   | N         | N    | N    | N        |
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
            - If a string is provided, it will use the sql engine to parse the string into a data type. See the [SQL Reference](https://docs.getdaft.io/en/stable/sql/datatypes/) for supported datatypes.
            - a python `type` can also be provided, in which case the corresponding Daft data type will be used.

        Examples:
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

            Example with python type and sql types:
            >>> import daft
            >>> df = daft.from_pydict({"a": [1, 2, 3]})
            >>> df = df.select(
            ...     daft.col("a").cast(str).alias("str"),
            ...     daft.col("a").cast(int).alias("int"),
            ...     daft.col("a").cast(float).alias("float"),
            ...     daft.col("a").cast("string").alias("sql_string"),
            ...     daft.col("a").cast("int").alias("sql_int"),
            ...     daft.col("a").cast("tinyint").alias("sql_tinyint"),
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
            dtype = DataType._from_pydatatype(sql_datatype(dtype))
        else:
            assert isinstance(dtype, (DataType, type))
            dtype = DataType._infer_type(dtype)
        expr = self._expr.cast(dtype._dtype)
        return Expression._from_pyexpr(expr)

    def ceil(self) -> Expression:
        """The ceiling of a numeric expression."""
        f = native.get_function_from_registry("ceil")
        return Expression._from_pyexpr(f(self._expr))

    def floor(self) -> Expression:
        """The floor of a numeric expression."""
        f = native.get_function_from_registry("floor")
        return Expression._from_pyexpr(f(self._expr))

    def clip(self, min: Expression | None = None, max: Expression | None = None) -> Expression:
        """Clips an expression to the given minimum and maximum values.

        Args:
            min: Minimum value to clip to. If None (or column value is Null), no lower clipping is applied.
            max: Maximum value to clip to. If None (or column value is Null), no upper clipping is applied.

        """
        min_expr = Expression._to_expression(min)._expr
        max_expr = Expression._to_expression(max)._expr
        f = native.get_function_from_registry("clip")
        return Expression._from_pyexpr(f(self._expr, min_expr, max_expr))

    def sign(self) -> Expression:
        """The sign of a numeric expression."""
        f = native.get_function_from_registry("sign")
        return Expression._from_pyexpr(f(self._expr))

    def signum(self) -> Expression:
        """The signum of a numeric expression."""
        f = native.get_function_from_registry("sign")
        return Expression._from_pyexpr(f(self._expr))

    def negate(self) -> Expression:
        """The negative of a numeric expression."""
        f = native.get_function_from_registry("negative")
        return Expression._from_pyexpr(f(self._expr))

    def negative(self) -> Expression:
        """The negative of a numeric expression."""
        f = native.get_function_from_registry("negative")
        return Expression._from_pyexpr(f(self._expr))

    def round(self, decimals: int | Expression = 0) -> Expression:
        """The round of a numeric expression.

        Args:
            decimals: number of decimal places to round to. Defaults to 0.
        """
        assert isinstance(decimals, int)
        f = native.get_function_from_registry("round")
        decimals_expr = Expression._to_expression(decimals)._expr
        return Expression._from_pyexpr(f(self._expr, decimals=decimals_expr))

    def sqrt(self) -> Expression:
        """The square root of a numeric expression."""
        f = native.get_function_from_registry("sqrt")
        return Expression._from_pyexpr(f(self._expr))

    def cbrt(self) -> Expression:
        """The cube root of a numeric expression."""
        f = native.get_function_from_registry("cbrt")
        return Expression._from_pyexpr(f(self._expr))

    def sin(self) -> Expression:
        """The elementwise sine of a numeric expression."""
        f = native.get_function_from_registry("sin")
        return Expression._from_pyexpr(f(self._expr))

    def cos(self) -> Expression:
        """The elementwise cosine of a numeric expression."""
        f = native.get_function_from_registry("cos")
        return Expression._from_pyexpr(f(self._expr))

    def tan(self) -> Expression:
        """The elementwise tangent of a numeric expression."""
        f = native.get_function_from_registry("tan")
        return Expression._from_pyexpr(f(self._expr))

    def csc(self) -> Expression:
        """The elementwise cosecant of a numeric expression."""
        f = native.get_function_from_registry("csc")
        return Expression._from_pyexpr(f(self._expr))

    def sec(self) -> Expression:
        """The elementwise secant of a numeric expression."""
        f = native.get_function_from_registry("sec")
        return Expression._from_pyexpr(f(self._expr))

    def cot(self) -> Expression:
        """The elementwise cotangent of a numeric expression."""
        f = native.get_function_from_registry("cot")
        return Expression._from_pyexpr(f(self._expr))

    def sinh(self) -> Expression:
        """The elementwise hyperbolic sine of a numeric expression."""
        f = native.get_function_from_registry("sinh")
        return Expression._from_pyexpr(f(self._expr))

    def cosh(self) -> Expression:
        """The elementwise hyperbolic cosine of a numeric expression."""
        f = native.get_function_from_registry("cosh")
        return Expression._from_pyexpr(f(self._expr))

    def tanh(self) -> Expression:
        """The elementwise hyperbolic tangent of a numeric expression."""
        f = native.get_function_from_registry("tanh")
        return Expression._from_pyexpr(f(self._expr))

    def arcsin(self) -> Expression:
        """The elementwise arc sine of a numeric expression."""
        f = native.get_function_from_registry("arcsin")
        return Expression._from_pyexpr(f(self._expr))

    def arccos(self) -> Expression:
        """The elementwise arc cosine of a numeric expression."""
        f = native.get_function_from_registry("arccos")
        return Expression._from_pyexpr(f(self._expr))

    def arctan(self) -> Expression:
        """The elementwise arc tangent of a numeric expression."""
        f = native.get_function_from_registry("arctan")
        return Expression._from_pyexpr(f(self._expr))

    def arctan2(self, other: Expression) -> Expression:
        """Calculates the four quadrant arctangent of coordinates (y, x), in radians.

        * ``x = 0``, ``y = 0``: ``0``
        * ``x >= 0``: ``[-pi/2, pi/2]``
        * ``y >= 0``: ``(pi/2, pi]``
        * ``y < 0``: ``(-pi, -pi/2)``
        """
        expr = Expression._to_expression(other)
        f = native.get_function_from_registry("arctan2")
        return Expression._from_pyexpr(f(self._expr, expr._expr))

    def arctanh(self) -> Expression:
        """The elementwise inverse hyperbolic tangent of a numeric expression."""
        f = native.get_function_from_registry("arctanh")
        return Expression._from_pyexpr(f(self._expr))

    def arccosh(self) -> Expression:
        """The elementwise inverse hyperbolic cosine of a numeric expression."""
        f = native.get_function_from_registry("arccosh")
        return Expression._from_pyexpr(f(self._expr))

    def arcsinh(self) -> Expression:
        """The elementwise inverse hyperbolic sine of a numeric expression."""
        f = native.get_function_from_registry("arcsinh")
        return Expression._from_pyexpr(f(self._expr))

    def radians(self) -> Expression:
        """The elementwise radians of a numeric expression."""
        f = native.get_function_from_registry("radians")
        return Expression._from_pyexpr(f(self._expr))

    def degrees(self) -> Expression:
        """The elementwise degrees of a numeric expression."""
        f = native.get_function_from_registry("degrees")
        return Expression._from_pyexpr(f(self._expr))

    def log2(self) -> Expression:
        """The elementwise log base 2 of a numeric expression."""
        f = native.get_function_from_registry("log2")
        return Expression._from_pyexpr(f(self._expr))

    def log10(self) -> Expression:
        """The elementwise log base 10 of a numeric expression."""
        f = native.get_function_from_registry("log10")
        return Expression._from_pyexpr(f(self._expr))

    def log(self, base: float = math.e) -> Expression:  # type: ignore
        """The elementwise log with given base, of a numeric expression.

        Args:
            base: The base of the logarithm. Defaults to e.
        """
        assert isinstance(base, (int, float)), f"base must be an int or float, but {type(base)} was provided."
        base = lit(base)
        f = native.get_function_from_registry("log")
        expr = f(self._expr, base._expr)
        return Expression._from_pyexpr(expr)

    def ln(self) -> Expression:
        """The elementwise natural log of a numeric expression."""
        f = native.get_function_from_registry("ln")
        return Expression._from_pyexpr(f(self._expr))

    def log1p(self) -> Expression:
        """The ln(self + 1) of a numeric expression."""
        f = native.get_function_from_registry("log1p")
        return Expression._from_pyexpr(f(self._expr))

    def exp(self) -> Expression:
        """The e^self of a numeric expression."""
        f = native.get_function_from_registry("exp")
        return Expression._from_pyexpr(f(self._expr))

    def expm1(self) -> Expression:
        """The e^self - 1 of a numeric expression."""
        f = native.get_function_from_registry("expm1")
        return Expression._from_pyexpr(f(self._expr))

    def bitwise_and(self, other: Expression) -> Expression:
        """Bitwise AND of two integer expressions."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr & expr._expr)

    def bitwise_or(self, other: Expression) -> Expression:
        """Bitwise OR of two integer expressions."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr | expr._expr)

    def bitwise_xor(self, other: Expression) -> Expression:
        """Bitwise XOR of two integer expressions."""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr ^ expr._expr)

    def shift_left(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the left (``expr << other``).

        Args:
            other: The number of bits to shift the expression to the left
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr << expr._expr)

    def shift_right(self, other: Expression) -> Expression:
        """Shifts the bits of an integer expression to the right (``expr >> other``).

        .. NOTE::
            For unsigned integers, this expression perform a logical right shift.
            For signed integers, this expression perform an arithmetic right shift.

        Args:
            other: The number of bits to shift the expression to the right
        """
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr >> expr._expr)

    def count(self, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid) -> Expression:
        """Counts the number of values in the expression.

        Args:
            mode: A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".
        """
        if isinstance(mode, builtins.str):
            mode = CountMode.from_count_mode_str(mode)
        expr = self._expr.count(mode)
        return Expression._from_pyexpr(expr)

    def count_distinct(self) -> Expression:
        expr = self._expr.count_distinct()
        return Expression._from_pyexpr(expr)

    def sum(self) -> Expression:
        """Calculates the sum of the values in the expression."""
        expr = self._expr.sum()
        return Expression._from_pyexpr(expr)

    def approx_count_distinct(self) -> Expression:
        """Calculates the approximate number of non-`NULL` distinct values in the expression.

        Approximation is performed using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm.

        Examples:
            A global calculation of approximate distinct values in a non-NULL column:

            >>> import daft
            >>> df = daft.from_pydict({"values": [1, 2, 3, None]})
            >>> df = df.agg(
            ...     df["values"].approx_count_distinct().alias("distinct_values"),
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
        expr = self._expr.approx_count_distinct()
        return Expression._from_pyexpr(expr)

    def approx_percentiles(self, percentiles: builtins.float | builtins.list[builtins.float]) -> Expression:
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

            >>> df = daft.from_pydict({"class": ["a", "a", "a", "b", "c"], "scores": [1, 2, 3, 1, None]})
            >>> df = (
            ...     df.groupby("class")
            ...     .agg(
            ...         df["scores"].approx_percentiles(0.5).alias("approx_median_score"),
            ...         df["scores"].approx_percentiles([0.25, 0.5, 0.75]).alias("approx_percentiles_scores"),
            ...     )
            ...     .sort("class")
            ... )
            >>> df.show()
            ╭───────┬─────────────────────┬────────────────────────────────╮
            │ class ┆ approx_median_score ┆ approx_percentiles_scores      │
            │ ---   ┆ ---                 ┆ ---                            │
            │ Utf8  ┆ Float64             ┆ FixedSizeList[Float64; 3]      │
            ╞═══════╪═════════════════════╪════════════════════════════════╡
            │ a     ┆ 1.993661701417351   ┆ [0.9900000000000001, 1.993661… │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b     ┆ 0.9900000000000001  ┆ [0.9900000000000001, 0.990000… │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ c     ┆ None                ┆ None                           │
            ╰───────┴─────────────────────┴────────────────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        expr = self._expr.approx_percentiles(percentiles)
        return Expression._from_pyexpr(expr)

    def mean(self) -> Expression:
        """Calculates the mean of the values in the expression."""
        expr = self._expr.mean()
        return Expression._from_pyexpr(expr)

    def stddev(self) -> Expression:
        """Calculates the standard deviation of the values in the expression."""
        expr = self._expr.stddev()
        return Expression._from_pyexpr(expr)

    def min(self) -> Expression:
        """Calculates the minimum value in the expression."""
        expr = self._expr.min()
        return Expression._from_pyexpr(expr)

    def max(self) -> Expression:
        """Calculates the maximum value in the expression."""
        expr = self._expr.max()
        return Expression._from_pyexpr(expr)

    def bool_and(self) -> Expression:
        """Calculates the boolean AND of all values in a list.

        For each list:
        - Returns True if all non-null values are True
        - Returns False if any non-null value is False
        - Returns null if the list is empty or contains only null values

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"values": [[True, True], [True, False], [None, None], []]})
            >>> df.with_column("result", df["values"].list.bool_and()).collect()
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
        expr = self._expr.bool_and()
        return Expression._from_pyexpr(expr)

    def bool_or(self) -> Expression:
        """Calculates the boolean OR of all values in a list.

        For each list:
        - Returns True if any non-null value is True
        - Returns False if all non-null values are False
        - Returns null if the list is empty or contains only null values

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"values": [[True, False], [False, False], [None, None], []]})
            >>> df.with_column("result", df["values"].list.bool_or()).collect()
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
        expr = self._expr.bool_or()
        return Expression._from_pyexpr(expr)

    def any_value(self, ignore_nulls: bool = False) -> Expression:
        """Returns any value in the expression.

        Args:
            ignore_nulls: whether to ignore null values when selecting the value. Defaults to False.
        """
        expr = self._expr.any_value(ignore_nulls)
        return Expression._from_pyexpr(expr)

    def skew(self) -> Expression:
        """Calculates the skewness of the values from the expression."""
        expr = self._expr.skew()
        return Expression._from_pyexpr(expr)

    def agg_list(self) -> Expression:
        """Aggregates the values in the expression into a list."""
        expr = self._expr.agg_list()
        return Expression._from_pyexpr(expr)

    def agg_set(self) -> Expression:
        """Aggregates the values in the expression into a set (ignoring nulls).

        Returns:
            Expression: A List expression containing the distinct values from the input

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"values": [1, 1, None, 2, 2, None]})
            >>> df.agg(df["values"].agg_set().alias("distinct_values")).show()
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
            >>> df.agg(df["values"].agg_set().alias("distinct_values")).show()
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
        expr = self._expr.agg_set()
        return Expression._from_pyexpr(expr)

    def agg_concat(self) -> Expression:
        """Aggregates the values in the expression into a single string by concatenating them."""
        expr = self._expr.agg_concat()
        return Expression._from_pyexpr(expr)

    def _explode(self) -> Expression:
        f = native.get_function_from_registry("explode")
        return Expression._from_pyexpr(f(self._expr))

    def if_else(self, if_true: Expression, if_false: Expression) -> Expression:
        """Conditionally choose values between two expressions using the current boolean expression as a condition.

        Args:
            if_true (Expression): Values to choose if condition is true
            if_false (Expression): Values to choose if condition is false

        Returns:
            Expression: New expression where values are chosen from `if_true` and `if_false`.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"A": [1, 2, 3], "B": [0, 2, 4]})
            >>> df = df.with_column(
            ...     "A_if_bigger_else_B",
            ...     (df["A"] > df["B"]).if_else(df["A"], df["B"]),
            ... )
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

        """
        if_true = Expression._to_expression(if_true)
        if_false = Expression._to_expression(if_false)
        return Expression._from_pyexpr(self._expr.if_else(if_true._expr, if_false._expr))

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

        """
        from daft.udf import UDF

        inferred_return_dtype = DataType._infer_type(return_dtype)

        def batch_func(self_series: Series) -> list[Any]:
            return [func(x) for x in self_series]

        name = getattr(func, "__module__", "")
        if name:
            name = name + "."
        name = name + getattr(func, "__qualname__")

        return UDF(
            inner=batch_func,
            name=name,
            return_dtype=inferred_return_dtype,
        )(self)

    def is_null(self) -> Expression:
        """Checks if values in the Expression are Null (a special value indicating missing data).

        Returns:
            Expression: Boolean Expression indicating whether values are missing

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1.0, None, float("nan")]})
            >>> df = df.select(df["x"].is_null())
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
        expr = self._expr.is_null()
        return Expression._from_pyexpr(expr)

    def not_null(self) -> Expression:
        """Checks if values in the Expression are not Null (a special value indicating missing data).

        Returns:
            Expression: Boolean Expression indicating whether values are not missing

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [1.0, None, float("nan")]})
            >>> df = df.select(df["x"].not_null())
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
        expr = self._expr.not_null()
        return Expression._from_pyexpr(expr)

    def fill_null(self, fill_value: Expression) -> Expression:
        """Fills null values in the Expression with the provided fill_value.

        Returns:
            Expression: Expression with null values filled with the provided fill_value

        Examples:
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

        """
        fill_value = Expression._to_expression(fill_value)
        expr = self._expr.fill_null(fill_value._expr)
        return Expression._from_pyexpr(expr)

    def is_in(self, other: Any) -> Expression:
        """Checks if values in the Expression are in the provided list.

        Returns:
            Expression: Boolean Expression indicating whether values are in the provided list

        Examples:
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

        """
        if isinstance(other, Collection):
            other = [Expression._to_expression(item) for item in other]
        elif not isinstance(other, Expression):
            series = item_to_series("items", other)
            other = [Expression._from_pyexpr(_series_lit(series._series))]
        else:
            other = [other]

        expr = self._expr.is_in([item._expr for item in other])
        return Expression._from_pyexpr(expr)

    def between(self, lower: Any, upper: Any) -> Expression:
        """Checks if values in the Expression are between lower and upper, inclusive.

        Returns:
            Expression: Boolean Expression indicating whether values are between lower and upper, inclusive.

        Examples:
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

        """
        lower = Expression._to_expression(lower)
        upper = Expression._to_expression(upper)

        expr = self._expr.between(lower._expr, upper._expr)
        return Expression._from_pyexpr(expr)

    def hash(self, seed: Any | None = None) -> Expression:
        """Hashes the values in the Expression.

        Uses the [XXH3_64bits](https://xxhash.com/) non-cryptographic hash function to hash the values in the expression.

        Args:
            seed (optional): Seed used for generating the hash. Defaults to 0.

        Note:
            Null values will produce a hash value instead of being propagated as null.

        """
        return self._eval_expressions("hash", seed=seed)

    def minhash(
        self,
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
            num_hashes: The number of hash permutations to compute.
            ngram_size: The number of tokens in each shingle/ngram.
            seed (optional): Seed used for generating permutations and the initial string hashes. Defaults to 1.
            hash_function (optional): Hash function to use for initial string hashing. One of "murmurhash3", "xxhash", or "sha1". Defaults to "murmurhash3".

        """
        return self._eval_expressions(
            "minhash", num_hashes=num_hashes, ngram_size=ngram_size, seed=seed, hash_function=hash_function
        )

    def encode(self, codec: EncodingCodec) -> Expression:
        r"""Encodes the expression (binary strings) using the specified codec.

        Args:
            codec (str): encoding codec (deflate, gzip, zlib)

        Returns:
            Expression: A new expression, of type `binary`, with the encoded value.

        Note:
            This inputs either a string or binary and returns a binary.
            If the input value is a string and 'utf-8' is the codec, then it's just a cast to binary.
            If the input value is a binary and 'utf-8' is the codec, we verify the bytes are valid utf-8.

        Examples:
            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"text": [b"hello, world!"]})  # binary
            >>> df.select(col("text").encode("zlib")).show()
            ╭────────────────────────────────╮
            │ text                           │
            │ ---                            │
            │ Binary                         │
            ╞════════════════════════════════╡
            │ b"x\x9c\xcbH\xcd\xc9\xc9\xd7Q… │
            ╰────────────────────────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

            >>> import daft
            >>> from daft import col
            >>> df = daft.from_pydict({"text": ["hello, world!"]})  # string
            >>> df.select(col("text").encode("zlib")).show()
            ╭────────────────────────────────╮
            │ text                           │
            │ ---                            │
            │ Binary                         │
            ╞════════════════════════════════╡
            │ b"x\x9c\xcbH\xcd\xc9\xc9\xd7Q… │
            ╰────────────────────────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        return self._eval_expressions("encode", codec=codec)

    def decode(self, codec: EncodingCodec) -> Expression:
        """Decodes the expression (binary strings) using the specified codec.

        Args:
            codec (str): decoding codec (deflate, gzip, zlib)

        Returns:
            Expression: A new expression with the decoded values.

        Note:
            This inputs a binary and returns either a binary or string. For now,
            only decoding with 'utf-8' returns a string.

        Examples:
            >>> import daft
            >>> import zlib
            >>> from daft import col
            >>> df = daft.from_pydict({"bytes": [zlib.compress(b"hello, world!")]})
            >>> df.select(col("bytes").decode("zlib")).show()
            ╭──────────────────╮
            │ bytes            │
            │ ---              │
            │ Binary           │
            ╞══════════════════╡
            │ b"hello, world!" │
            ╰──────────────────╯
            <BLANKLINE>
            (Showing first 1 of 1 rows)

        """
        return self._eval_expressions("decode", codec=codec)

    def try_encode(self, codec: EncodingCodec) -> Expression:
        """Encodes or returns null, see `Expression.encode`."""
        return self._eval_expressions("try_encode", codec=codec)

    def try_decode(self, codec: EncodingCodec) -> Expression:
        """Decodes or returns null, see `Expression.decode`."""
        return self._eval_expressions("try_decode", codec=codec)

    def deserialize(self, format: Literal["json"], dtype: DataTypeLike) -> Expression:
        """Deserializes the expression (string) using the specified format and data type.

        Args:
            format (Literal["json"]): The serialization format.
            dtype: The target data type to deserialize into.

        Returns:
            Expression: A new expression with the deserialized value.
        """
        if isinstance(dtype, str):
            dtype = DataType._from_pydatatype(sql_datatype(dtype))
        else:
            assert isinstance(dtype, (DataType, type))
            dtype = DataType._infer_type(dtype)
        return self._eval_expressions("deserialize", format, dtype._dtype)

    def try_deserialize(self, format: Literal["json"], dtype: DataTypeLike) -> Expression:
        """Deserializes the expression (string) using the specified format and data type, inserting nulls on failures.

        Args:
            format (Literal["json"]): The serialization format.
            dtype: The target data type to deserialize into.

        Returns:
            Expression: A new expression with the deserialized value (or null).
        """
        if isinstance(dtype, str):
            dtype = DataType._from_pydatatype(sql_datatype(dtype))
        else:
            assert isinstance(dtype, (DataType, type))
            dtype = DataType._infer_type(dtype)
        return self._eval_expressions("try_deserialize", format, dtype._dtype)

    def serialize(self, format: Literal["json"]) -> Expression:
        """Serializes the expression as a string using the specified format.

        Args:
            format (Literal["json"]): The serialization format.

        Returns:
            Expression: A new expression with the serialized string.
        """
        return self._eval_expressions("serialize", format)

    def jq(self, filter: builtins.str) -> Expression:
        """Applies a [jq](https://jqlang.github.io/jq/manual/) filter to the expression (string), returning the results as a string.

        Args:
            file (str): The jq filter.

        Returns:
            Expression: Expression representing the result of the jq filter as a column of JSON-compatible strings.

        Warning:
            This expression uses [jaq](https://github.com/01mf02/jaq) as its filter executor which can differ from the
            [jq](https://jqlang.org/) command-line tool. Please consult [jq vs. jaq](https://github.com/01mf02/jaq?tab=readme-ov-file#differences-between-jq-and-jaq)
            for a detailed look into possible differences.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"col": ['{"a": 1}', '{"a": 2}', '{"a": 3}']})
            >>> df.with_column("res", df["col"].jq(".a")).collect()
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

        """
        return self._eval_expressions("jq", filter)

    def name(self) -> builtins.str:
        return self._expr.name()

    def over(self, window: Window) -> Expression:
        """Apply the expression as a window function.

        Args:
            window: The window specification (created using ``daft.Window``)
                defining partitioning, ordering, and framing.

        Examples:
            >>> import daft
            >>> from daft import Window, col
            >>> df = daft.from_pydict(
            ...     {
            ...         "group": ["A", "A", "A", "B", "B", "B"],
            ...         "date": ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05", "2020-01-06"],
            ...         "value": [1, 2, 3, 4, 5, 6],
            ...     }
            ... )
            >>> window_spec = Window().partition_by("group").order_by("date")
            >>> df = df.with_column("cumulative_sum", col("value").sum().over(window_spec))
            >>> df.sort(["group", "date"]).show()
            ╭───────┬────────────┬───────┬────────────────╮
            │ group ┆ date       ┆ value ┆ cumulative_sum │
            │ ---   ┆ ---        ┆ ---   ┆ ---            │
            │ Utf8  ┆ Utf8       ┆ Int64 ┆ Int64          │
            ╞═══════╪════════════╪═══════╪════════════════╡
            │ A     ┆ 2020-01-01 ┆ 1     ┆ 1              │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ A     ┆ 2020-01-02 ┆ 2     ┆ 3              │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ A     ┆ 2020-01-03 ┆ 3     ┆ 6              │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B     ┆ 2020-01-04 ┆ 4     ┆ 4              │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B     ┆ 2020-01-05 ┆ 5     ┆ 9              │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B     ┆ 2020-01-06 ┆ 6     ┆ 15             │
            ╰───────┴────────────┴───────┴────────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)

        Returns:
            Expression: The result of applying this expression as a window function.
        """
        expr = self._expr.over(window._spec)
        return Expression._from_pyexpr(expr)

    def lag(self, offset: int = 1, default: Any | None = None) -> Expression:
        """Get the value from a previous row within a window partition.

        Args:
            offset: The number of rows to shift backward. Must be >= 0.
            default: Value to use when no previous row exists. Can be a column reference.

        Returns:
            Expression: Value from the row `offset` positions before the current row.

        Examples:
            >>> import daft
            >>> from daft import Window, col
            >>> df = daft.from_pydict(
            ...     {
            ...         "category": ["A", "A", "A", "B", "B", "B"],
            ...         "value": [1, 2, 3, 4, 5, 6],
            ...         "default_val": [10, 20, 30, 40, 50, 60],
            ...     }
            ... )
            >>>
            >>> # Simple lag with null default
            >>> window = Window().partition_by("category").order_by("value")
            >>> df = df.with_column("lagged", col("value").lag(1).over(window))
            >>>
            >>> # Lag with column reference as default
            >>> df = df.with_column("lagged_with_default", col("value").lag(1, default=col("default_val")).over(window))
            >>> df.sort(["category", "value"]).show()
            ╭──────────┬───────┬─────────────┬────────┬─────────────────────╮
            │ category ┆ value ┆ default_val ┆ lagged ┆ lagged_with_default │
            │ ---      ┆ ---   ┆ ---         ┆ ---    ┆ ---                 │
            │ Utf8     ┆ Int64 ┆ Int64       ┆ Int64  ┆ Int64               │
            ╞══════════╪═══════╪═════════════╪════════╪═════════════════════╡
            │ A        ┆ 1     ┆ 10          ┆ None   ┆ 10                  │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ A        ┆ 2     ┆ 20          ┆ 1      ┆ 1                   │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ A        ┆ 3     ┆ 30          ┆ 2      ┆ 2                   │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B        ┆ 4     ┆ 40          ┆ None   ┆ 40                  │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B        ┆ 5     ┆ 50          ┆ 4      ┆ 4                   │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B        ┆ 6     ┆ 60          ┆ 5      ┆ 5                   │
            ╰──────────┴───────┴─────────────┴────────┴─────────────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        if default is not None:
            default = Expression._to_expression(default)
        expr = self._expr.offset(-offset, default._expr if default is not None else None)
        return Expression._from_pyexpr(expr)

    def lead(self, offset: int = 1, default: Any | None = None) -> Expression:
        """Get the value from a future row within a window partition.

        Args:
            offset: The number of rows to shift forward. Must be >= 0.
            default: Value to use when no future row exists. Can be a column reference.

        Returns:
            Expression: Value from the row `offset` positions after the current row.

        Examples:
            >>> import daft
            >>> from daft import Window, col
            >>> df = daft.from_pydict(
            ...     {
            ...         "category": ["A", "A", "A", "B", "B", "B"],
            ...         "value": [1, 2, 3, 4, 5, 6],
            ...         "default_val": [10, 20, 30, 40, 50, 60],
            ...     }
            ... )
            >>>
            >>> # Simple lag with null default
            >>> window = Window().partition_by("category").order_by("value")
            >>> df = df.with_column("lead", col("value").lead(1).over(window))
            >>>
            >>> # Lead with column reference as default
            >>> df = df.with_column("lead_with_default", col("value").lead(1, default=col("default_val")).over(window))
            >>> df.sort(["category", "value"]).show()
            ╭──────────┬───────┬─────────────┬───────┬───────────────────╮
            │ category ┆ value ┆ default_val ┆ lead  ┆ lead_with_default │
            │ ---      ┆ ---   ┆ ---         ┆ ---   ┆ ---               │
            │ Utf8     ┆ Int64 ┆ Int64       ┆ Int64 ┆ Int64             │
            ╞══════════╪═══════╪═════════════╪═══════╪═══════════════════╡
            │ A        ┆ 1     ┆ 10          ┆ 2     ┆ 2                 │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ A        ┆ 2     ┆ 20          ┆ 3     ┆ 3                 │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ A        ┆ 3     ┆ 30          ┆ None  ┆ 30                │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B        ┆ 4     ┆ 40          ┆ 5     ┆ 5                 │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B        ┆ 5     ┆ 50          ┆ 6     ┆ 6                 │
            ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ B        ┆ 6     ┆ 60          ┆ None  ┆ 60                │
            ╰──────────┴───────┴─────────────┴───────┴───────────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        if default is not None:
            default = Expression._to_expression(default)
        expr = self._expr.offset(offset, default._expr if default is not None else None)
        return Expression._from_pyexpr(expr)

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

    def url_parse(self) -> Expression:
        """Parses URLs in a string column and extracts URL components.

        Returns:
            Expression: a Struct expression containing the parsed URL components:
                - scheme (str): The URL scheme (e.g., "https", "http")
                - username (str): The username, if present
                - password (str): The password, if present
                - host (str): The hostname or IP address
                - port (int): The port number, if specified
                - path (str): The path component
                - query (str): The query string, if present
                - fragment (str): The fragment/anchor, if present

        Examples:
            >>> import daft
            >>> df = daft.from_pydict(
            ...     {"urls": ["https://user:pass@example.com:8080/path?query=value#fragment", "http://localhost/api"]}
            ... )
            >>> # Parse URLs and expand all components
            >>> df.select(daft.col("urls").url_parse()).select(daft.col("urls").struct.get("*")).collect()  # doctest: +SKIP

        Note:
            Invalid URLs will result in null values for all components.
            The parsed result is automatically aliased to 'urls' to enable easy struct field expansion.
        """
        f = native.get_function_from_registry("url_parse")
        return Expression._from_pyexpr(f(self._expr))

    def explode(self) -> Expression:
        """Explode a list expression.

        A row is created for each item in the lists, and the other non-exploded output columns are broadcasted to match.

        If exploding multiple columns at once, all list lengths must match.

        Tip: See also
            [DataFrame.explode](https://docs.daft.ai/en/stable/api/dataframe/#daft.DataFrame.explain)

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"id": [1, 2, 3], "sentence": ["lorem ipsum", "foo bar baz", "hi"]})
            >>>
            >>> # Explode one column, broadcast the rest
            >>> df.with_column("word", df["sentence"].str.split(" ").explode()).show()
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
            >>>
            >>> # Explode multiple columns with the same lengths
            >>> df.select(
            ...     df["sentence"].str.split(" ").explode().alias("word"),
            ...     df["sentence"].str.capitalize().str.split(" ").explode().alias("capitalized_word"),
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
            >>> # This will error because exploded lengths are different:
            # df.select(
            #     df["sentence"]
            #             .str.split(" ")
            #             .explode()
            #             .alias("word"),
            #     df["sentence"]
            #             .str.split("a")
            #             .explode()
            #             .alias("split_on_a")
            # ).show()
        """
        f = native.get_function_from_registry("explode")
        return Expression._from_pyexpr(f(self._expr))


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

    def _eval_expressions(self, func_name: str, *args: Any, **kwargs: Any) -> Expression:
        e = Expression._from_pyexpr(self._expr)
        return e._eval_expressions(func_name, *args, **kwargs)


class ExpressionUrlNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.url` attribute."""

    @staticmethod
    def _should_use_multithreading_tokio_runtime() -> bool:
        """Whether or not our expression should use the multithreaded tokio runtime under the hood, or a singlethreaded one.

        This matters because for distributed workloads, each process has its own tokio I/O runtime. if each distributed process
        is multithreaded (by default we spin up `N_CPU` threads) then we will be running `(N_CPU * N_PROC)` number of threads, and
        opening `(N_CPU * N_PROC * max_connections)` number of connections. This is too large for big machines with many CPU cores.

        Hence for Ray we default to doing the singlethreaded runtime. This means that we will have a limit of
        `(singlethreaded=1 * N_PROC * max_connections)` number of open connections per machine, which works out to be reasonable at ~2-4k connections.

        For local execution, we run in a single process which means that it all shares the same tokio I/O runtime and connection pool.
        Thus we just have `(multithreaded=N_CPU * max_connections)` number of open connections, which is usually reasonable as well.
        """
        using_ray_runner = context.get_context().get_or_create_runner().name == "ray"
        return not using_ray_runner

    @staticmethod
    def _override_io_config_max_connections(max_connections: int, io_config: IOConfig | None) -> IOConfig:
        """Use a user-provided `max_connections` argument to override the value in S3Config.

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
        on_error: Literal["raise", "null"] = "raise",
        io_config: IOConfig | None = None,
    ) -> Expression:
        """Treats each string as a URL, and downloads the bytes contents as a bytes column.

        Args:
            max_connections: The maximum number of connections to use per thread to use for downloading URLs. Defaults to 32.
            on_error: Behavior when a URL download error is encountered - "raise" to raise the error immediately or "null" to log
                the error but fallback to a Null value. Defaults to "raise".
            io_config: IOConfig to use when accessing remote storage. Note that the S3Config's `max_connections` parameter will be overridden
                with `max_connections` that is passed in as a kwarg.

        Returns:
            Expression: a Binary expression which is the bytes contents of the URL, or None if an error occurred during download

        Note:
            If you are observing excessive S3 issues (such as timeouts, DNS errors or slowdown errors) during URL downloads,
            you may wish to reduce the value of ``max_connections`` (defaults to 32) to reduce the amount of load you are placing
            on your S3 servers.

            Alternatively, if you are running on machines with lower number of cores but very high network bandwidth, you can increase
            ``max_connections`` to get higher throughput with additional parallelism

        """
        multi_thread = ExpressionUrlNamespace._should_use_multithreading_tokio_runtime()
        io_config = ExpressionUrlNamespace._override_io_config_max_connections(max_connections, io_config)

        if io_config.unity.endpoint is None:
            try:
                from daft.catalog.__unity import UnityCatalog
            except ImportError:
                pass
            else:
                from daft.session import current_catalog

                catalog = current_catalog()
                if isinstance(catalog, UnityCatalog):
                    unity_catalog = catalog._inner
                    io_config = io_config.replace(unity=unity_catalog.to_io_config().unity)

        max_connections_expr = Expression._to_expression(max_connections)._expr
        on_error_expr = Expression._to_expression(on_error)._expr
        multi_thread_expr = Expression._to_expression(multi_thread)._expr
        io_config_expr = Expression._to_expression(io_config)._expr

        f = native.get_function_from_registry("url_download")

        return Expression._from_pyexpr(
            f(
                self._expr,
                multi_thread=multi_thread_expr,
                on_error=on_error_expr,
                max_connections=max_connections_expr,
                io_config=io_config_expr,
            )
        )

    def upload(
        self,
        location: str | Expression,
        max_connections: int = 32,
        on_error: Literal["raise", "null"] = "raise",
        io_config: IOConfig | None = None,
    ) -> Expression:
        """Uploads a column of binary data to the provided location(s) (also supports S3, local etc).

        Files will be written into the location (folder(s)) with a generated UUID filename, and the result
        will be returned as a column of string paths that is compatible with the ``.url.download()`` Expression.

        Args:
            location: a folder location or column of folder locations to upload data into
            max_connections: The maximum number of connections to use per thread to use for uploading data. Defaults to 32.
            on_error: Behavior when a URL upload error is encountered - "raise" to raise the error immediately or "null" to log
                the error but fallback to a Null value. Defaults to "raise".
            io_config: IOConfig to use when uploading data

        Returns:
            Expression: a String expression containing the written filepath

        Examples:
            >>> col("data").url.upload("s3://my-bucket/my-folder")  # doctest: +SKIP

            Upload to row-specific URLs

            >>> col("data").url.upload(col("paths"))  # doctest: +SKIP

        """
        location_expr = Expression._to_expression(location)._expr
        multi_thread = ExpressionUrlNamespace._should_use_multithreading_tokio_runtime()
        # If the user specifies a single location via a string, we should upload to a single folder. Otherwise,
        # if the user gave an expression, we assume that each row has a specific url to upload to.
        # Consider moving the check for is_single_folder to a lower IR.
        is_single_folder = isinstance(location, str)
        io_config = ExpressionUrlNamespace._override_io_config_max_connections(max_connections, io_config)
        max_connections_expr = Expression._to_expression(max_connections)._expr
        on_error_expr = Expression._to_expression(on_error)._expr
        multi_thread_expr = Expression._to_expression(multi_thread)._expr
        io_config_expr = Expression._to_expression(io_config)._expr
        is_single_folder_expr = Expression._to_expression(is_single_folder)._expr
        f = native.get_function_from_registry("url_upload")
        return Expression._from_pyexpr(
            f(
                self._expr,
                location_expr,
                max_connections=max_connections_expr,
                on_error=on_error_expr,
                multi_thread=multi_thread_expr,
                is_single_folder=is_single_folder_expr,
                io_config=io_config_expr,
            )
        )


class ExpressionFloatNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.float` attribute."""

    def is_nan(self) -> Expression:
        """Checks if values are NaN (a special float value indicating not-a-number).

        Returns:
            Expression: Boolean Expression indicating whether values are invalid.

        Note:
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"data": [1.0, None, float("nan")]})
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

        """
        f = native.get_function_from_registry("is_nan")
        return Expression._from_pyexpr(f(self._expr))

    def is_inf(self) -> Expression:
        """Checks if values in the Expression are Infinity.

        Returns:
            Expression: Boolean Expression indicating whether values are Infinity.

        Note:
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"data": [-float("inf"), 0.0, float("inf"), None]})
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

        """
        f = native.get_function_from_registry("is_inf")
        return Expression._from_pyexpr(f(self._expr))

    def not_nan(self) -> Expression:
        """Checks if values are not NaN (a special float value indicating not-a-number).

        Returns:
            Expression: Boolean Expression indicating whether values are not invalid.

        Note:
            Nulls will be propagated! I.e. this operation will return a null for null values.

        Examples:
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

        """
        f = native.get_function_from_registry("not_nan")
        return Expression._from_pyexpr(f(self._expr))

    def fill_nan(self, fill_value: Expression) -> Expression:
        """Fills NaN values in the Expression with the provided fill_value.

        Returns:
            Expression: Expression with Nan values filled with the provided fill_value

        Examples:
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

        """
        fill_value = Expression._to_expression(fill_value)
        f = native.get_function_from_registry("fill_nan")
        return Expression._from_pyexpr(f(self._expr, fill_value._expr))


class ExpressionDatetimeNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.dt` attribute."""

    def date(self) -> Expression:
        """Retrieves the date for a datetime column.

        Returns:
            Expression: a Date expression

        Examples:
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

        """
        return self._eval_expressions("date")

    def day(self) -> Expression:
        """Retrieves the day for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the day extracted from a datetime column

        Examples:
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

        """
        return self._eval_expressions("day")

    def hour(self) -> Expression:
        """Retrieves the day for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the day extracted from a datetime column

        Examples:
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

        """
        return self._eval_expressions("hour")

    def minute(self) -> Expression:
        """Retrieves the minute for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the minute extracted from a datetime column

        Examples:
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

        """
        return self._eval_expressions("minute")

    def second(self) -> Expression:
        """Retrieves the second for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the second extracted from a datetime column

        Examples:
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

        """
        return self._eval_expressions("second")

    def millisecond(self) -> Expression:
        """Retrieves the millisecond for a datetime column.

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(1978, 1, 1, 1, 1, 1, 0),
            ...             datetime(2024, 10, 13, 5, 30, 14, 500_000),
            ...             datetime(2065, 1, 1, 10, 20, 30, 60_000),
            ...         ]
            ...     }
            ... )
            >>> df = df.select(daft.col("datetime").dt.millisecond())
            >>> df.show()
            ╭──────────╮
            │ datetime │
            │ ---      │
            │ UInt32   │
            ╞══════════╡
            │ 0        │
            ├╌╌╌╌╌╌╌╌╌╌┤
            │ 500      │
            ├╌╌╌╌╌╌╌╌╌╌┤
            │ 60       │
            ╰──────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        return self._eval_expressions("millisecond")

    def microsecond(self) -> Expression:
        """Retrieves the microsecond for a datetime column.

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(1978, 1, 1, 1, 1, 1, 0),
            ...             datetime(2024, 10, 13, 5, 30, 14, 500_000),
            ...             datetime(2065, 1, 1, 10, 20, 30, 60_000),
            ...         ]
            ...     }
            ... )
            >>> df.select(daft.col("datetime").dt.microsecond()).show()
            ╭──────────╮
            │ datetime │
            │ ---      │
            │ UInt32   │
            ╞══════════╡
            │ 0        │
            ├╌╌╌╌╌╌╌╌╌╌┤
            │ 500000   │
            ├╌╌╌╌╌╌╌╌╌╌┤
            │ 60000    │
            ╰──────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        return self._eval_expressions("microsecond")

    def nanosecond(self) -> Expression:
        """Retrieves the nanosecond for a datetime column.

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(1978, 1, 1, 1, 1, 1, 0),
            ...             datetime(2024, 10, 13, 5, 30, 14, 500_000),
            ...             datetime(2065, 1, 1, 10, 20, 30, 60_000),
            ...         ]
            ...     }
            ... )
            >>>
            >>> df.select(daft.col("datetime").dt.nanosecond()).show()
            ╭───────────╮
            │ datetime  │
            │ ---       │
            │ UInt32    │
            ╞═══════════╡
            │ 0         │
            ├╌╌╌╌╌╌╌╌╌╌╌┤
            │ 500000000 │
            ├╌╌╌╌╌╌╌╌╌╌╌┤
            │ 60000000  │
            ╰───────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        return self._eval_expressions("nanosecond")

    def unix_date(self) -> Expression:
        """Retrieves the number of days since 1970-01-01 00:00:00 UTC.

        Returns:
            Expression: a UInt64 expression

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(1978, 1, 1, 1, 1, 1, 0),
            ...             datetime(2024, 10, 13, 5, 30, 14, 500_000),
            ...             datetime(2065, 1, 1, 10, 20, 30, 60_000),
            ...         ]
            ...     }
            ... )
            >>>
            >>> df.select(daft.col("datetime").alias("unix_date").dt.unix_date()).show()
            ╭───────────╮
            │ unix_date │
            │ ---       │
            │ UInt64    │
            ╞═══════════╡
            │ 2922      │
            ├╌╌╌╌╌╌╌╌╌╌╌┤
            │ 20009     │
            ├╌╌╌╌╌╌╌╌╌╌╌┤
            │ 34699     │
            ╰───────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        return self._eval_expressions("unix_date")

    def time(self) -> Expression:
        """Retrieves the time for a datetime column.

        Returns:
            Expression: a Time expression

        Examples:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "x": [
            ...             datetime.datetime(2021, 1, 1, 0, 1, 1),
            ...             datetime.datetime(2021, 1, 1, 12, 1, 59),
            ...             datetime.datetime(2021, 1, 1, 23, 59, 59),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("time", df["x"].dt.time())
            >>> df.show()
            ╭───────────────────────────────┬────────────────────╮
            │ x                             ┆ time               │
            │ ---                           ┆ ---                │
            │ Timestamp(Microseconds, None) ┆ Time(Microseconds) │
            ╞═══════════════════════════════╪════════════════════╡
            │ 2021-01-01 00:01:01           ┆ 00:01:01           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-01 12:01:59           ┆ 12:01:59           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2021-01-01 23:59:59           ┆ 23:59:59           │
            ╰───────────────────────────────┴────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        return self._eval_expressions("time")

    def month(self) -> Expression:
        """Retrieves the month for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the month extracted from a datetime column

        Examples:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
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

        """
        return self._eval_expressions("month")

    def quarter(self) -> Expression:
        """Retrieves the quarter for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the quarter extracted from a datetime column

        Examples:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime.datetime(2024, 1, 1, 0, 0, 0),
            ...             datetime.datetime(2023, 7, 4, 0, 0, 0),
            ...             datetime.datetime(2022, 12, 5, 0, 0, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("quarter", df["datetime"].dt.quarter()).collect()
            ╭───────────────────────────────┬─────────╮
            │ datetime                      ┆ quarter │
            │ ---                           ┆ ---     │
            │ Timestamp(Microseconds, None) ┆ UInt32  │
            ╞═══════════════════════════════╪═════════╡
            │ 2024-01-01 00:00:00           ┆ 1       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 2023-07-04 00:00:00           ┆ 3       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 2022-12-05 00:00:00           ┆ 4       │
            ╰───────────────────────────────┴─────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        return self._eval_expressions("quarter")

    def year(self) -> Expression:
        """Retrieves the year for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the year extracted from a datetime column

        Examples:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
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

        """
        return self._eval_expressions("year")

    def day_of_week(self) -> Expression:
        """Retrieves the day of the week for a datetime column, starting at 0 for Monday and ending at 6 for Sunday.

        Returns:
            Expression: a UInt32 expression with just the day_of_week extracted from a datetime column

        Examples:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
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

        """
        return self._eval_expressions("day_of_week")

    def day_of_month(self) -> Expression:
        """Retrieves the day of the month for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the day_of_month extracted from a datetime column

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(2024, 1, 1, 0, 0, 0),
            ...             datetime(2024, 2, 1, 0, 0, 0),
            ...             datetime(2024, 12, 31, 0, 0, 0),
            ...             datetime(2023, 12, 31, 0, 0, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("day_of_month", df["datetime"].dt.day_of_month()).collect()
            ╭───────────────────────────────┬──────────────╮
            │ datetime                      ┆ day_of_month │
            │ ---                           ┆ ---          │
            │ Timestamp(Microseconds, None) ┆ UInt32       │
            ╞═══════════════════════════════╪══════════════╡
            │ 2024-01-01 00:00:00           ┆ 1            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-02-01 00:00:00           ┆ 1            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-12-31 00:00:00           ┆ 31           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2023-12-31 00:00:00           ┆ 31           │
            ╰───────────────────────────────┴──────────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        return self._eval_expressions("day_of_month")

    def day_of_year(self) -> Expression:
        """Retrieves the ordinal day for a datetime column. Starting at 1 for January 1st and ending at 365 or 366 for December 31st.

        Returns:
            Expression: a UInt32 expression with just the day_of_year extracted from a datetime column

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(2024, 1, 1, 0, 0, 0),
            ...             datetime(2024, 2, 1, 0, 0, 0),
            ...             datetime(2024, 12, 31, 0, 0, 0),  # 2024 is a leap year
            ...             datetime(2023, 12, 31, 0, 0, 0),  # not leap year
            ...         ],
            ...     }
            ... )
            >>> df.with_column("day_of_year", df["datetime"].dt.day_of_year()).collect()
            ╭───────────────────────────────┬─────────────╮
            │ datetime                      ┆ day_of_year │
            │ ---                           ┆ ---         │
            │ Timestamp(Microseconds, None) ┆ UInt32      │
            ╞═══════════════════════════════╪═════════════╡
            │ 2024-01-01 00:00:00           ┆ 1           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-02-01 00:00:00           ┆ 32          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-12-31 00:00:00           ┆ 366         │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2023-12-31 00:00:00           ┆ 365         │
            ╰───────────────────────────────┴─────────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        return self._eval_expressions("day_of_year")

    def week_of_year(self) -> Expression:
        """Retrieves the week of the year for a datetime column.

        Returns:
            Expression: a UInt32 expression with just the week_of_year extracted from a datetime column

        Examples:
            >>> import daft
            >>> from datetime import datetime
            >>> df = daft.from_pydict(
            ...     {
            ...         "datetime": [
            ...             datetime(2024, 1, 1, 0, 0, 0),
            ...             datetime(2024, 2, 1, 0, 0, 0),
            ...             datetime(2024, 12, 31, 0, 0, 0),  # part of week 1 of 2025 according to ISO 8601 standard
            ...             datetime(2023, 12, 31, 0, 0, 0),
            ...         ],
            ...     }
            ... )
            >>> df.with_column("week_of_year", df["datetime"].dt.week_of_year()).collect()
            ╭───────────────────────────────┬──────────────╮
            │ datetime                      ┆ week_of_year │
            │ ---                           ┆ ---          │
            │ Timestamp(Microseconds, None) ┆ UInt32       │
            ╞═══════════════════════════════╪══════════════╡
            │ 2024-01-01 00:00:00           ┆ 1            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-02-01 00:00:00           ┆ 5            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2024-12-31 00:00:00           ┆ 1            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2023-12-31 00:00:00           ┆ 52           │
            ╰───────────────────────────────┴──────────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        return self._eval_expressions("week_of_year")

    def truncate(self, interval: str, relative_to: Expression | None = None) -> Expression:
        """Truncates the datetime column to the specified interval.

        Args:
            interval: The interval to truncate to. Must be a string representing a valid interval in "{integer} {unit}" format, e.g. "1 day". Valid time units are: 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week'.
            relative_to: Optional timestamp to truncate relative to. If not provided, truncates to the start of the Unix epoch: 1970-01-01 00:00:00.

        Returns:
            Expression: a DateTime expression truncated to the specified interval

        Examples:
            >>> import daft, datetime
            >>> df = daft.from_pydict(
            ...     {
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

        """
        return self._eval_expressions("truncate", relative_to, interval=interval)

    def to_unix_epoch(self, time_unit: str | TimeUnit | None = None) -> Expression:
        """Converts a datetime column to a Unix timestamp. with the specified time unit. (default: seconds).

        See [daft.datatype.TimeUnit](https://docs.getdaft.io/en/stable/api/datatypes/#daft.datatype.DataType.timeunit) for more information on time units and valid values.

        Examples:
            >>> import daft
            >>> from datetime import date
            >>> df = daft.from_pydict(
            ...     {
            ...         "dates": [
            ...             date(2001, 1, 1),
            ...             date(2001, 1, 2),
            ...             date(2001, 1, 3),
            ...             None,
            ...         ]
            ...     }
            ... )
            >>> df.with_column("timestamp", daft.col("dates").dt.to_unix_epoch("ns")).show()
            ╭────────────┬────────────────────╮
            │ dates      ┆ timestamp          │
            │ ---        ┆ ---                │
            │ Date       ┆ Int64              │
            ╞════════════╪════════════════════╡
            │ 2001-01-01 ┆ 978307200000000000 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2001-01-02 ┆ 978393600000000000 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2001-01-03 ┆ 978480000000000000 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ None       ┆ None               │
            ╰────────────┴────────────────────╯
            <BLANKLINE>
            (Showing first 4 of 4 rows)
        """
        return self._eval_expressions("to_unix_epoch", time_unit=time_unit)

    def strftime(self, format: str | None = None) -> Expression:
        """Converts a datetime/date column to a string column.

        Args:
            format: The format to use for the conversion. If None, defaults to ISO 8601 format.

        Note:
            The format must be a valid datetime format string. (defaults to ISO 8601 format)
            See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html


        Examples:
            >>> import daft
            >>> from datetime import datetime, date
            >>> df = daft.from_pydict(
            ...     {
            ...         "dates": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
            ...         "datetimes": [
            ...             datetime(2023, 1, 1, 12, 1),
            ...             datetime(2023, 1, 2, 12, 0, 0, 0),
            ...             datetime(2023, 1, 3, 12, 0, 0, 999_999),
            ...         ],
            ...     }
            ... )
            >>> df = df.with_column("datetimes_s", daft.col("datetimes").cast(daft.DataType.timestamp("s")))
            >>> df.select(
            ...     daft.col("dates").dt.strftime().alias("iso_date"),
            ...     daft.col("dates").dt.strftime(format="%m/%d/%Y").alias("custom_date"),
            ...     daft.col("datetimes").dt.strftime().alias("iso_datetime"),
            ...     daft.col("datetimes_s").dt.strftime().alias("iso_datetime_s"),
            ...     daft.col("datetimes_s").dt.strftime(format="%Y/%m/%d %H:%M:%S").alias("custom_datetime"),
            ... ).show()
            ╭────────────┬─────────────┬────────────────────────────┬─────────────────────┬─────────────────────╮
            │ iso_date   ┆ custom_date ┆ iso_datetime               ┆ iso_datetime_s      ┆ custom_datetime     │
            │ ---        ┆ ---         ┆ ---                        ┆ ---                 ┆ ---                 │
            │ Utf8       ┆ Utf8        ┆ Utf8                       ┆ Utf8                ┆ Utf8                │
            ╞════════════╪═════════════╪════════════════════════════╪═════════════════════╪═════════════════════╡
            │ 2023-01-01 ┆ 01/01/2023  ┆ 2023-01-01T12:01:00.000000 ┆ 2023-01-01T12:01:00 ┆ 2023/01/01 12:01:00 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2023-01-02 ┆ 01/02/2023  ┆ 2023-01-02T12:00:00.000000 ┆ 2023-01-02T12:00:00 ┆ 2023/01/02 12:00:00 │
            ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 2023-01-03 ┆ 01/03/2023  ┆ 2023-01-03T12:00:00.999999 ┆ 2023-01-03T12:00:00 ┆ 2023/01/03 12:00:00 │
            ╰────────────┴─────────────┴────────────────────────────┴─────────────────────┴─────────────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)
        """
        return self._eval_expressions("strftime", format=format)

    def total_seconds(self) -> Expression:
        """Calculates the total number of seconds for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of seconds for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Seconds", daft.col("duration").dt.total_seconds()).show()
            ╭────────────────────────┬───────────────╮
            │ duration               ┆ Total Seconds │
            │ ---                    ┆ ---           │
            │ Duration[Microseconds] ┆ Int64         │
            ╞════════════════════════╪═══════════════╡
            │ 1s                     ┆ 1             │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 0             │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 0             │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 86400         │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 3600          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 60            │
            ╰────────────────────────┴───────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_seconds")

    def total_milliseconds(self) -> Expression:
        """Calculates the total number of milliseconds for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of milliseconds for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Milliseconds", daft.col("duration").dt.total_milliseconds()).show()
            ╭────────────────────────┬────────────────────╮
            │ duration               ┆ Total Milliseconds │
            │ ---                    ┆ ---                │
            │ Duration[Microseconds] ┆ Int64              │
            ╞════════════════════════╪════════════════════╡
            │ 1s                     ┆ 1000               │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 1                  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 0                  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 86400000           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 3600000            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 60000              │
            ╰────────────────────────┴────────────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_milliseconds")

    def total_microseconds(self) -> Expression:
        """Calculates the total number of microseconds for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of microseconds for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Microseconds", daft.col("duration").dt.total_microseconds()).show()
            ╭────────────────────────┬────────────────────╮
            │ duration               ┆ Total Microseconds │
            │ ---                    ┆ ---                │
            │ Duration[Microseconds] ┆ Int64              │
            ╞════════════════════════╪════════════════════╡
            │ 1s                     ┆ 1000000            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 1000               │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 1                  │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 86400000000        │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 3600000000         │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 60000000           │
            ╰────────────────────────┴────────────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_microseconds")

    def total_nanoseconds(self) -> Expression:
        """Calculates the total number of nanoseconds for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of nanoseconds for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Nanoseconds", daft.col("duration").dt.total_nanoseconds()).show()
            ╭────────────────────────┬───────────────────╮
            │ duration               ┆ Total Nanoseconds │
            │ ---                    ┆ ---               │
            │ Duration[Microseconds] ┆ Int64             │
            ╞════════════════════════╪═══════════════════╡
            │ 1s                     ┆ 1000000000        │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 1000000           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 1000              │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 86400000000000    │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 3600000000000     │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 60000000000       │
            ╰────────────────────────┴───────────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_nanoseconds")

    def total_minutes(self) -> Expression:
        """Calculates the total number of minutes for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of minutes for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Minutes", daft.col("duration").dt.total_minutes()).show()
            ╭────────────────────────┬───────────────╮
            │ duration               ┆ Total Minutes │
            │ ---                    ┆ ---           │
            │ Duration[Microseconds] ┆ Int64         │
            ╞════════════════════════╪═══════════════╡
            │ 1s                     ┆ 0             │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 0             │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 0             │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 1440          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 60            │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 1             │
            ╰────────────────────────┴───────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_minutes")

    def total_hours(self) -> Expression:
        """Calculates the total number of hours for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of hours for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Hours", daft.col("duration").dt.total_hours()).show()
            ╭────────────────────────┬─────────────╮
            │ duration               ┆ Total Hours │
            │ ---                    ┆ ---         │
            │ Duration[Microseconds] ┆ Int64       │
            ╞════════════════════════╪═════════════╡
            │ 1s                     ┆ 0           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 0           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 0           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 24          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 1           │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 0           │
            ╰────────────────────────┴─────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_hours")

    def total_days(self) -> Expression:
        """Calculates the total number of days for a duration column.

        Returns:
            Expression: a UInt64 expression with the total number of days for a duration column

        Examples:
            >>> import daft
            >>> from datetime import date, datetime, time, timedelta
            >>> df = daft.from_pydict(
            ...     {
            ...         "duration": [
            ...             timedelta(seconds=1),
            ...             timedelta(milliseconds=1),
            ...             timedelta(microseconds=1),
            ...             timedelta(days=1),
            ...             timedelta(hours=1),
            ...             timedelta(minutes=1),
            ...         ]
            ...     }
            ... )
            >>> df.with_column("Total Days", daft.col("duration").dt.total_days()).show()
            ╭────────────────────────┬────────────╮
            │ duration               ┆ Total Days │
            │ ---                    ┆ ---        │
            │ Duration[Microseconds] ┆ Int64      │
            ╞════════════════════════╪════════════╡
            │ 1s                     ┆ 0          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1000µs                 ┆ 0          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1µs                    ┆ 0          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1d                     ┆ 1          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1h                     ┆ 0          │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1m                     ┆ 0          │
            ╰────────────────────────┴────────────╯
            <BLANKLINE>
            (Showing first 6 of 6 rows)
        """
        return self._eval_expressions("total_days")


class ExpressionStringNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.str` attribute."""

    def contains(self, substr: str | Expression) -> Expression:
        """Checks whether each string contains the given pattern in a string column.

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value contains the provided pattern

        Examples:
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

        """
        substr_expr = Expression._to_expression(substr)._expr
        f = native.get_function_from_registry("utf8_contains")
        return Expression._from_pyexpr(f(self._expr, substr_expr))

    def match(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given regular expression pattern in a string column.

        Args:
            pattern: Regex pattern to search for as string or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern

        Examples:
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

        """
        pattern_expr = Expression._to_expression(pattern)
        f = native.get_function_from_registry("regexp_match")
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr))

    def endswith(self, suffix: str | Expression) -> Expression:
        """Checks whether each string ends with the given pattern in a string column.

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value ends with the provided pattern

        Examples:
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

        """
        suffix_expr = Expression._to_expression(suffix)._expr
        f = native.get_function_from_registry("ends_with")

        return Expression._from_pyexpr(f(self._expr, suffix_expr))

    def startswith(self, prefix: str | Expression) -> Expression:
        """Checks whether each string starts with the given pattern in a string column.

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value starts with the provided pattern

        Examples:
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

        """
        prefix_expr = Expression._to_expression(prefix)._expr
        f = native.get_function_from_registry("starts_with")

        return Expression._from_pyexpr(f(self._expr, prefix_expr))

    def split(self, pattern: str | Expression, regex: bool = False) -> Expression:
        r"""Splits each string on the given literal or regex pattern, into a list of strings.

        Args:
            pattern: The pattern on which each string should be split, or a column to pick such patterns from.
            regex: Whether the pattern is a regular expression. Defaults to False.

        Returns:
            Expression: A List[Utf8] expression containing the string splits for each string in the column.

        Examples:
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


        """
        pattern_expr = Expression._to_expression(pattern)
        f_name = "regexp_split" if regex else "split"
        f = native.get_function_from_registry(f_name)
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr))

    def concat(self, other: str | Expression) -> Expression:
        """Concatenates two string expressions together.

        Args:
            other (Expression): a string expression to concatenate with

        Returns:
            Expression: a String expression which is `self` concatenated with `other`

        Note:
            Another (easier!) way to invoke this functionality is using the Python `+` operator which is
            aliased to using `.str.concat`. These are equivalent:

        Examples:
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

        """
        # Delegate to + operator implementation.
        other_expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr) + other_expr

    def extract(self, pattern: str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from the first regex match in each string in a string column.

        Args:
            pattern: The regex pattern to extract
            index: The index of the regex match group to extract

        Returns:
            Expression: a String expression with the extracted regex match

        Note:
            If index is 0, the entire match is returned.
            If the pattern does not match or the group does not exist, a null value is returned.

        Examples:
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


        Tip: See Also
            [extract_all](https://docs.getdaft.io/en/stable/api/expressions/#daft.expressions.expressions.ExpressionStringNamespace.extract_all)
        """
        pattern_expr = Expression._to_expression(pattern)
        idx = Expression._to_expression(index)
        f = native.get_function_from_registry("regexp_extract")
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr, idx._expr))

    def extract_all(self, pattern: str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from all regex matches in each string in a string column.

        Args:
            pattern: The regex pattern to extract
            index: The index of the regex match group to extract

        Returns:
            Expression: a List[Utf8] expression with the extracted regex matches

        Note:
            This expression always returns a list of strings.
            If index is 0, the entire match is returned. If the pattern does not match or the group does not exist, an empty list is returned.

        Examples:
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

        Tip: See Also
            [extract](https://docs.getdaft.io/en/stable/api/expressions/#daft.expressions.expressions.ExpressionStringNamespace.extract)
        """
        pattern_expr = Expression._to_expression(pattern)
        idx = Expression._to_expression(index)
        f = native.get_function_from_registry("regexp_extract_all")
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr, idx._expr))

    def replace(
        self,
        pattern: str | Expression,
        replacement: str | Expression,
        regex: bool = False,
    ) -> Expression:
        """Replaces all occurrences of a pattern in a string column with a replacement string. The pattern can be a literal string or a regex pattern.

        Args:
            pattern: The pattern to replace
            replacement: The replacement string
            regex: Whether the pattern is a regex pattern or an exact match. Defaults to False.

        Returns:
            Expression: a String expression with patterns replaced by the replacement string

        Examples:
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

        """
        pattern_expr = Expression._to_expression(pattern)
        replacement_expr = Expression._to_expression(replacement)
        if regex:
            f_name = "regexp_replace"
        else:
            f_name = "replace"
        f = native.get_function_from_registry(f_name)
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr, replacement_expr._expr))

    def length(self) -> Expression:
        """Retrieves the length for a UTF-8 string column.

        Returns:
            Expression: an UInt64 expression with the length of each string

        Examples:
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

        """
        f = native.get_function_from_registry("length")
        return Expression._from_pyexpr(f(self._expr))

    def length_bytes(self) -> Expression:
        """Retrieves the length for a UTF-8 string column in bytes.

        Returns:
            Expression: an UInt64 expression with the length of each string

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["😉test", "hey̆", "baz"]})
            >>> df = df.select(df["x"].str.length_bytes())
            >>> df.show()
            ╭────────╮
            │ x      │
            │ ---    │
            │ UInt64 │
            ╞════════╡
            │ 8      │
            ├╌╌╌╌╌╌╌╌┤
            │ 5      │
            ├╌╌╌╌╌╌╌╌┤
            │ 3      │
            ╰────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        f = native.get_function_from_registry("length_bytes")
        return Expression._from_pyexpr(f(self._expr))

    def lower(self) -> Expression:
        """Convert UTF-8 string to all lowercase.

        Returns:
            Expression: a String expression which is `self` lowercased

        Examples:
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

        """
        f = native.get_function_from_registry("lower")
        return Expression._from_pyexpr(f(self._expr))

    def upper(self) -> Expression:
        """Convert UTF-8 string to all upper.

        Returns:
            Expression: a String expression which is `self` uppercased

        Examples:
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

        """
        f = native.get_function_from_registry("upper")
        return Expression._from_pyexpr(f(self._expr))

    def lstrip(self) -> Expression:
        """Strip whitespace from the left side of a UTF-8 string.

        Returns:
            Expression: a String expression which is `self` with leading whitespace stripped

        Examples:
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

        """
        f = native.get_function_from_registry("lstrip")
        return Expression._from_pyexpr(f(self._expr))

    def rstrip(self) -> Expression:
        """Strip whitespace from the right side of a UTF-8 string.

        Returns:
            Expression: a String expression which is `self` with trailing whitespace stripped

        Examples:
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

        """
        f = native.get_function_from_registry("rstrip")
        return Expression._from_pyexpr(f(self._expr))

    def reverse(self) -> Expression:
        """Reverse a UTF-8 string.

        Returns:
            Expression: a String expression which is `self` reversed

        Examples:
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

        """
        f = native.get_function_from_registry("reverse")
        return Expression._from_pyexpr(f(self._expr))

    def capitalize(self) -> Expression:
        """Capitalize a UTF-8 string.

        Returns:
            Expression: a String expression which is `self` uppercased with the first character and lowercased the rest

        Examples:
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

        """
        f = native.get_function_from_registry("capitalize")
        return Expression._from_pyexpr(f(self._expr))

    def left(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) left-most characters of each string.

        Returns:
            Expression: a String expression which is the `n` left-most characters of `self`

        Examples:
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

        """
        nchars_expr = Expression._to_expression(nchars)
        f = native.get_function_from_registry("left")
        return Expression._from_pyexpr(f(self._expr, nchars_expr._expr))

    def right(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) right-most characters of each string.

        Returns:
            Expression: a String expression which is the `n` right-most characters of `self`

        Examples:
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

        """
        nchars_expr = Expression._to_expression(nchars)
        f = native.get_function_from_registry("right")
        return Expression._from_pyexpr(f(self._expr, nchars_expr._expr))

    def find(self, substr: str | Expression) -> Expression:
        """Returns the index of the first occurrence of the substring in each string.

        Returns:
            Expression: an Int64 expression with the index of the first occurrence of the substring in each string

        Note:
            The returned index is 0-based. If the substring is not found, -1 is returned.

        Examples:
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

        """
        substr_expr = Expression._to_expression(substr)
        f = native.get_function_from_registry("find")
        return Expression._from_pyexpr(f(self._expr, substr_expr._expr))

    def rpad(self, length: int | Expression, pad: str | Expression) -> Expression:
        """Right-pads each string by truncating or padding with the character.

        Returns:
            Expression: a String expression which is `self` truncated or right-padded with the pad character

        Note:
            If the string is longer than the specified length, it will be truncated.
            The pad character must be a single character.

        Examples:
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

        """
        length_expr = Expression._to_expression(length)
        pad_expr = Expression._to_expression(pad)
        f = native.get_function_from_registry("rpad")

        return Expression._from_pyexpr(f(self._expr, length_expr._expr, pad_expr._expr))

    def lpad(self, length: int | Expression, pad: str | Expression) -> Expression:
        """Left-pads each string by truncating on the right or padding with the character.

        Returns:
            Expression: a String expression which is `self` truncated or left-padded with the pad character

        Note:
            If the string is longer than the specified length, it will be truncated on the right.
            The pad character must be a single character.

        Examples:
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

        """
        length_expr = Expression._to_expression(length)
        pad_expr = Expression._to_expression(pad)
        f = native.get_function_from_registry("lpad")
        return Expression._from_pyexpr(f(self._expr, length_expr._expr, pad_expr._expr))

    def repeat(self, n: int | Expression) -> Expression:
        """Repeats each string n times.

        Returns:
            Expression: a String expression which is `self` repeated `n` times

        Examples:
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

        """
        n_expr = Expression._to_expression(n)
        f = native.get_function_from_registry("repeat")
        return Expression._from_pyexpr(f(self._expr, n_expr._expr))

    def like(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given SQL LIKE pattern, case sensitive.

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern

        Note:
            Use % as a multiple-character wildcard or _ as a single-character wildcard.

        Examples:
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

        """
        pattern_expr = Expression._to_expression(pattern)
        f = native.get_function_from_registry("like")
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr))

    def ilike(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given SQL LIKE pattern, case insensitive.

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern

        Note:
            Use % as a multiple-character wildcard or _ as a single-character wildcard.

        Examples:
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

        """
        pattern_expr = Expression._to_expression(pattern)
        f = native.get_function_from_registry("ilike")
        return Expression._from_pyexpr(f(self._expr, pattern_expr._expr))

    def substr(self, start: int | Expression, length: int | Expression | None = None) -> Expression:
        """Extract a substring from a string, starting at a specified index and extending for a given length.

        Returns:
            Expression: A String expression representing the extracted substring.

        Note:
            If `length` is not provided, the substring will include all characters from `start` to the end of the string.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["daft", "query", "engine"]})
            >>> df = df.select(df["x"].str.substr(2, 4))
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

        """
        start_expr = Expression._to_expression(start)
        length_expr = Expression._to_expression(length)
        f = native.get_function_from_registry("substr")
        return Expression._from_pyexpr(f(self._expr, start_expr._expr, length_expr._expr))

    def to_date(self, format: str) -> Expression:
        """Converts a string to a date using the specified format.

        Returns:
            Expression: a Date expression which is parsed by given format

        Note:
            The format must be a valid date format string. See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

        Examples:
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

        """
        format_expr = Expression._to_expression(format)._expr
        f = native.get_function_from_registry("to_date")
        return Expression._from_pyexpr(f(self._expr, format=format_expr))

    def to_datetime(self, format: str, timezone: str | None = None) -> Expression:
        """Converts a string to a datetime using the specified format and timezone.

        Returns:
            Expression: a DateTime expression which is parsed by given format and timezone

        Note:
            The format must be a valid datetime format string. See: https://docs.rs/chrono/latest/chrono/format/strftime/index.html

        Examples:
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
            >>> df = df.with_column(
            ...     "datetime", df["x"].str.to_datetime("%Y-%m-%d %H:%M:%S%.3f %z", timezone="Asia/Shanghai")
            ... )
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

        """
        format_expr = Expression._to_expression(format)._expr
        timezone_expr = Expression._to_expression(timezone)._expr
        f = native.get_function_from_registry("to_datetime")
        return Expression._from_pyexpr(f(self._expr, format=format_expr, timezone=timezone_expr))

    def normalize(
        self,
        *,
        remove_punct: bool = False,
        lowercase: bool = False,
        nfd_unicode: bool = False,
        white_space: bool = False,
    ) -> Expression:
        r"""Normalizes a string for more useful deduplication.

        Args:
            remove_punct: Whether to remove all punctuation (ASCII).
            lowercase: Whether to convert the string to lowercase.
            nfd_unicode: Whether to normalize and decompose Unicode characters according to NFD.
            white_space: Whether to normalize whitespace, replacing newlines etc with spaces and removing double spaces.

        Returns:
            Expression: a String expression which is normalized.

        Note:
            All processing options are off by default.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": ["hello world", "Hello, world!", "HELLO,   \nWORLD!!!!"]})
            >>> df = df.with_column(
            ...     "normalized", df["x"].str.normalize(remove_punct=True, lowercase=True, white_space=True)
            ... )
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

        """
        remove_punct_expr = Expression._to_expression(remove_punct)._expr
        lowercase_expr = Expression._to_expression(lowercase)._expr
        nfd_unicode_expr = Expression._to_expression(nfd_unicode)._expr
        white_space_expr = Expression._to_expression(white_space)._expr
        f = native.get_function_from_registry("normalize")
        return Expression._from_pyexpr(
            f(
                self._expr,
                remove_punct=remove_punct_expr,
                lowercase=lowercase_expr,
                nfd_unicode=nfd_unicode_expr,
                white_space=white_space_expr,
            )
        )

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

        Args:
            tokens_path: The name of a built-in tokenizer, or the path to a token file (supports downloading).
            io_config (optional): IOConfig to use when accessing remote storage.
            pattern (optional): Regex pattern to use to split strings in tokenization step. Necessary if loading from a file.
            special_tokens (optional): Name of the set of special tokens to use. Currently only "llama3" supported. Necessary if loading from a file.
            use_special_tokens (optional): Whether or not to parse special tokens included in input. Disabled by default. Automatically enabled if `special_tokens` is provided.

        Returns:
            Expression: An expression with the encodings of the strings as lists of unsigned 32-bit integers.

        Note:
            If using this expression with Llama 3 tokens, note that Llama 3 does some extra preprocessing on
            strings in certain edge cases. This may result in slightly different encodings in these cases.

        """
        return self._eval_expressions(
            "tokenize_encode",
            tokens_path=tokens_path,
            use_special_tokens=use_special_tokens,
            io_config=io_config,
            pattern=pattern,
            special_tokens=special_tokens,
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

        Uses [https://github.com/openai/tiktoken](https://github.com/openai/tiktoken) for tokenization.

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
        return self._eval_expressions(
            "tokenize_decode",
            tokens_path=tokens_path,
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

        Note:
            If a pattern is a substring of another pattern, the longest pattern is matched first.
            For example, in the string "hello world", with patterns "hello", "world", and "hello world",
            one match is counted for "hello world".
        """
        if isinstance(patterns, str):
            patterns = [patterns]
        if not isinstance(patterns, Expression):
            series = item_to_series("items", patterns)
            patterns = Expression._from_pyexpr(_series_lit(series._series))

        whole_words_expr = Expression._to_expression(whole_words)._expr
        case_sensitive_expr = Expression._to_expression(case_sensitive)._expr
        f = native.get_function_from_registry("count_matches")

        return Expression._from_pyexpr(
            f(self._expr, patterns._expr, whole_words=whole_words_expr, case_sensitive=case_sensitive_expr)
        )


class ExpressionListNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.list` attribute."""

    def map(self, expr: Expression) -> Expression:
        """Evaluates an expression on all elements in the list.

        Args:
            expr: Expression to run.  you can select the element with `daft.element()`
        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
            >>> df.with_column("letters_capitalized", df["letters"].list.map(daft.element().str.upper())).collect()
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
        return self._eval_expressions("list_map", expr)

    def join(self, delimiter: str | Expression) -> Expression:
        """Joins every element of a list using the specified string delimiter.

        Args:
            delimiter (str | Expression): the delimiter to use to join lists with

        Returns:
            Expression: a String expression which is every element of the list joined on the delimiter
        """
        return self._eval_expressions("list_join", delimiter)

    def value_counts(self) -> Expression:
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
            >>> df = daft.from_pydict({"letters": [["a", "b", "a"], ["b", "c", "b", "c"]]})
            >>> df.with_column("value_counts", df["letters"].list.value_counts()).collect()
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
        return self._eval_expressions("list_value_counts")

    def count(self, mode: Literal["all", "valid", "null"] | CountMode = CountMode.Valid) -> Expression:
        """Counts the number of elements in each list.

        Args:
            mode: A string ("all", "valid", or "null") that represents whether to count all values, non-null (valid) values, or null values. Defaults to "valid".

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        return self._eval_expressions("list_count", mode)

    def lengths(self) -> Expression:
        """Gets the length of each list.

        (DEPRECATED) Please use Expression.list.length instead

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        warnings.warn(
            "This function will be deprecated from Daft version >= 0.3.5!  Instead, please use 'Expression.list.length'",
            category=DeprecationWarning,
        )

        return self._eval_expressions("list_count", CountMode.All)

    def length(self) -> Expression:
        """Gets the length of each list.

        Returns:
            Expression: a UInt64 expression which is the length of each list
        """
        return self._eval_expressions("list_count", CountMode.All)

    def get(self, idx: int | Expression, default: object = None) -> Expression:
        """Gets the element at an index in each list.

        Args:
            idx: index or indices to retrieve from each list
            default: the default value if the specified index is out of bounds

        Returns:
            Expression: an expression with the type of the list values
        """
        return self._eval_expressions("list_get", idx, default)

    def slice(self, start: int | Expression, end: int | Expression | None = None) -> Expression:
        """Gets a subset of each list.

        Args:
            start: index or column of indices. The slice will include elements starting from this index. If `start` is negative, it represents an offset from the end of the list
            end: optional index or column of indices. The slice will not include elements from this index onwards. If `end` is negative, it represents an offset from the end of the list. If not provided, the slice will include elements up to the end of the list

        Returns:
            Expression: an expression with a list of the type of the list values
        """
        return self._eval_expressions("list_slice", start, end)

    def chunk(self, size: int) -> Expression:
        """Splits each list into chunks of the given size.

        Args:
            size: size of chunks to split the list into. Must be greater than 0
        Returns:
            Expression: an expression with lists of fixed size lists of the type of the list values
        """
        return self._eval_expressions("list_chunk", size)

    def sum(self) -> Expression:
        """Sums each list. Empty lists and lists with all nulls yield null.

        Returns:
            Expression: an expression with the type of the list values
        """
        return self._eval_expressions("list_sum")

    def mean(self) -> Expression:
        """Calculates the mean of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return self._eval_expressions("list_mean")

    def min(self) -> Expression:
        """Calculates the minimum of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return self._eval_expressions("list_min")

    def max(self) -> Expression:
        """Calculates the maximum of each list. If no non-null values in a list, the result is null.

        Returns:
            Expression: a Float64 expression with the type of the list values
        """
        return self._eval_expressions("list_max")

    def bool_and(self) -> Expression:
        """Calculates the boolean AND of all values in a list.

        For each list:
        - Returns True if all non-null values are True
        - Returns False if any non-null value is False
        - Returns null if the list is empty or contains only null values

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"values": [[True, True], [True, False], [None, None], []]})
            >>> df.with_column("result", df["values"].list.bool_and()).collect()
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
        return self._eval_expressions("list_bool_and")

    def bool_or(self) -> Expression:
        """Calculates the boolean OR of all values in a list.

        For each list:
        - Returns True if any non-null value is True
        - Returns False if all non-null values are False
        - Returns null if the list is empty or contains only null values

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"values": [[True, False], [False, False], [None, None], []]})
            >>> df.with_column("result", df["values"].list.bool_or()).collect()
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
        return self._eval_expressions("list_bool_or")

    def sort(self, desc: bool | Expression | None = None, nulls_first: bool | Expression | None = None) -> Expression:
        """Sorts the inner lists of a list column.

        Args:
            desc: Whether to sort in descending order. Defaults to false. Pass in a boolean column to control for each row.

        Returns:
            Expression: An expression with the sorted lists

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [[1, 3], [4, 2], [6, 7, 1]]})
            >>> df.select(df["a"].list.sort()).show()
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
        return self._eval_expressions("list_sort", desc=desc, nulls_first=nulls_first)

    def distinct(self) -> Expression:
        """Returns a list of distinct elements in each list, preserving order of first occurrence and ignoring nulls.

        Returns:
            Expression: An expression with lists containing only distinct elements

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [[1, 2, 2, 3], [4, 4, 6, 2], [6, 7, 1], [None, 1, None, 1]]})
            >>> df.select(df["a"].list.distinct()).show()
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
            >>> df.select(df["a"].list.distinct()).show()
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
        return self._eval_expressions("list_distinct")

    def unique(self) -> Expression:
        """Returns a list of distinct elements in each list, preserving order of first occurrence and ignoring nulls.

        Alias for [Expression.list.distinct](https://docs.getdaft.io/en/stable/api/expressions/#daft.expressions.expressions.ExpressionListNamespace.distinct).

        Returns:
            Expression: An expression with lists containing only distinct elements

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"a": [[1, 2, 2, 3], [4, 4, 6, 2], [6, 7, 1], [None, 1, None, 1]]})
            >>> df.select(df["a"].list.unique()).show()
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
            >>> df.select(df["a"].list.unique()).show()
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

        Tip: See Also
            [Expression.list.distinct](https://docs.getdaft.io/en/stable/api/expressions/#daft.expressions.expressions.ExpressionListNamespace.distinct)

        """
        return self.distinct()


class ExpressionStructNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.struct` attribute."""

    def get(self, name: str) -> Expression:
        """Retrieves one field from a struct column, or all fields with "*".

        Args:
            name: the name of the field to retrieve

        Returns:
            Expression: the field expression
        """
        return Expression._from_pyexpr(self._expr.struct_get(name))


class ExpressionMapNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.map` attribute."""

    def get(self, key: Expression) -> Expression:
        """Retrieves the value for a key in a map column.

        Args:
            key: the key to retrieve

        Returns:
            Expression: the value expression

        Examples:
            >>> import pyarrow as pa
            >>> import daft
            >>> pa_array = pa.array([[("a", 1)], [], [("b", 2)]], type=pa.map_(pa.string(), pa.int64()))
            >>> df = daft.from_arrow(pa.table({"map_col": pa_array}))
            >>> df = df.with_column("a", df["map_col"].map.get("a"))
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
        return Expression._from_pyexpr(self._expr.map_get(key_expr._expr))


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


class ExpressionImageNamespace(ExpressionNamespace):
    """Expression operations for image columns. The following methods are available under the `expr.image` attribute."""

    def decode(
        self,
        on_error: Literal["raise", "null"] = "raise",
        mode: str | ImageMode | None = None,
    ) -> Expression:
        """Decodes the binary data in this column into images.

        This can only be applied to binary columns that contain encoded images (e.g. PNG, JPEG, etc.)

        Args:
            on_error: Whether to raise when encountering an error, or log a warning and return a null
            mode: What mode to convert the images into before storing it in the column. This may prevent
                errors relating to unsupported types.

        Returns:
            Expression: An Image expression represnting an image column.
        """
        image_mode = Expression._to_expression(mode)._expr
        raise_on_error = lit(on_error)._expr
        f = native.get_function_from_registry("image_decode")

        return Expression._from_pyexpr(f(self._expr, on_error=raise_on_error, mode=image_mode))

    def encode(self, image_format: str | ImageFormat) -> Expression:
        """Encode an image column as the provided image file format, returning a binary column of encoded bytes.

        Args:
            image_format: The image file format into which the images will be encoded.

        Returns:
            Expression: A Binary expression representing a binary column of encoded image bytes.
        """
        if isinstance(image_format, str):
            image_format = ImageFormat.from_format_string(image_format.upper())
        if not isinstance(image_format, ImageFormat):
            raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
        f = native.get_function_from_registry("image_encode")
        image_format_expr = lit(image_format)._expr
        return Expression._from_pyexpr(f(self._expr, image_format=image_format_expr))

    def resize(self, w: int, h: int) -> Expression:
        """Resize image into the provided width and height.

        Args:
            w: Desired width of the resized image.
            h: Desired height of the resized image.

        Returns:
            Expression: An Image expression representing an image column of the resized images.
        """
        width = lit(w)._expr
        height = lit(h)._expr
        f = native.get_function_from_registry("image_resize")
        return Expression._from_pyexpr(f(self._expr, w=width, h=height))

    def crop(self, bbox: tuple[int, int, int, int] | Expression) -> Expression:
        """Crops images with the provided bounding box.

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
        f = native.get_function_from_registry("image_crop")
        return Expression._from_pyexpr(f(self._expr, bbox._expr))

    def to_mode(self, mode: str | ImageMode) -> Expression:
        if isinstance(mode, str):
            mode = ImageMode.from_mode_string(mode.upper())
        if not isinstance(mode, ImageMode):
            raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
        image_mode = lit(mode)._expr
        f = native.get_function_from_registry("to_mode")
        return Expression._from_pyexpr(f(self._expr, mode=image_mode))


class ExpressionPartitioningNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.partition` attribute."""

    def days(self) -> Expression:
        """Partitioning Transform that returns the number of days since epoch (1970-01-01).

        Unlike other temporal partitioning expressions, this expression is date type instead of int. This is to conform to the behavior of other implementations of Iceberg partition transforms.

        Returns:
            Date Expression
        """
        return Expression._from_pyexpr(self._expr.partitioning_days())

    def hours(self) -> Expression:
        """Partitioning Transform that returns the number of hours since epoch (1970-01-01).

        Returns:
            Expression: Int32 Expression in hours
        """
        return Expression._from_pyexpr(self._expr.partitioning_hours())

    def months(self) -> Expression:
        """Partitioning Transform that returns the number of months since epoch (1970-01-01).

        Returns:
            Expression: Int32 Expression in months
        """
        return Expression._from_pyexpr(self._expr.partitioning_months())

    def years(self) -> Expression:
        """Partitioning Transform that returns the number of years since epoch (1970-01-01).

        Returns:
            Expression: Int32 Expression in years
        """
        return Expression._from_pyexpr(self._expr.partitioning_years())

    def iceberg_bucket(self, n: int) -> Expression:
        """Partitioning Transform that returns the Hash Bucket following the Iceberg Specification of murmur3_32_x86.

        See <https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements> for more details.

        Args:
            n (int): Number of buckets

        Returns:
            Expression: Int32 Expression with the Hash Bucket
        """
        return Expression._from_pyexpr(self._expr.partitioning_iceberg_bucket(n))

    def iceberg_truncate(self, w: int) -> Expression:
        """Partitioning Transform that truncates the input to a standard width `w` following the Iceberg Specification.

        See <https://iceberg.apache.org/spec/#truncate-transform-details> for more details.

        Args:
            w (int): width of the truncation

        Returns:
            Expression: Expression of the Same Type of the input
        """
        return Expression._from_pyexpr(self._expr.partitioning_iceberg_truncate(w))


class ExpressionJsonNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.json` attribute."""

    def query(self, jq_query: str) -> Expression:
        """Query JSON data in a column using a JQ-style filter <https://jqlang.github.io/jq/manual/>.

        This expression uses jaq as the underlying executor, see <https://github.com/01mf02/jaq> for the full list of supported filters.

        Args:
            jq_query (str): JQ query string

        Returns:
            Expression: Expression representing the result of the JQ query as a column of JSON-compatible strings

        Examples:
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

        """
        warnings.warn(
            "This API is deprecated in daft >=0.5.1 and will be removed in >=0.6.0. Users should use `Expression.jq` instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        f = native.get_function_from_registry("jq")
        filter = Expression._to_expression(jq_query)._expr

        return Expression._from_pyexpr(f(self._expr, filter))


class ExpressionEmbeddingNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.embedding` attribute."""

    def cosine_distance(self, other: Expression) -> Expression:
        """Compute the cosine distance between two embeddings.

        Args:
            other (Expression): The other embedding to compute the cosine distance against.

        Returns:
            Expression: a Float64 Expression with the cosine distance between the two embeddings.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"e1": [[1, 2, 3], [1, 2, 3]], "e2": [[1, 2, 3], [-1, -2, -3]]})
            >>> dtype = daft.DataType.fixed_size_list(daft.DataType.float32(), 3)
            >>> df = df.with_column("dist", df["e1"].cast(dtype).embedding.cosine_distance(df["e2"].cast(dtype)))
            >>> df.show()
            ╭─────────────┬──────────────┬─────────╮
            │ e1          ┆ e2           ┆ dist    │
            │ ---         ┆ ---          ┆ ---     │
            │ List[Int64] ┆ List[Int64]  ┆ Float64 │
            ╞═════════════╪══════════════╪═════════╡
            │ [1, 2, 3]   ┆ [1, 2, 3]    ┆ 0       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ [1, 2, 3]   ┆ [-1, -2, -3] ┆ 2       │
            ╰─────────────┴──────────────┴─────────╯
            <BLANKLINE>
            (Showing first 2 of 2 rows)

        """
        return self._eval_expressions("cosine_distance", other)


class ExpressionBinaryNamespace(ExpressionNamespace):
    """The following methods are available under the `expr.binary` attribute."""

    def length(self) -> Expression:
        """Retrieves the length for a binary string column.

        Returns:
            Expression: an UInt64 expression with the length of each binary string in bytes

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [b"foo", b"bar", b"baz"]})
            >>> df = df.select(df["x"].binary.length())
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

        """
        return self._eval_expressions("binary_length")

    def concat(self, other: Expression) -> Expression:
        r"""Concatenates two binary strings.

        Args:
            other: The binary string to concatenate with, can be either an Expression or a bytes literal

        Returns:
            Expression: A binary expression containing the concatenated strings

        Examples:
            >>> import daft
            >>> df = daft.from_pydict(
            ...     {"a": [b"Hello", b"\\xff\\xfe", b"", b"World"], "b": [b" World", b"\\x00", b"empty", b"!"]}
            ... )
            >>> df = df.select(df["a"].binary.concat(df["b"]))
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
        return self._eval_expressions("binary_concat", other)

    def slice(self, start: Expression | int, length: Expression | int | None = None) -> Expression:
        r"""Returns a slice of each binary string.

        Args:
            start: The starting position (0-based) of the slice.
            length: The length of the slice. If None, returns all characters from start to the end.

        Returns:
            A new expression representing the slice.

        Examples:
            >>> import daft
            >>> df = daft.from_pydict({"x": [b"Hello World", b"\xff\xfe\x00", b"empty"]})
            >>> df = df.select(df["x"].binary.slice(1, 3))
            >>> df.show()
            ╭─────────────╮
            │ x           │
            │ ---         │
            │ Binary      │
            ╞═════════════╡
            │ b"ell"      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b"\xfe\x00" │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ b"mpt"      │
            ╰─────────────╯
            <BLANKLINE>
            (Showing first 3 of 3 rows)

        """
        return self._eval_expressions("binary_slice", start, length)
