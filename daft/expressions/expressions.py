from __future__ import annotations

import builtins
import os
import sys
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING, Callable, Iterable, Iterator, TypeVar, overload

import pyarrow as pa

from daft.daft import CountMode
from daft.daft import PyExpr as _PyExpr
from daft.daft import col as _col
from daft.daft import date_lit as _date_lit
from daft.daft import decimal_lit as _decimal_lit
from daft.daft import lit as _lit
from daft.daft import series_lit as _series_lit
from daft.daft import time_lit as _time_lit
from daft.daft import timestamp_lit as _timestamp_lit
from daft.daft import udf as _udf
from daft.datatype import DataType, TimeUnit
from daft.expressions.datetime import ExpressionDatetimeNamespace
from daft.expressions.float import ExpressionFloatNamespace
from daft.expressions.image import ExpressionImageNamespace
from daft.expressions.json import ExpressionJsonNamespace
from daft.expressions.list import ExpressionListNamespace
from daft.expressions.partitioning import ExpressionPartitioningNamespace
from daft.expressions.string import ExpressionStringNamespace
from daft.expressions.struct import ExpressionStructNamespace
from daft.expressions.testing import expr_structurally_equal
from daft.expressions.url import ExpressionUrlNamespace
from daft.logical.schema import Field, Schema
from daft.series.series import Series, item_to_series

if sys.version_info < (3, 8):
    pass
else:
    pass

if TYPE_CHECKING:
    pass


# Implementation taken from: https://github.com/pola-rs/polars/blob/main/py-polars/polars/utils/various.py#L388-L399
# This allows Sphinx to correctly work against our "namespaced" accessor functions by overriding @property to
# return a class instance of the namespace instead of a property object.
accessor_namespace_property: type[property] = property
if os.getenv("DAFT_SPHINX_BUILD") == "1":
    from typing import Any

    # when building docs (with Sphinx) we need access to the functions
    # associated with the namespaces from the class, as we don't have
    # an instance; @sphinx_accessor is a @property that allows this.
    NS = TypeVar("NS")

    class sphinx_accessor(property):  # noqa: D101
        def __get__(  # type: ignore[override]
            self,
            instance: Any,
            cls: type[NS],
        ) -> NS:
            try:
                return self.fget(instance if isinstance(instance, cls) else cls)  # type: ignore[misc]
            except (AttributeError, ImportError):
                return self  # type: ignore[return-value]

    accessor_namespace_property = sphinx_accessor


def lit(value: object) -> Expression:
    """Creates an Expression representing a column with every value set to the provided value

    Example:
        >>> col("x") + lit(1)

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
        >>> col("x")

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

    @accessor_namespace_property
    def str(self) -> ExpressionStringNamespace:
        """Access methods that work on columns of strings"""
        return ExpressionStringNamespace.from_expression(self)

    @accessor_namespace_property
    def dt(self) -> ExpressionDatetimeNamespace:
        """Access methods that work on columns of datetimes"""
        return ExpressionDatetimeNamespace.from_expression(self)

    @accessor_namespace_property
    def float(self) -> ExpressionFloatNamespace:
        """Access methods that work on columns of floats"""
        return ExpressionFloatNamespace.from_expression(self)

    @accessor_namespace_property
    def url(self) -> ExpressionUrlNamespace:
        """Access methods that work on columns of URLs"""
        return ExpressionUrlNamespace.from_expression(self)

    @accessor_namespace_property
    def list(self) -> ExpressionListNamespace:
        """Access methods that work on columns of lists"""
        return ExpressionListNamespace.from_expression(self)

    @accessor_namespace_property
    def struct(self) -> ExpressionStructNamespace:
        """Access methods that work on columns of structs"""
        return ExpressionStructNamespace.from_expression(self)

    @accessor_namespace_property
    def image(self) -> ExpressionImageNamespace:
        """Access methods that work on columns of images"""
        return ExpressionImageNamespace.from_expression(self)

    @accessor_namespace_property
    def partitioning(self) -> ExpressionPartitioningNamespace:
        """Access methods that support partitioning operators"""
        return ExpressionPartitioningNamespace.from_expression(self)

    @accessor_namespace_property
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
        """Takes the logical AND of two boolean expressions (``e1 & e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr & expr._expr)

    def __rand__(self, other: Expression) -> Expression:
        """Takes the logical reverse AND of two boolean expressions (``e1 & e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(expr._expr & self._expr)

    def __or__(self, other: Expression) -> Expression:
        """Takes the logical OR of two boolean expressions (``e1 | e2``)"""
        expr = Expression._to_expression(other)
        return Expression._from_pyexpr(self._expr | expr._expr)

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

    def __invert__(self) -> Expression:
        """Inverts a boolean expression (``~e``)"""
        expr = self._expr.__invert__()
        return Expression._from_pyexpr(expr)

    def alias(self, name: builtins.str) -> Expression:
        """Gives the expression a new name, which is its column's name in the DataFrame schema and the name
        by which subsequent expressions can refer to the results of this expression.

        Example:
            >>> col("x").alias("y")

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

            >>> # [1.0, 2.5, None]: float32 -> [1, 2, None]: int64
            >>> col("float").cast(DataType.int64())
            >>>
            >>> # [Path("/tmp1"), Path("/tmp2"), Path("/tmp3")]: Python -> ["/tmp1", "/tmp1", "/tmp1"]: utf8
            >>> col("path_obj_col").cast(DataType.string())

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

    def radians(self) -> Expression:
        """The elementwise radians of a numeric expression (``expr.radians()``)"""
        expr = self._expr.radians()
        return Expression._from_pyexpr(expr)

    def degrees(self) -> Expression:
        """The elementwise degrees of a numeric expression (``expr.degrees()``)"""
        expr = self._expr.degrees()
        return Expression._from_pyexpr(expr)

    def exp(self) -> Expression:
        """The e^self of a numeric expression (``expr.exp()``)"""
        expr = self._expr.exp()
        return Expression._from_pyexpr(expr)

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
            >>> # x = [2, 2, 2]
            >>> # y = [1, 2, 3]
            >>> # a = ["a", "a", "a"]
            >>> # b = ["b", "b", "b"]
            >>> # if_else_result = ["a", "b", "b"]
            >>> (col("x") > col("y")).if_else(col("a"), col("b"))

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
            >>> def f(x_val: str) -> int:
            >>>     return int(x_val) if x_val.isnumeric() else 0
            >>>
            >>> col("x").apply(f, return_dtype=DataType.int64())

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
            >>> # [1., None, NaN] -> [False, True, False]
            >>> col("x").is_null()

        Returns:
            Expression: Boolean Expression indicating whether values are missing
        """
        expr = self._expr.is_null()
        return Expression._from_pyexpr(expr)

    def not_null(self) -> Expression:
        """Checks if values in the Expression are not Null (a special value indicating missing data)

        Example:
            >>> # [1., None, NaN] -> [True, False, True]
            >>> col("x").not_null()

        Returns:
            Expression: Boolean Expression indicating whether values are not missing
        """
        expr = self._expr.not_null()
        return Expression._from_pyexpr(expr)

    def fill_null(self, fill_value: Expression) -> Expression:
        """Fills null values in the Expression with the provided fill_value

        Example:
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

        Returns:
            Expression: Expression with null values filled with the provided fill_value
        """

        fill_value = Expression._to_expression(fill_value)
        expr = self._expr.fill_null(fill_value._expr)
        return Expression._from_pyexpr(expr)

    def is_in(self, other: Any) -> Expression:
        """Checks if values in the Expression are in the provided list

        Example:
            >>> # [1, 2, 3] -> [True, False, True]
            >>> col("x").is_in([1, 3])

        Returns:
            Expression: Boolean Expression indicating whether values are in the provided list
        """

        if not isinstance(other, Expression):
            series = item_to_series("items", other)
            other = Expression._to_expression(series)

        expr = self._expr.is_in(other._expr)
        return Expression._from_pyexpr(expr)

    def name(self) -> builtins.str:
        return self._expr.name()

    def __repr__(self) -> builtins.str:
        return repr(self._expr)

    def _to_sql(self, db_scheme: builtins.str) -> builtins.str:
        return self._expr.to_sql(db_scheme)

    def _to_field(self, schema: Schema) -> Field:
        return Field._from_pyfield(self._expr.to_field(schema._schema))

    def __hash__(self) -> int:
        return self._expr.__hash__()

    def __reduce__(self) -> tuple:
        return Expression._from_pyexpr, (self._expr,)

    ###
    # Helper methods required by optimizer:
    # These should be removed from the Python API for Expressions when logical plans and optimizer are migrated to Rust
    ###

    def _input_mapping(self) -> builtins.str | None:
        return self._expr._input_mapping()

    def _required_columns(self) -> set[builtins.str]:
        return self._expr._required_columns()

    def _is_column(self) -> bool:
        return self._expr._is_column()


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
    def __getitem__(self, idx: slice) -> list[Expression]:
        ...

    @overload
    def __getitem__(self, idx: int) -> Expression:
        ...

    def __getitem__(self, idx: int | slice) -> Expression | list[Expression]:
        # Relies on the fact that Python dictionaries are ordered
        return list(self._output_name_to_exprs.values())[idx]

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ExpressionsProjection):
            return False

        return len(self._output_name_to_exprs) == len(other._output_name_to_exprs) and all(
            (s.name() == o.name()) and expr_structurally_equal(s, o)
            for s, o in zip(self._output_name_to_exprs.values(), other._output_name_to_exprs.values())
        )

    def required_columns(self) -> set[str]:
        """Column names required to run this ExpressionsProjection"""
        result: set[str] = set()
        for e in self._output_name_to_exprs.values():
            result |= e._required_columns()
        return result

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
